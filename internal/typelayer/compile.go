package typelayer

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"strings"

	"github.com/wave-rf/chtypes/go/v2/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// compileSettings is the fixed parsing profile every table handle is compiled
// with. skip_unknown_fields=0 makes an unknown field a refusal (ClickHouse code
// 117) instead of silent data loss; it is never forwarded to the real INSERT,
// which carries only the exported columns. Error recovery
// (input_format_allow_errors_ratio, _num) stays off, as on a default INSERT: the
// first record the reader cannot read refuses the body (see Ingest).
//
// No type gate is set: every table compiled here already exists on the
// server, so compileTable compiles it under PostureLoad, as a server loads a
// table it holds, where CREATE's checks, the allow_experimental_* and
// allow_suspicious_* gates among them, do not apply (spec/abi-v2
// input:schema_options). A fixed gate list could never be complete: a
// Nullable(Tuple(...)) column, which needs a gate no list here held, kept its
// table unavailable for good. The statement is the table's own definition
// where chtypes models it, else its columns over a neutral engine
// (compileSchema). That rebuild is WaveHouse's, but of a table the server
// holds, from the server's own system.columns, and chtypes' maintainers
// confirmed it compiles under load (2026-10-27).
// flatten_nested=0 compiles a Nested column the way system.columns reported
// it: a table created with flatten_nested=0 arrives as one `n Nested(...)`
// column, and the default would split it into `n.a`, `n.b` Arrays the server
// does not have. A flattened table already arrives as those Arrays, so the
// setting changes nothing for it.
// date_time_input_format, input_format_null_as_default and
// output_format_json_quote_denormals must stay per-call parse settings (see
// InsertSettings), never schema settings here, because chtypes declines them as
// WHERE settings.
var compileSettings = map[string]string{
	"input_format_skip_unknown_fields": "0",
	"flatten_nested":                   "0",
}

// probeSettings is compileSettings plus the type gates, for readBy's probe,
// which must compile as a CREATE: only a CREATE refuses an expression over a
// column the statement lacks, and the load posture admits it. A type no gate
// here admits refuses the probe, which readBy counts as a read: such a
// table's EPHEMERAL column is unwritable when any other expression could read
// it, and writable only where nothing reads it (a DEFAULT probe refused over a
// gate, with no MATERIALIZED, ALIAS or EPHEMERAL expression), which is
// harmless since an EPHEMERAL value is never stored. A
// gate name the loaded line does not know fails the compile with code 115, so
// the list holds only names measured accepted on all five published chtypes
// lines (26.3.60.2, 26.7.19.5, 26.8.34.13, 26.9.25.15 and 26.10.2.103, build
// 20261104.155011; 26.7 from 20261021.130847).
var probeSettings = func() map[string]string {
	m := map[string]string{
		"allow_suspicious_low_cardinality_types": "1",
		"allow_suspicious_fixed_string_types":    "1",
		"allow_suspicious_variant_types":         "1",
		"allow_experimental_json_type":           "1",
		"allow_experimental_variant_type":        "1",
		"allow_experimental_dynamic_type":        "1",
		"allow_experimental_time_time64_type":    "1",
		"allow_experimental_bfloat16_type":       "1",
		"allow_experimental_object_type":         "1",
		"allow_experimental_nlp_functions":       "1",
	}
	maps.Copy(m, compileSettings)
	return m
}()

// compiled is one table shape's handle: a schema every goroutine shares, the
// filters compiled over it, and its columns as the compiler declared them.
// Calls on one handle run concurrently and Close waits for the calls inside
// it, so nothing here takes a lock around a call. One handle per table shape is
// shared by all requests; since library build 20261006.170903 that scales
// about as well as a handle per goroutine (Wave-RF/chtypes#480).
type compiled struct {
	schema  *chtypes.Schema
	filters *filterCache
	desc    []chtypes.Column
}

// compileTable compiles stmt on srv and reads its columns back. A non-empty
// second return is the cause.
func compileTable(lib *chtypes.Library, srv *chtypes.Server, stmt string) (*compiled, string) {
	c, err := compileStatement(lib, srv, stmt)
	if err != nil {
		return nil, describeCompileError(err)
	}
	return c, ""
}

// compileStatement is compileTable with the library's error as it came.
func compileStatement(lib *chtypes.Library, srv *chtypes.Server, stmt string) (*compiled, error) {
	s, err := lib.CompileTable(stmt, chtypes.OnServer(srv), chtypes.WithSettings(compileSettings),
		chtypes.WithPosture(chtypes.PostureLoad))
	if err != nil {
		return nil, err
	}
	d, err := s.Describe()
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("cannot describe the compiled table: %w", err)
	}
	return &compiled{schema: s, filters: newFilterCache(filterCacheSize), desc: d.Columns}, nil
}

// compileSchema compiles a table from its own definition
// (system.tables.create_table_query, ts.DDL) under PostureLoad, so chtypes
// models its engine, keys, partition, indices, CHECKs and TTL as the server
// holds them: protections that depend on the sorting key, such as an index
// analysis the server refuses, then answer as the server does. A definition
// chtypes declines (an engine it does not model, such as Distributed, S3 or
// Buffer, or a view's) falls back to the table's columns on the neutral
// engine, which serves it without those protections and says so in a WARN;
// so does a table discovered without its definition, silently. The rebuild
// carries none of the table's CHECKs, partitioning, engine behavior, TTL or
// settings, so its preview can accept a row the real INSERT refuses; the
// server still enforces them, and the worker parks such a row on the DLQ.
func compileSchema(id tenant.ID, lib *chtypes.Library, srv *chtypes.Server, ts *discovery.TableSchema, cols []colDecl) (*compiled, string) {
	for _, c := range cols {
		if strings.Contains(c.DefaultExpression, maskedValue) {
			return nil, fmt.Sprintf("column %q's %s expression holds a value the server masks as %s, which cannot be computed without the secret", c.Name, c.DefaultKind, maskedValue)
		}
	}
	if ts.DDL != "" {
		cause := "the definition holds a value the server masks as " + maskedValue
		masked := strings.Contains(ts.DDL, maskedValue)
		if !masked && secretCall.MatchString(ts.DDL) {
			cause = "the definition calls a function that takes a key"
		} else if !masked {
			c, err := compileStatement(lib, srv, ts.DDL)
			if err == nil {
				return c, ""
			}
			cause = definitionCause(err)
		}
		slog.Warn("chtypes could not compile the table's own definition; compiling its columns on a neutral engine, without its engine, keys, partition, indices, CHECKs or TTL",
			"tenant", id, "table", ts.Name, "cause", cause)
	}
	return compileColumns(lib, srv, cols)
}

// maskedValue is how the server spells a secret in the definitions it
// reports (system.tables.create_table_query, system.columns): an HMAC or
// encryption key, an engine's password. An expression holding it computes
// with the literal '[HIDDEN]' in the key's place, a wrong value (an HMAC keeps
// its three arguments), so a column whose expression holds it makes its
// table unavailable rather than export that value for the worker to store,
// and a definition holding it (a CHECK over such a key, an S3 password) is
// not compiled.
const maskedValue = "'[HIDDEN]'"

// secretCall matches a call of a ClickHouse function that takes a key: the
// ones the server masks in what it reports, and the keyed hashes, which it
// never masks. A definition calling one where the key is not masked puts the key
// into a CHECK, partition, sorting-key, index or TTL expression chtypes
// evaluates at parse, and a refusal there quotes its constant arguments to
// the client; such a definition is not compiled, as before the table's own
// definition was, and its table is served from its columns.
var secretCall = regexp.MustCompile(`(?i)\b(?:encrypt|decrypt|tryDecrypt|aes_encrypt_mysql|aes_decrypt_mysql|HMAC|sipHash64Keyed|sipHash128Keyed|sipHash128ReferenceKeyed)\s*\(`)

// definitionCause is why a table's own definition did not compile, safe to
// log: an external engine's definition carries its wiring (endpoint, bucket,
// user, access key), and both ClickHouse's message for a refused statement and
// some of chtypes' declines quote text from it (a setting's value, a
// dictionary's name), so a cause is named by its ids alone: ClickHouse's code
// and code name, or chtypes' catalog reason.
func definitionCause(err error) string {
	var se *chtypes.SchemaError
	if errors.As(err, &se) {
		if se.ChName != "" {
			return fmt.Sprintf("compile refused with ClickHouse code %d (%s)", se.ChCode, se.ChName)
		}
		return fmt.Sprintf("compile refused with ClickHouse code %d", se.ChCode)
	}
	var ue *chtypes.UnsupportedError
	if errors.As(err, &ue) && ue.Reason != "" {
		return "chtypes declined: " + ue.Reason
	}
	return "the definition did not compile"
}

// compileColumns builds the CREATE TABLE for cols and compiles it on srv.
func compileColumns(lib *chtypes.Library, srv *chtypes.Server, cols []colDecl) (*compiled, string) {
	stmt, err := createTable(lib, cols)
	if err != nil {
		return nil, "cannot build the table's CREATE statement: " + err.Error()
	}
	return compileTable(lib, srv, stmt)
}

// close releases the filters, then the schema. A filter holds a counted
// reference to its schema, so the order is not a safety requirement.
func (c *compiled) close() {
	if c == nil {
		return
	}
	c.filters.closeAll()
	_ = c.schema.Close()
}

// describeCompileError keeps ClickHouse's own refusal (a real code) distinct
// from chtypes declining to answer — the two mean different things to an
// operator and to the HTTP status a caller picks.
func describeCompileError(err error) string {
	var se *chtypes.SchemaError
	if errors.As(err, &se) {
		return fmt.Sprintf("compile refused with ClickHouse code %d: %s", se.ChCode, se.Message)
	}
	var ue *chtypes.UnsupportedError
	if errors.As(err, &ue) {
		return "chtypes declined: " + ue.Message
	}
	return err.Error()
}
