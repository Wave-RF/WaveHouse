package typelayer

import (
	"errors"
	"fmt"

	"github.com/wave-rf/chtypes/go/v2/chtypes"
)

// compileSettings is the fixed parsing profile every table handle is compiled
// with. skip_unknown_fields=0 makes an unknown field a refusal (ClickHouse code
// 117) instead of silent data loss; it is never forwarded to the real INSERT,
// which carries only the exported columns. Error recovery
// (input_format_allow_errors_ratio, _num) stays off, as on a default INSERT: the
// first record the reader cannot read refuses the body (see Ingest).
//
// The type gates admit the column types ClickHouse refuses to create by
// default. Every table compiled here already exists on the server, so its
// CREATE passed these gates there; without them chtypes refuses every record
// of, say, a LowCardinality(UInt64) table with code 455, which the server
// would insert. A gate name the loaded line does not know fails the compile
// with code 115 (allow_experimental_qbit_type did on 25.8, a line chtypes
// does not publish), so the list holds only names measured accepted on all
// four published chtypes lines (26.3.38.2, 26.7.19.5, 26.8.15.10 and 26.9.8.3).
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
	s, err := lib.CompileTable(stmt, chtypes.OnServer(srv), chtypes.WithSettings(compileSettings))
	if err != nil {
		return nil, describeCompileError(err)
	}
	d, err := s.Describe()
	if err != nil {
		_ = s.Close()
		return nil, "cannot describe the compiled table: " + err.Error()
	}
	return &compiled{schema: s, filters: newFilterCache(filterCacheSize), desc: d.Columns}, ""
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
