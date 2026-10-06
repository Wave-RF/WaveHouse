package typelayer

import (
	"errors"
	"fmt"

	"github.com/wave-rf/chtypes/go/chtypes"
)

// compileSettings is the fixed parsing profile every table handle is compiled
// with. allow_errors_ratio turns "first bad row ends the batch" into
// skip-and-continue so every record gets its own verdict; skip_unknown_fields=0
// makes an unknown field a real per-row rejection (ClickHouse code 117) instead
// of silent data loss. Neither is ever forwarded to the real INSERT.
//
// The type gates admit the column types ClickHouse refuses to create by
// default. Every table compiled here already exists on the server, so its
// CREATE passed these gates there; without them chtypes refuses every record
// of, say, a LowCardinality(UInt64) table with code 455, which the server
// would insert. A gate name the loaded line does not know fails the compile
// with code 115 (allow_experimental_qbit_type did on 25.8, which chtypes 1.0
// does not publish), so the list holds only names measured accepted on all
// four chtypes 1.0 lines (26.3.38.2, 26.7.19.5, 26.8.15.10 and 26.9.8.3).
var compileSettings = map[string]string{
	"input_format_allow_errors_ratio":  "1",
	"input_format_skip_unknown_fields": "0",

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
// shared by all requests; chtypes go 1.0.2 improved how ingest scales across
// goroutines, and a gap remains upstream (Wave-RF/chtypes#456).
type compiled struct {
	schema  *chtypes.Schema
	filters *filterCache
	desc    []chtypes.Column
}

// compileTable compiles stmt and reads its columns back. A non-empty second
// return is the cause.
func compileTable(lib *chtypes.Library, stmt string) (*compiled, string) {
	s, err := lib.CompileTable(stmt, chtypes.WithSettings(compileSettings))
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

// compileColumns builds the CREATE TABLE for cols and compiles it.
func compileColumns(lib *chtypes.Library, cols []colDecl) (*compiled, string) {
	stmt, err := createTable(lib, cols)
	if err != nil {
		return nil, "cannot build the table's CREATE statement: " + err.Error()
	}
	return compileTable(lib, stmt)
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
