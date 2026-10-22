package typelayertest

import (
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// RecoveryBodies are 76 bodies on which ClickHouse's reader, when allowed to
// recover from a bad value (input_format_allow_errors_ratio), resumes inside a
// record or past the next one: records went missing behind an accepted
// verdict, fragments of a record parsed as rows of their own, and errors
// landed on the wrong record. Ingest parses with recovery off, as a default
// INSERT does, so each body is now either accepted whole, record for record,
// or refused whole at the record ClickHouse failed on. Both suites (the type
// layer's and the HTTP handler's) run every one against the real artifact.
//
// The measured answers are the 26.8 artifact's.
var RecoveryBodies = []BodyCase{
	{
		Class: ClassSkip, Name: "tsv blank line", Table: "pings", ContentType: "text/tab-separated-values",
		Format:  typelayer.FormatTSV,
		Body:    u + "\t/rA\t1\n\n" + u + "\t/rB\t2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "tsv blank line, header=absent", Table: "pings", ContentType: "text/tab-separated-values; header=absent",
		Format: typelayer.FormatTSV, Strict: true,
		Body:    u + "\t/rA\t1\n\n" + u + "\t/rB\t2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "tsv blank line, header=present", Table: "pings", ContentType: "text/tab-separated-values; header=present",
		Format:  typelayer.FormatTSVWithNames,
		Body:    "id\tpage\tn\n" + u + "\t/rA\t1\n\n" + u + "\t/rB\t2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "tsv LF then LF CR", Table: "pings", ContentType: "text/tab-separated-values",
		Format:  typelayer.FormatTSV,
		Body:    u + "\t/rA\t1\n\n\r" + u + "\t/rB\t2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "tsv LF CR (CR opens record 2)", Table: "pings", ContentType: "text/tab-separated-values",
		Format:  typelayer.FormatTSV,
		Body:    u + "\t/rA\t1\n\r" + u + "\t/rB\t2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "csv blank line", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    u + ",/rA,1\n\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "csv blank line, header=absent", Table: "pings", ContentType: "text/csv; header=absent",
		Format: typelayer.FormatCSV, Strict: true,
		Body:    u + ",/rA,1\n\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "csv blank line, header=present", Table: "pings", ContentType: "text/csv; header=present",
		Format:  typelayer.FormatCSVWithNames,
		Body:    "id,page,n\n" + u + ",/rA,1\n\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassSkip, Name: "csv quoted multi-line", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,\"/rA\nzz\",1\n" + u + ",\"/rB\nzz\",2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassSkip, Name: "csv quoted multi-line, header=absent", Table: "pings", ContentType: "text/csv; header=absent",
		Format: typelayer.FormatCSV, Strict: true,
		Body:    "x,\"/rA\nzz\",1\n" + u + ",\"/rB\nzz\",2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassSkip, Name: "csv quoted multi-line, header=present", Table: "pings", ContentType: "text/csv; header=present",
		Format:  typelayer.FormatCSVWithNames,
		Body:    "id,page,n\nx,\"/rA\nzz\",1\n" + u + ",\"/rB\nzz\",2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassSkip, Name: "ndjson objects spanning lines", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"x\",\n\"page\":\"/rA\",\n\"n\":1}\n{\"id\":\"" + u + "\",\n\"page\":\"/rB\",\n\"n\":2}\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassSkip, Name: "json array, objects spanning lines", Table: "pings", ContentType: "application/json",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "[{\"id\":\"x\",\n\"page\":\"/rA\",\n\"n\":1},\n{\"id\":\"" + u + "\",\n\"page\":\"/rB\",\n\"n\":2}]",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "csv fragment is a full row", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,\"/rA\nzz\",1\n" + u + ",\"/rB\n" + u + ",zz\",2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "csv fragment is a full row, header=absent", Table: "pings", ContentType: "text/csv; header=absent",
		Format: typelayer.FormatCSV, Strict: true,
		Body:    "x,\"/rA\nzz\",1\n" + u + ",\"/rB\n" + u + ",zz\",2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "csv fragment is a full row, header=present", Table: "pings", ContentType: "text/csv; header=present",
		Format:  typelayer.FormatCSVWithNames,
		Body:    "id,page,n\nx,\"/rA\nzz\",1\n" + u + ",\"/rB\n" + u + ",zz\",2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "csv multi-line note (msg,n,id)", Table: "notes", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "hello,1,x\n\"this is a longer first line of the note\nline two\",2," + u + "\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "csv multi-line note + good third record", Table: "notes", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "hello,1,x\n\"this is a longer first line of the note\nline two\",2," + u + "\nbye,3," + u + "\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "csv phantom inside the bad record itself", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,\"" + strings.Repeat("a", 40) + "\n" + u + ",zz\",1\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "tsv escaped LF fragment is a full row", Table: "pings", ContentType: "text/tab-separated-values",
		Format:  typelayer.FormatTSV,
		Body:    "x\t/rA\t1\n" + u + "\t/rB\\\n" + u + "\tzz\t2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "tsv multi-line note (escaped LF)", Table: "notes", ContentType: "text/tab-separated-values",
		Format:  typelayer.FormatTSV,
		Body:    "hello\t1\tx\nthis is a longer first line of the note\\\nline two\t2\t" + u + "\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "ndjson nested object on its own line", Table: "pj", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"x\",\"page\":\"/rA\",\"n\":1}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2,\"meta\":\n{\"n\":7}\n}\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "ndjson one bad record with nested object line", Table: "pj", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"x\",\"page\":\"/r" + strings.Repeat("A", 36) + "\",\"meta\":\n{\"n\":7}\n}\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "json single object with nested object line", Table: "pj", ContentType: "application/json",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"x\",\"page\":\"/r" + strings.Repeat("A", 36) + "\",\"meta\":\n{\"n\":7}\n}",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "ndjson pretty items array", Table: "pj", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"x\",\"page\":\"/rA\",\"n\":1}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2,\"meta\":[\n  {\"n\":7},\n  {\"n\":8}\n]}\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "ndjson raw LF in string then {}", Table: "pj", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"x\",\"page\":\"/rA\",\"n\":1}\n{\"id\":\"" + u + "\",\"n\":2,\"page\":\"/rB\n{}\"}\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "json array raw LF in string then {}", Table: "pj", ContentType: "application/json",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "[{\"id\":\"x\",\"page\":\"/rA\",\"n\":1},{\"id\":\"" + u + "\",\"n\":2,\"page\":\"/rB\n{}\"}]",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "json array 4 records count-preserving", Table: "pj", ContentType: "application/json",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "[{\"id\":\"x\",\"page\":\"/rA\",\"n\":1},{\"id\":\"" + u + "\",\"n\":2,\"page\":\"/rB\n{}\"},{\"id\":\"y\",\"page\":\"/rC\",\"n\":3},{\"n\":4}]",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassFragment, Name: "ndjson 4 records count-preserving", Table: "pj", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"x\",\"page\":\"/rA\",\"n\":1}\n{\"id\":\"" + u + "\",\"n\":2,\"page\":\"/rB\n{}\"}\n{\"id\":\"y\",\"page\":\"/rC\",\"n\":3}\n{\"n\":4}\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "csv bad UInt8", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    u + ",/rA,abc\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 117, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "csv 36-char bad UUID", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    bad36 + ",/rA,1\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "csv extra field", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    u + ",/rA,1,extra\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 117, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "csv short UUID in the last record", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    u + ",/rA,1\nx,/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassOrdinary, Name: "csv short UUID last, no trailing LF", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    u + ",/rA,1\n" + u + ",/rB,2\nx,/rC,3",
		Refused: &Refusal{Code: 376, Record: 3},
	},
	{
		Class: ClassOrdinary, Name: "tsv bad UInt8", Table: "pings", ContentType: "text/tab-separated-values",
		Format:  typelayer.FormatTSV,
		Body:    u + "\t/rA\tabc\n" + u + "\t/rB\t2\n",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "ndjson bad UInt8", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":\"abc\"}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "ndjson unknown field", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":1,\"zz\":1}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 117, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "ndjson short UUID in the last record", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":1}\n{\"id\":\"x\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 376, Record: 2},
	},
	{
		Class: ClassOrdinary, Name: "ndjson 36-char bad UUID first", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"" + bad36 + "\",\"page\":\"/rA\",\"n\":1}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassOrdinary, Name: "json array bad UInt8", Table: "pings", ContentType: "application/json",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "[{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":\"abc\"},{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}]",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassControl, Name: "csv all good", Table: "pings", ContentType: "text/csv",
		Format: typelayer.FormatCSV,
		Body:   u + ",/rA,1\n" + u + ",/rB,2\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassControl, Name: "csv quoted LFs all good", Table: "pings", ContentType: "text/csv",
		Format: typelayer.FormatCSV,
		Body:   u + ",\"/rA\nzz\",1\n" + u + ",\"/rB\nzz\",2\n",
		Rows:   []string{`["` + u + `", "/rA\nzz", 1]`, `["` + u + `", "/rB\nzz", 2]`},
	},
	{
		Class: ClassControl, Name: "tsv escaped LF all good", Table: "pings", ContentType: "text/tab-separated-values",
		Format: typelayer.FormatTSV,
		Body:   u + "\t/rA\\\nzz\t1\n" + u + "\t/rB\t2\n",
		Rows:   []string{`["` + u + `", "/rA\nzz", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassControl, Name: "ndjson blank line all good", Table: "pings", ContentType: "application/x-ndjson",
		Format: typelayer.FormatJSONEachRow,
		Body:   "{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":1}\n\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassControl, Name: "ndjson pretty all good", Table: "pj", ContentType: "application/x-ndjson",
		Format: typelayer.FormatJSONEachRow,
		Body:   "{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2,\"meta\":\n{\"n\":7}\n}\n",
		Rows:   []string{`["` + u + `", "/rB", 2, "{\"n\":7}"]`},
	},
	{
		Class: ClassInRecord, Name: "csv 36-char bad UUID before quoted LF, fragment is a row", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    bad36 + ",\"/rA\n" + u + ",zz\",1\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassInRecord, Name: "csv 36-char bad UUID before quoted LF, junk fragment", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    bad36 + ",\"/rA\nzz\",1\n" + u + ",/rB,2\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassInRecord, Name: "ndjson multi-line object, bad value on line 1", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"" + u + "\",\"n\":\"abc\",\n\"page\":\"/rA\"}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassInRecord, Name: "ndjson multi-line object, bad value, nested {} line", Table: "pj", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"" + u + "\",\"n\":\"abc\",\"meta\":\n{\"n\":7}\n}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassInRecord, Name: "json array pretty, bad value", Table: "pj", ContentType: "application/json",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "[\n {\"id\":\"" + u + "\",\"n\":\"abc\",\"meta\":\n  {\"n\":7}\n },\n {\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n]",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassFraming, Name: "ndjson scalar line", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "1\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassFraming, Name: "ndjson garbage line", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "abc\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassFraming, Name: "ndjson truncated object", Table: "pings", ContentType: "application/x-ndjson",
		Format:  typelayer.FormatJSONEachRow,
		Body:    "{\"id\":\"" + u + "\",\"page\":\"/rA\"\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Refused: &Refusal{Code: 27, Record: 1},
	},
	{
		Class: ClassFraming, Name: "ndjson leading spaces", Table: "pings", ContentType: "application/x-ndjson",
		Format: typelayer.FormatJSONEachRow,
		Body:   "  {\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":1}\n\t{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassFraming, Name: "csv CRLF", Table: "pings", ContentType: "text/csv",
		Format: typelayer.FormatCSV,
		Body:   u + ",/rA,1\r\n" + u + ",/rB,2\r\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassFraming, Name: "csv detected header", Table: "pings", ContentType: "text/csv",
		Format: typelayer.FormatCSV,
		Body:   "id,page,n\n" + u + ",/rA,1\n" + u + ",/rB,2\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassFraming, Name: "csv BOM detected header", Table: "pings", ContentType: "text/csv",
		Format: typelayer.FormatCSV,
		Body:   "\ufeffid,page,n\n" + u + ",/rA,1\n" + u + ",/rB,2\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassFraming, Name: "json array good", Table: "pings", ContentType: "application/json",
		Format: typelayer.FormatJSONEachRow,
		Body:   "[{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":1}, {\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}]",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassFraming, Name: "json array pretty good", Table: "pings", ContentType: "application/json",
		Format: typelayer.FormatJSONEachRow,
		Body:   "[\n {\"id\":\"" + u + "\",\n  \"page\":\"/rA\",\n  \"n\":1},\n {\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n]",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassFraming, Name: "tsv header=present good", Table: "pings", ContentType: "text/tab-separated-values; header=present",
		Format: typelayer.FormatTSVWithNames,
		Body:   "page\tid\n/rA\t" + u + "\n/rB\t" + u + "\n",
		Rows:   []string{`["` + u + `", "/rA", 0]`, `["` + u + `", "/rB", 0]`},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end k=23", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 23) + ",1\n" + u + ",/rB,2\n" + bad36 + ",\"/rC\n" + u + ",zz\",3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end k=24", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 24) + ",1\n" + u + ",/rB,2\n" + bad36 + ",\"/rC\n" + u + ",zz\",3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end k=25", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 25) + ",1\n" + u + ",/rB,2\n" + bad36 + ",\"/rC\n" + u + ",zz\",3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end k=26", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 26) + ",1\n" + u + ",/rB,2\n" + bad36 + ",\"/rC\n" + u + ",zz\",3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end k=27", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 27) + ",1\n" + u + ",/rB,2\n" + bad36 + ",\"/rC\n" + u + ",zz\",3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end, no phantom k=23", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 23) + ",1\n" + u + ",/rB,2\n" + u + ",/rC,3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end, no phantom k=24", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 24) + ",1\n" + u + ",/rB,2\n" + u + ",/rC,3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end, no phantom k=25", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 25) + ",1\n" + u + ",/rB,2\n" + u + ",/rC,3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end, no phantom k=26", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 26) + ",1\n" + u + ",/rB,2\n" + u + ",/rC,3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBoundary, Name: "csv window ends at line end, no phantom k=27", Table: "pings", ContentType: "text/csv",
		Format:  typelayer.FormatCSV,
		Body:    "x,/r" + strings.Repeat("A", 27) + ",1\n" + u + ",/rB,2\n" + u + ",/rC,3\n",
		Refused: &Refusal{Code: 376, Record: 1},
	},
	{
		Class: ClassBOM, Name: "csv BOM UUID-first", Table: "pings", ContentType: "text/csv",
		Format: typelayer.FormatCSV,
		Body:   "\ufeff" + u + ",/rA,1\n" + u + ",/rB,2\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassBOM, Name: "csv BOM String-first", Table: "notes", ContentType: "text/csv",
		Format: typelayer.FormatCSV,
		Body:   "\ufeffhello,1," + u + "\nbye,2," + u + "\n",
		Rows:   []string{"[\"\ufeffhello\", 1, \"" + u + `"]`, `["bye", 2, "` + u + `"]`},
	},
	{
		Class: ClassBOM, Name: "tsv BOM UUID-first", Table: "pings", ContentType: "text/tab-separated-values",
		Format: typelayer.FormatTSV,
		Body:   "\ufeff" + u + "\t/rA\t1\n" + u + "\t/rB\t2\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassBOM, Name: "ndjson BOM", Table: "pings", ContentType: "application/x-ndjson",
		Format: typelayer.FormatJSONEachRow,
		Body:   "\ufeff{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":1}\n{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassBOM, Name: "json array BOM", Table: "pings", ContentType: "application/json",
		Format: typelayer.FormatJSONEachRow,
		Body:   "\ufeff[{\"id\":\"" + u + "\",\"page\":\"/rA\",\"n\":1},{\"id\":\"" + u + "\",\"page\":\"/rB\",\"n\":2}]",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
	{
		Class: ClassBOM, Name: "csv header=present BOM", Table: "pings", ContentType: "text/csv; header=present",
		Format: typelayer.FormatCSVWithNames,
		Body:   "\ufeffid,page,n\n" + u + ",/rA,1\n" + u + ",/rB,2\n",
		Rows:   []string{`["` + u + `", "/rA", 1]`, `["` + u + `", "/rB", 2]`},
	},
}

// BodyCase is one body of RecoveryBodies, with ClickHouse's answer to it.
// Exactly one of Refused and Rows is set.
type BodyCase struct {
	Class BodyClass
	Name  string
	// Table is one of BodyTables'.
	Table string
	// ContentType is how the HTTP suite sends it; Format and Strict are how
	// that reaches the type layer.
	ContentType string
	Format      typelayer.Format
	Strict      bool
	Body        string
	// Refused is the code and the 1-based record ClickHouse refuses the body
	// at; nothing in it is accepted.
	Refused *Refusal
	// Rows is every record's exported row, in order, when the body is
	// accepted: no record lost and none added.
	Rows []string
}

// Refusal is ClickHouse's refusal of a body: its code, and the 1-based record
// it failed on.
type Refusal struct {
	Code, Record int
}

// BodyClass groups RecoveryBodies by what recovery used to do with them.
type BodyClass string

// The classes of RecoveryBodies.
const (
	// ClassSkip: a bad record whose recovery took the head of the next one (a
	// blank line, a quoted or pretty-printed line break).
	ClassSkip BodyClass = "skip"
	// ClassFragment: a fragment of a record parsed as a row, so a record was
	// lost behind an accepted verdict or a row nobody sent was published.
	ClassFragment BodyClass = "fragment"
	// ClassOrdinary: an ordinary bad value, which recovery handled correctly.
	ClassOrdinary BodyClass = "ordinary"
	// ClassControl: clean bodies with line breaks inside records.
	ClassControl BodyClass = "control"
	// ClassInRecord: an ordinary bad value before a line break inside its own
	// record.
	ClassInRecord BodyClass = "in-record"
	// ClassFraming: framing a record count has to read past (headers, CRLF,
	// leading spaces, scalar lines, arrays).
	ClassFraming BodyClass = "framing"
	// ClassBoundary: a short UUID whose 36-byte read ends exactly on a line
	// end, so recovery skipped the whole next record.
	ClassBoundary BodyClass = "boundary"
	// ClassBOM: a leading UTF-8 byte order mark.
	ClassBOM BodyClass = "bom"
)

// u is a valid UUID, and bad36 one character off it.
const (
	u     = "01234567-89ab-cdef-0123-456789abcdef"
	bad36 = "01234567-89ab-cdef-0123-456789abcdez"
)

// BodyTables are the tables RecoveryBodies are written for.
func BodyTables() []*discovery.TableSchema {
	return []*discovery.TableSchema{
		{Name: "pings", Columns: []discovery.Column{
			{Name: "id", Type: "UUID", Position: 1},
			{Name: "page", Type: "String", Position: 2},
			{Name: "n", Type: "UInt8", Position: 3},
		}},
		{Name: "notes", Columns: []discovery.Column{
			{Name: "msg", Type: "String", Position: 1},
			{Name: "n", Type: "UInt8", Position: 2},
			{Name: "id", Type: "UUID", Position: 3},
		}},
		{Name: "pj", Columns: []discovery.Column{
			{Name: "id", Type: "UUID", Position: 1},
			{Name: "page", Type: "String", Position: 2},
			{Name: "n", Type: "UInt8", Position: 3},
			{Name: "meta", Type: "String", Position: 4},
		}},
	}
}
