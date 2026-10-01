package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

const testUUID = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"

// uuidRegistry holds the two tables a malformed UUID takes records from:
// visits as a producer writes it (the id generated when omitted), and pings
// with the id first, for the positional formats. stamps is DateTime-first, a
// first column that skips a leading byte order mark like UUID does.
func uuidRegistry(t *testing.T) *discovery.SchemaRegistry {
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{
			Name: "stamps",
			Columns: []discovery.Column{
				{Name: "ts", Type: "DateTime", Position: 1},
				{Name: "page", Type: "String", Position: 2},
			},
		},
		{
			Name: "visits",
			Columns: []discovery.Column{
				{Name: "page", Type: "String", Position: 1},
				{Name: "id", Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateUUIDv4()", Position: 2},
			},
		},
		{
			Name: "pings",
			Columns: []discovery.Column{
				{Name: "id", Type: "UUID", Position: 1},
				{Name: "page", Type: "String", Position: 2},
				{Name: "n", Type: "UInt8", Position: 3},
			},
		},
	})
}

// TestIngest_ShortAnswerDeclinesTheWholeBatch is the regression guard for a
// silent loss measured on 26.8.15.10: a UUID shorter than 36 characters makes
// ClickHouse's UUID reader consume a fixed 36-byte window past it, so the
// records the window reaches are never read and get no verdict. chtypes
// answered ONE verdict for these three-record bodies, and the batch reported
// `total: 1` with /b and /c gone. Now the body's own count holds: every record
// is answered declined and nothing is published.
func TestIngest_ShortAnswerDeclinesTheWholeBatch(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, table, contentType, body string
	}{
		{"a JSON array", "visits", "application/json", `[{"page":"/a","id":"x"},{"page":"/b"},{"page":"/c"}]`},
		{"an NDJSON body", "visits", "application/x-ndjson", "{\"page\":\"/a\",\"id\":\"x\"}\n{\"page\":\"/b\"}\n{\"page\":\"/c\"}\n"},
		{"a bare CSV body", "pings", "text/csv", "zzz,/a,1\n" + testUUID + ",/b,2\n" + testUUID + ",/c,3"},
		{"a header=absent CSV body", "pings", "text/csv; header=absent", "zzz,/a,1\n" + testUUID + ",/b,2\n" + testUUID + ",/c,3\n"},
		{"a header=present CSV body", "pings", "text/csv; header=present", "id,page,n\nzzz,/a,1\n" + testUUID + ",/b,2\n" + testUUID + ",/c,3\n"},
		{"a TSV body", "pings", "text/tab-separated-values", "zzz\t/a\t1\n" + testUUID + "\t/b\t2\n" + testUUID + "\t/c\t3\n"},
		{"a byte-order-marked CSV body", "pings", "text/csv", utf8BOMString + "zzz,/a,1\n" + testUUID + ",/b,2\n" + testUUID + ",/c,3\n"},
		// A String first column keeps the mark, so ClickHouse reads this
		// names line as a record whose id is `id`, which takes /a with it.
		{"a byte-order-marked header a String-first table reads as a record", "visits", "text/csv", utf8BOMString + "page,id\n/a," + testUUID + "\n/b," + testUUID + "\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, uuidRegistry(t), pub)
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, tt.table, tt.contentType, tt.body)))

			require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
			resp := decodeBatchResult(t, w)
			assert.Equal(t, 3, resp.Total, "total is the body's records, not chtypes' short count")
			assert.Equal(t, 0, resp.Succeeded)
			assert.Equal(t, 3, resp.Failed)
			require.Len(t, resp.Results, 3)
			for i, r := range resp.Results {
				assert.Equal(t, i+1, r.Index)
				assert.Contains(t, r.Error, "validation engine declined: the body holds")
				assert.Contains(t, r.Error, "chtypes answered", "the message names both counts")
				assert.Contains(t, r.Error, "record 1 was refused (code 376", "and where the records went missing")
				assert.Zero(t, r.ExceptionCode, "a decline is not ClickHouse's verdict on the record")
			}
			assert.Empty(t, pub.Messages, "nothing from a short-answered body is published")
		})
	}
}

// TestIngest_ShortAnswerDeclinesTheSingleObject: a single object whose bad
// UUID swallows the objects concatenated after it is declined (422) rather
// than refused for its own value, because the body's count is not met.
// Alone, the same object is ClickHouse's own refusal.
func TestIngest_ShortAnswerDeclinesTheSingleObject(t *testing.T) {
	t.Parallel()
	h := newTestIngestHandler(t, uuidRegistry(t), &testutil.MockPublisher{})

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "visits", "application/json", `{"page":"/a","id":"x"}`)))
	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())

	w = httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "visits", "application/json", `{"page":"/a","id":"x"} {"page":"/b"} {"page":"/c"}`)))
	require.Equal(t, http.StatusUnprocessableEntity, w.Code, "body=%s", w.Body.String())
	assert.Contains(t, jsonErrorMessage(t, w), "the body holds at least 3 records but chtypes answered 1")
}

// TestIngest_CountedBodiesKeepPerRecordVerdicts: the count check must never
// trip on a body chtypes read whole. A bad value that is not a short UUID is
// still one record's refusal with its siblings published, and quoted newlines,
// blank NDJSON lines, a pretty-printed array and every CSV header form keep
// their per-record answers.
func TestIngest_CountedBodiesKeepPerRecordVerdicts(t *testing.T) {
	t.Parallel()
	u := testUUID
	for _, tt := range []struct {
		name, table, contentType, body string
		total, ok                      int
	}{
		{"a bad UInt8 in a JSON array", "pings", "application/json", `[{"page":"/a","n":"bad"},{"page":"/b"},{"page":"/c"}]`, 3, 2},
		{"a full-length bad UUID in a JSON array", "pings", "application/json", `[{"page":"/a","id":"` + u[:35] + `z"},{"page":"/b"},{"page":"/c"}]`, 3, 2},
		{"a bad UUID in the last record", "pings", "application/x-ndjson", "{\"page\":\"/b\"}\n{\"page\":\"/a\",\"id\":\"x\"}\n", 2, 1},
		{"blank and whitespace NDJSON lines", "pings", "application/x-ndjson", "{\"page\":\"/a\"}\n\n  \n{\"page\":\"/b\"}\n\n", 2, 2},
		{"a pretty-printed NDJSON body", "pings", "application/x-ndjson", "{\n  \"page\": \"/a\"\n}\n{\n  \"page\": \"/b\"\n}\n", 2, 2},
		{"a pretty-printed JSON array", "pings", "application/json", "[\n  {\"page\": \"/a\"},\n  {\"page\": \"/b\"}\n]\n", 2, 2},
		{"a raw newline inside a JSON string", "pings", "application/x-ndjson", "{\"page\":\"/a\nb\"}\n{\"page\":\"/c\"}\n", 2, 2},
		{"a quoted newline in bare CSV", "pings", "text/csv", u + ",\"/a\nx\",1\n" + u + ",/b,2\n", 2, 2},
		{"a quoted newline after a header", "pings", "text/csv; header=present", "page,id\n\"/a\nx\"," + u + "\n/b," + u + "\n", 2, 2},
		{"a detected names header", "pings", "text/csv", "id,page,n\n" + u + ",/a,1\n" + u + ",/b,2\n", 2, 2},
		{"a detected names and types header", "pings", "text/csv", "id,page,n\nUUID,String,UInt8\n" + u + ",/a,1\n", 1, 1},
		{"a detected subset header", "pings", "text/csv", "page,id\n/a," + u + "\n", 1, 1},
		{"a detected TSV header", "pings", "text/tab-separated-values", "id\tpage\tn\n" + u + "\t/a\t1\n", 1, 1},
		{"CRLF line endings", "pings", "text/csv; header=absent", u + ",/a,1\r\n" + u + ",/b,2\r\n", 2, 2},
		{"LF CR line endings", "pings", "text/csv; header=absent", u + ",/a,1\n\r" + u + ",/b,2\n\r", 2, 2},
		{"LF CR line endings after a detected header", "pings", "text/csv", "id,page,n\n\r" + u + ",/a,1\n\r" + u + ",/b,2\n\r", 2, 2},
		// ClickHouse skips a leading byte order mark before a UUID or
		// DateTime first column, so the names line after it is a header.
		{"a byte order mark before a detected header", "pings", "text/csv", utf8BOMString + "id,page,n\n" + u + ",/a,1\n" + u + ",/b,2\n", 2, 2},
		{"a byte order mark before a DateTime-first header", "stamps", "text/csv", utf8BOMString + "ts,page\n2024-01-01 00:00:00,/a\n2024-01-02 00:00:00,/b\n", 2, 2},
		{"a byte order mark before a detected TSV header", "pings", "text/tab-separated-values", utf8BOMString + "id\tpage\tn\n" + u + "\t/a\t1\n" + u + "\t/b\t2\n", 2, 2},
		{"a byte order mark before a quoted newline", "visits", "text/csv; header=present", utf8BOMString + "\"page\",id\n\"/a\nx\"," + u + "\n/b," + u + "\n", 2, 2},
		{"a byte order mark before a header=absent body", "pings", "text/csv; header=absent", utf8BOMString + u + ",/a,1\n" + u + ",/b,2\n", 2, 2},
		{"a byte order mark before an NDJSON body", "pings", "application/x-ndjson", utf8BOMString + "{\"page\":\"/a\"}\n{\"page\":\"/b\"}\n", 2, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, uuidRegistry(t), pub)
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, tt.table, tt.contentType, tt.body)))

			require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
			resp := decodeBatchResult(t, w)
			assert.Equal(t, tt.total, resp.Total)
			assert.Equal(t, tt.ok, resp.Succeeded)
			assert.Len(t, pub.Messages, tt.ok)
			for _, r := range resp.Results {
				assert.NotContains(t, r.Error, "declined", "a body chtypes read whole is never declined")
			}
		})
	}
}

// utf8BOMString is a UTF-8 byte order mark, as an editor or a spreadsheet
// export leads a file with it.
const utf8BOMString = "\xEF\xBB\xBF"

// TestIngest_JSONArrayLedByAMarkOrAFormFeed is the regression guard for a
// silent loss: the array-or-object byte skipped only space, tab, CR and LF, so
// an array led by a byte order mark or a form feed took the single-object path
// and published element 0 behind `200 {"ok":true}`. ClickHouse's JSON reader
// skips the mark at the start and all six ASCII whitespace bytes, so these are
// arrays: every element is answered and published.
func TestIngest_JSONArrayLedByAMarkOrAFormFeed(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ name, body string }{
		{"a byte order mark", utf8BOMString + `[{"page":"/a"},{"page":"/b"},{"page":"/c"}]`},
		{"a form feed", "\f" + `[{"page":"/a"},{"page":"/b"},{"page":"/c"}]`},
		{"a vertical tab", "\v" + `[{"page":"/a"},{"page":"/b"},{"page":"/c"}]`},
		{"a pretty-printed, marked array", utf8BOMString + "\n[\n  {\"page\": \"/a\"},\n\f  {\"page\": \"/b\"},\n  {\"page\": \"/c\"}\v\n]\f\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, uuidRegistry(t), pub)
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, "pings", "application/json", tt.body)))

			require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
			resp := decodeBatchResult(t, w)
			assert.Equal(t, 3, resp.Total, "the batch response, not the single-object one")
			assert.Equal(t, 3, resp.Succeeded)
			require.Len(t, pub.Messages, 3, "every element is published")
			for i, page := range []string{"/a", "/b", "/c"} {
				assert.Equal(t, page, publishedRow(t, pub.Messages[i].Data)["page"])
			}
		})
	}
}

// TestIngest_MarkedSingleObjectAndEmptyBodies: a single object led by a mark is
// still one object, and a body that is only a mark and whitespace is empty.
func TestIngest_MarkedSingleObjectAndEmptyBodies(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, uuidRegistry(t), pub)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "pings", "application/json", utf8BOMString+"\f"+`{"page":"/a"}`)))
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.JSONEq(t, `{"ok":true}`, w.Body.String())
	require.Len(t, pub.Messages, 1)

	for _, tt := range []struct{ contentType, message string }{
		{"application/json", "empty body"},
		{"application/x-ndjson", "empty ndjson body"},
		{"text/csv", "empty csv body"},
	} {
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "pings", tt.contentType, utf8BOMString+" \f\v\n")))
		require.Equal(t, http.StatusBadRequest, w.Code, "%s: body=%s", tt.contentType, w.Body.String())
		assert.Equal(t, tt.message, jsonErrorMessage(t, w), tt.contentType)
	}
	assert.Len(t, pub.Messages, 1, "nothing more is published")
}

// TestIngest_EmptyArrayElementIsInvalidJSON: a leading, doubled or trailing
// comma frames as a blank line, which is no record to ClickHouse, so the
// element count could not hold. It is refused as the invalid JSON it is.
func TestIngest_EmptyArrayElementIsInvalidJSON(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	for _, body := range []string{`[{"page":"/a"},]`, `[{"page":"/a"},,{"page":"/b"}]`, `[,{"page":"/a"}]`} {
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "application/json", body)))
		require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
		assert.Contains(t, jsonErrorMessage(t, w), "invalid json: empty element in the json array")
	}
	assert.Empty(t, pub.Messages)
}
