package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/typelayer/typelayertest"
)

const testUUID = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"

// uuidRegistry holds visits as a producer writes it (the id generated when
// omitted), and pings with the id first, for the positional formats. stamps is
// DateTime-first, a first column that skips a leading byte order mark like
// UUID does.
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

// requireRefused asserts a body ClickHouse's parser refused: a whole-request
// 400 clickhouse.rejected with its code and the 1-based record it failed on
// (none for record 0, a header), and nothing published. It returns the
// message.
func requireRefused(t *testing.T, w *httptest.ResponseRecorder, pub *testutil.MockPublisher, code, record int) string {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	msg, got := errorAndCode(t, w)
	assert.Equal(t, codeCHRejected, errorClass(t, w), msg)
	assert.Equal(t, code, got, msg)
	if record > 0 {
		assert.True(t, strings.HasPrefix(msg, fmt.Sprintf("record %d: ", record)), "names the record ClickHouse failed on: %s", msg)
	} else {
		assert.False(t, strings.HasPrefix(msg, "record "), msg)
	}
	assert.Empty(t, pub.Messages, "nothing from a refused body is published")
	return msg
}

// TestIngest_RecoveryBodies: every body on which recovering from a bad value
// lost a record, published a fragment as a row or blamed the wrong record
// (typelayertest.RecoveryBodies) is now one of two answers. Refused: a 400
// clickhouse.rejected carrying ClickHouse's code and naming the record it
// failed on, with nothing published. Accepted: every record answered ok and
// published as exactly its own row, and nothing else.
func TestIngest_RecoveryBodies(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, typelayertest.BodyTables())
	eng := newTestIngestHandler(t, reg, nil).Types
	for _, c := range typelayertest.RecoveryBodies {
		t.Run(string(c.Class)+"/"+c.Name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(fixedRegistry(reg), pub)
			h.Types = eng
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, c.Table, c.ContentType, c.Body)))

			if c.Refused != nil {
				requireRefused(t, w, pub, c.Refused.Code, c.Refused.Record)
				return
			}
			require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
			if c.ContentType == "application/json" && !strings.Contains(c.Body, "[") {
				assert.JSONEq(t, `{"ok":true}`, w.Body.String())
			} else {
				resp := decodeBatchResult(t, w)
				assert.Equal(t, len(c.Rows), resp.Total)
				assert.Equal(t, len(c.Rows), resp.Succeeded)
				for i, r := range resp.Results {
					assert.Equal(t, recordResult{Index: i + 1, Ok: true}, r)
				}
			}
			// The envelope carries each row compacted.
			want := make([]string, len(c.Rows))
			for i, r := range c.Rows {
				var b bytes.Buffer
				require.NoError(t, json.Compact(&b, []byte(r)))
				want[i] = b.String()
			}
			published := make([]string, len(pub.Messages))
			for i, m := range pub.Messages {
				var evt ingest.EventMessage
				require.NoError(t, json.Unmarshal(m.Data, &evt))
				published[i] = string(evt.Row)
			}
			assert.Equal(t, want, published, "one row per record, in order, and nothing else")
		})
	}
}

// TestIngest_ShortUUIDRefusesTheBody: a UUID shorter than 36 characters makes
// ClickHouse's UUID reader consume a fixed 36-byte window past it, into the
// records after it. With error recovery that lost them silently; without it,
// the read fails and the whole body is refused at the record that holds the
// value, as a ClickHouse INSERT refuses it.
func TestIngest_ShortUUIDRefusesTheBody(t *testing.T) {
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
		// names line as a record whose id is `id`.
		{"a byte-order-marked header a String-first table reads as a record", "visits", "text/csv", utf8BOMString + "page,id\n/a," + testUUID + "\n/b," + testUUID + "\n"},
		{"a single object", "visits", "application/json", `{"page":"/a","id":"x"}`},
		{"a single object with objects after it", "visits", "application/json", `{"page":"/a","id":"x"} {"page":"/b"} {"page":"/c"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, uuidRegistry(t), pub)
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, tt.table, tt.contentType, tt.body)))

			requireRefused(t, w, pub, 376, 1)
		})
	}
}

// TestIngest_CleanBodiesAnswerEveryRecord: framing that a record count would
// have to read past — quoted newlines, blank NDJSON lines, pretty-printing,
// every CSV header form, CRLF and LF CR line ends, a byte order mark — is
// ClickHouse's reader's own business: every record is answered and published.
func TestIngest_CleanBodiesAnswerEveryRecord(t *testing.T) {
	t.Parallel()
	u := testUUID
	for _, tt := range []struct {
		name, table, contentType, body string
		total                          int
	}{
		{"blank and whitespace NDJSON lines", "pings", "application/x-ndjson", "{\"page\":\"/a\"}\n\n  \n{\"page\":\"/b\"}\n\n", 2},
		{"a pretty-printed NDJSON body", "pings", "application/x-ndjson", "{\n  \"page\": \"/a\"\n}\n{\n  \"page\": \"/b\"\n}\n", 2},
		{"a pretty-printed JSON array", "pings", "application/json", "[\n  {\"page\": \"/a\"},\n  {\"page\": \"/b\"}\n]\n", 2},
		{"a raw newline inside a JSON string", "pings", "application/x-ndjson", "{\"page\":\"/a\nb\"}\n{\"page\":\"/c\"}\n", 2},
		{"a quoted newline in bare CSV", "pings", "text/csv", u + ",\"/a\nx\",1\n" + u + ",/b,2\n", 2},
		{"a quoted newline after a header", "pings", "text/csv; header=present", "page,id\n\"/a\nx\"," + u + "\n/b," + u + "\n", 2},
		{"a detected names header", "pings", "text/csv", "id,page,n\n" + u + ",/a,1\n" + u + ",/b,2\n", 2},
		{"a detected names and types header", "pings", "text/csv", "id,page,n\nUUID,String,UInt8\n" + u + ",/a,1\n", 1},
		{"a detected subset header", "pings", "text/csv", "page,id\n/a," + u + "\n", 1},
		{"a detected TSV header", "pings", "text/tab-separated-values", "id\tpage\tn\n" + u + "\t/a\t1\n", 1},
		{"CRLF line endings", "pings", "text/csv; header=absent", u + ",/a,1\r\n" + u + ",/b,2\r\n", 2},
		{"LF CR line endings", "pings", "text/csv; header=absent", u + ",/a,1\n\r" + u + ",/b,2\n\r", 2},
		{"LF CR line endings after a detected header", "pings", "text/csv", "id,page,n\n\r" + u + ",/a,1\n\r" + u + ",/b,2\n\r", 2},
		// ClickHouse skips a leading byte order mark before a UUID or
		// DateTime first column, so the names line after it is a header.
		{"a byte order mark before a detected header", "pings", "text/csv", utf8BOMString + "id,page,n\n" + u + ",/a,1\n" + u + ",/b,2\n", 2},
		{"a byte order mark before a DateTime-first header", "stamps", "text/csv", utf8BOMString + "ts,page\n2024-01-01 00:00:00,/a\n2024-01-02 00:00:00,/b\n", 2},
		{"a byte order mark before a detected TSV header", "pings", "text/tab-separated-values", utf8BOMString + "id\tpage\tn\n" + u + "\t/a\t1\n" + u + "\t/b\t2\n", 2},
		{"a byte order mark before a quoted newline", "visits", "text/csv; header=present", utf8BOMString + "\"page\",id\n\"/a\nx\"," + u + "\n/b," + u + "\n", 2},
		{"a byte order mark before a header=absent body", "pings", "text/csv; header=absent", utf8BOMString + u + ",/a,1\n" + u + ",/b,2\n", 2},
		{"a byte order mark before an NDJSON body", "pings", "application/x-ndjson", utf8BOMString + "{\"page\":\"/a\"}\n{\"page\":\"/b\"}\n", 2},
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
			assert.Equal(t, tt.total, resp.Succeeded)
			assert.Len(t, pub.Messages, tt.total)
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

// TestIngest_MalformedArrayIsRefused: ClickHouse's reader reads a JSON array
// itself, and refuses one with an empty element, an unterminated one, or one
// with anything after its closing ']', so the request is refused and nothing
// is published, the elements before the fault included.
func TestIngest_MalformedArrayIsRefused(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	for _, body := range []string{
		`[{"page":"/a"},]`,
		"[{\"page\":\"/a\"},\n ]",
		`[{"page":"/a"},,{"page":"/b"}]`,
		`[,{"page":"/a"}]`,
		`[,]`,
		"[{\"page\":\"/a\"},\f]",
		`[{"page":"/a"}`,
		`[{"page":"/a"},`,
		`[`,
		`[{"page":"/a"},{"b`,
		`[{"page":"/a"}, {bad]`,
		`[{"page":"/a"}}`,
		`[{"page":"/a"},{"page":"/b"}] {"page":"/c"}`,
		`[{"page":"/a"}][{"page":"/b"}]`,
		`[{"page":"/a"}]]`,
		"[{\"page\":\"/a\"}]\nx",
		"[{\"page\":\"/a\"}]\xEF\xBB\xBF",
	} {
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "application/json", body)))
		require.Equal(t, http.StatusBadRequest, w.Code, "%q: body=%s", body, w.Body.String())
		msg, code := errorAndCode(t, w)
		assert.Equal(t, codeCHRejected, errorClass(t, w), "%q", body)
		assert.NotZero(t, code, "%q: %s", body, msg)
	}
	assert.Empty(t, pub.Messages)
}

// TestIngest_DeclinedBodyIsA422: a body chtypes cannot answer for — here a
// record that omits a column whose DEFAULT it does not support — is declined
// as a whole request, a 422 that is no verdict on the data, and nothing is
// published. Its reader stops at that record, so a per-record answer would
// leave the records after it unreported.
func TestIngest_DeclinedBodyIsA422(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{{
		Name: "snow",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "sid", Type: "UInt64", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateSnowflakeID()", Position: 2},
		},
	}})
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, reg, pub)
	for _, tt := range []struct{ contentType, body string }{
		{"application/x-ndjson", "{\"page\":\"/a\",\"sid\":1}\n{\"page\":\"/b\"}\n{\"page\":\"/c\"}\n"},
		{"application/json", `{"page":"/a"}`},
	} {
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "snow", tt.contentType, tt.body)))
		require.Equal(t, http.StatusUnprocessableEntity, w.Code, "body=%s", w.Body.String())
		msg, code := errorAndCode(t, w)
		assert.True(t, strings.HasPrefix(msg, "validation engine declined: "), msg)
		assert.Zero(t, code, "not ClickHouse's verdict on the data")
		assert.Empty(t, errorClass(t, w))
	}
	assert.Empty(t, pub.Messages)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "snow", "application/x-ndjson", "{\"page\":\"/a\",\"sid\":1}\n{\"page\":\"/b\",\"sid\":2}\n")))
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, 2, decodeBatchResult(t, w).Succeeded, "records that supply the column are unaffected")
	assert.Len(t, pub.Messages, 2)
}
