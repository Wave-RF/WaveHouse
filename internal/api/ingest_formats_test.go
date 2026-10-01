package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// TestIngest_JSONArray_CompactWithOneBadRecord is the regression guard for the
// whole reason the depth-1 comma rewrite exists.
//
// Measured: a SINGLE-LINE array with one bad record makes chtypes answer
// Outcome=rejected with no exported bytes — the records that parsed perfectly
// are lost with it. That breaks #195's promise that one bad record never
// obscures the rest of the batch. Newline-framing the elements restores it.
func TestIngest_JSONArray_CompactWithOneBadRecord(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "application/json",
		`[{"page":"/a"},{"page":"/b","nope":1},{"page":"/c"}]`)))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Results, 3)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.Equal(t, 117, resultAt(t, resp, 2).ExceptionCode)
	assert.True(t, resultAt(t, resp, 3).Ok)
	require.Len(t, pub.Messages, 2, "the siblings of a refused record still publish")
	assert.Equal(t, "/a", publishedRow(t, pub.Messages[0].Data)["page"])
	assert.Equal(t, "/c", publishedRow(t, pub.Messages[1].Data)["page"])
}

// TestIngest_CSV: CSV is header-less and POSITIONAL in the table's declaration
// order — the wire columns, which is declaration order minus MATERIALIZED, ALIAS
// and EPHEMERAL. Every wire column must be present; an empty field takes the
// column's DEFAULT.
func TestIngest_CSV(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	// clicks is page, button, count, event_id, org_id.
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv",
		"\"/a\",\"buy\",3,\"e1\",\"acme\"\n\"/b\",\"sell\",4,\"e2\",\"acme\"\n")))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	require.Len(t, pub.Messages, 2)
	row := publishedRow(t, pub.Messages[0].Data)
	assert.Equal(t, "/a", row["page"])
	assert.Equal(t, "buy", row["button"])
	assert.Equal(t, float64(3), row["count"])
	assert.Equal(t, "acme", row["org_id"])
}

// TestIngest_CSV_PositionalContract pins the three ways a producer gets the
// positional contract wrong, each with ClickHouse's own code, under
// `header=absent` (strictly positional, detection off). A header line there is
// not a header: it is one record that fails to parse, and the data rows around
// it still ingest.
func TestIngest_CSV_PositionalContract(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		body string
		ok   int
		bad  int
	}{
		{"a short row is a per-record failure", "\"/a\",\"buy\"\n\"/b\",\"sell\",4,\"e2\",\"acme\"\n", 1, 1},
		{"an extra field is a per-record failure", "\"/a\",\"buy\",3,\"e1\",\"acme\",99\n", 0, 1},
		{"a header line is one failed record, not a header", "page,button,count,event_id,org_id\n\"/b\",\"sell\",4,\"e2\",\"acme\"\n", 1, 1},
		{"an empty field takes the column default", "\"/a\",\"buy\",3,\"e1\",\n", 1, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, testRegistry(t), pub)
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv; header=absent", tt.body)))

			require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
			resp := decodeBatchResult(t, w)
			assert.Equal(t, tt.ok, resp.Succeeded)
			assert.Equal(t, tt.bad, resp.Failed)
			assert.Len(t, pub.Messages, tt.ok)
			for _, r := range resp.Results {
				if r.Error != "" {
					assert.NotZero(t, r.ExceptionCode, "a parser refusal carries ClickHouse's code")
				}
			}
		})
	}
}

// TestIngest_BareCSV_AutoDetectsHeader: a `text/csv` with no header parameter
// is ClickHouse's default CSV, which consumes a first line that spells the
// column names as a header. The indices and total count only data rows; the
// same body under header=absent rejects that line at index 1 (code 27); a bare
// body with no header line ingests every row.
func TestIngest_BareCSV_AutoDetectsHeader(t *testing.T) {
	t.Parallel()
	const header = "page,button,count,event_id,org_id\n"
	const rows = "\"/a\",\"buy\",3,\"e1\",\"acme\"\n\"/b\",\"sell\",4,\"e2\",\"acme\"\n"

	t.Run("bare consumes the header line", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := newTestIngestHandler(t, testRegistry(t), pub)
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv", header+rows)))
		require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
		resp := decodeBatchResult(t, w)
		assert.Equal(t, 2, resp.Total, "the detected header is not a record")
		assert.Equal(t, 2, resp.Succeeded)
		require.Len(t, pub.Messages, 2)
		assert.Equal(t, "/a", publishedRow(t, pub.Messages[0].Data)["page"])
	})

	t.Run("header=absent rejects the header line", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := newTestIngestHandler(t, testRegistry(t), pub)
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv; header=absent", header+rows)))
		require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
		resp := decodeBatchResult(t, w)
		assert.Equal(t, 3, resp.Total)
		assert.Equal(t, 2, resp.Succeeded)
		assert.Equal(t, 27, resultAt(t, resp, 1).ExceptionCode, "the first line is a record ClickHouse's positional reader refuses")
		assert.Len(t, pub.Messages, 2)
	})

	t.Run("bare with no header line ingests every row", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := newTestIngestHandler(t, testRegistry(t), pub)
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv", rows)))
		require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
		resp := decodeBatchResult(t, w)
		assert.Equal(t, 2, resp.Total)
		assert.Equal(t, 2, resp.Succeeded)
		assert.Len(t, pub.Messages, 2)
	})
}

// TestIngest_TSV is CSV's tab-separated twin, with the same positional contract
// and ClickHouse's own \N for null.
func TestIngest_TSV(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/tab-separated-values",
		"/a\tbuy\t3\te1\tacme\n/b\tsell\tnot-a-number\te2\tacme\n")))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	assert.Equal(t, 27, resultAt(t, resp, 2).ExceptionCode)
	require.Len(t, pub.Messages, 1)
	assert.Equal(t, "/a", publishedRow(t, pub.Messages[0].Data)["page"])
}

// TestIngest_CSV_IsAlwaysABatch: the positional formats have no arity question —
// a one-row CSV still answers with the per-record envelope, never {"ok":true}.
func TestIngest_CSV_IsAlwaysABatch(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv", "\"/a\",\"buy\",3,\"e1\",\"acme\"\n")))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
}

// TestIngest_CSV_EmptyBody names the format in the 400, like the NDJSON one.
func TestIngest_CSV_EmptyBody(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv", "  \n ")))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, jsonErrorMessage(t, w), "empty csv body")
	assert.Empty(t, pub.Messages)
}

// TestIngest_CSVWithNames: `text/csv; header=present` reads the first line as
// the column names, in any order. The header is not a record, so indices
// count data lines; a column the header omits takes its DEFAULT.
func TestIngest_CSVWithNames(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv; header=present",
		"org_id,page,count\nacme,/a,3\nacme,/b,not-a-number\nbeta,/c,5\n")))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total, "the header line is not a record")
	assert.Equal(t, 2, resp.Succeeded)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.False(t, resultAt(t, resp, 2).Ok)
	assert.NotZero(t, resultAt(t, resp, 2).ExceptionCode, "a parser refusal carries ClickHouse's code")
	assert.True(t, resultAt(t, resp, 3).Ok)
	require.Len(t, pub.Messages, 2)
	row := publishedRow(t, pub.Messages[0].Data)
	assert.Equal(t, "/a", row["page"])
	assert.Equal(t, float64(3), row["count"])
	assert.Equal(t, "acme", row["org_id"])
	assert.Equal(t, "", row["button"], "a column the header omits takes its DEFAULT")
	assert.Equal(t, "/c", publishedRow(t, pub.Messages[1].Data)["page"])
}

// TestIngest_TSVWithNames is the tab-separated twin.
func TestIngest_TSVWithNames(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/tab-separated-values; header=present",
		"count\tpage\n7\t/a\n")))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
	require.Len(t, pub.Messages, 1)
	assert.Equal(t, float64(7), publishedRow(t, pub.Messages[0].Data)["count"])
}

// TestIngest_WithNames_HeaderRefusals: a header ClickHouse refuses is a
// verdict on the body, not on a record — a whole-request 400 with its own
// code, and nothing published. A header alone is zero records.
func TestIngest_WithNames_HeaderRefusals(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		ct, body, mention string
	}{
		"unknown column":  {"text/csv; header=present", "page,extra\n/a,1\n", "extra"},
		"repeated column": {"text/csv; header=present", "page,page\n/a,/b\n", "page"},
		"tsv unknown":     {"text/tab-separated-values; header=present", "page\textra\n/a\t1\n", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, testRegistry(t), pub)
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, "clicks", tc.ct, tc.body)))

			require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
			msg, code := errorAndCode(t, w)
			assert.Equal(t, 117, code)
			assert.Equal(t, codeCHRejected, errorClass(t, w), "a whole-request parser refusal carries the class too")
			assert.Contains(t, msg, tc.mention)
			assert.Empty(t, pub.Messages)
		})
	}

	t.Run("header only", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := newTestIngestHandler(t, testRegistry(t), pub)
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv; header=present", "page,count\n")))

		require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
		assert.Equal(t, 0, decodeBatchResult(t, w).Total)
		assert.Empty(t, pub.Messages)
	})

	t.Run("empty body names the format", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := newTestIngestHandler(t, testRegistry(t), pub)
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "text/csv; header=present", " \n")))

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, jsonErrorMessage(t, w), "empty csvwithnames body")
	})
}

// TestIngest_WithNames_RoleProjection: the header is read against the ROLE's
// compiled schema, so a denied column in it is ClickHouse's 117 for the whole
// body, an `_eq` check column the header omits is filled by its injected
// DEFAULT, and a record that supplies another value fails the check (403).
func TestIngest_WithNames_RoleProjection(t *testing.T) {
	t.Parallel()
	required := "acme"
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	h.PolicySource = staticPolicy(&policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"writer": {Insert: &policy.InsertPermissions{
			DenyColumns: []string{"count"},
			Check:       map[string]policy.Filter{"org_id": {Eq: &required}},
		}}},
	}})
	send := func(body string) *httptest.ResponseRecorder {
		req := rawIngestRequest(t, "clicks", "text/csv; header=present", body)
		req = req.WithContext(auth.WithRole(req.Context(), "writer"))
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(req))
		return w
	}

	w := send("page,count\n/a,1\n")
	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	_, code := errorAndCode(t, w)
	assert.Equal(t, 117, code)
	assert.Empty(t, pub.Messages)

	w = send("page,org_id\n/a,acme\n/b,evil\n")
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.Contains(t, resultAt(t, resp, 2).Error, `check failed for column "org_id"`)

	w = send("page\n/c\n")
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.True(t, resultAt(t, decodeBatchResult(t, w), 1).Ok)
	require.Len(t, pub.Messages, 2)
	assert.Equal(t, "acme", publishedRow(t, pub.Messages[1].Data)["org_id"], "the omitted check column took its injected DEFAULT")
}

// TestIngest_LargeBatch_IndicesStayContiguous guards the one thing removing the
// handler's 500-record chunking could plausibly break. Verdicts used to be
// gathered per chunk and stitched back together by a staging slice; they are
// now one array from one Ingest call, indexed directly. A batch that straddles
// the old boundary must still report 1-based indices in order, with each
// record's own outcome — an off-by-one there would attribute a refusal to the
// wrong record, which the response gives a caller no way to detect.
func TestIngest_LargeBatch_IndicesStayContiguous(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)

	const n = 600
	bad := map[int]bool{1: true, 250: true, 500: true, 501: true, n: true} // 1-based
	var b strings.Builder
	b.WriteByte('[')
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteByte(',')
		}
		if bad[i] {
			fmt.Fprintf(&b, `{"page":"/p%d","nope":%d}`, i, i)
			continue
		}
		fmt.Fprintf(&b, `{"page":"/p%d"}`, i)
	}
	b.WriteByte(']')

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "application/json", b.String())))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	require.Equal(t, n, resp.Total)
	assert.Equal(t, len(bad), resp.Failed)
	assert.Equal(t, n-len(bad), resp.Succeeded)
	require.Len(t, resp.Results, n)
	for i, r := range resp.Results {
		require.Equal(t, i+1, r.Index, "results must be 1-based and in order")
		if bad[i+1] {
			assert.Equal(t, 117, r.ExceptionCode, "record %d", i+1)
			continue
		}
		assert.True(t, r.Ok, "record %d", i+1)
	}
	require.Len(t, pub.Messages, n-len(bad))
	// The published rows are the accepted records, in order, with the refused
	// ones simply absent — so record 502 sits four slots earlier than its index
	// (records 1, 250, 500 and 501 were refused before it). Spot-checking a row
	// on the far side of the old chunk boundary is what would catch a verdict
	// misattributed across it.
	assert.Equal(t, "/p502", publishedRow(t, pub.Messages[502-1-4].Data)["page"])
	assert.Equal(t, "/p2", publishedRow(t, pub.Messages[0].Data)["page"], "record 1 was refused")
	assert.Equal(t, "/p599", publishedRow(t, pub.Messages[len(pub.Messages)-1].Data)["page"], "record 600 was refused")
}
