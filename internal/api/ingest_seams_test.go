package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// recordingValidator observes the two schema-driven steps and can fail
// validation on demand, so a test can prove the handler goes through the seam
// rather than calling discovery directly.
type recordingValidator struct {
	validateErr    error
	validated      int
	canonicalized  int
	canonicalizeAs string // non-empty ⇒ stamp this into record["page"]
}

func (v *recordingValidator) Validate(_ *discovery.TableSchema, _ map[string]any) error {
	v.validated++
	return v.validateErr
}

func (v *recordingValidator) CanonicalizeTimestamps(_ *discovery.TableSchema, record map[string]any) {
	v.canonicalized++
	if v.canonicalizeAs != "" {
		record["page"] = v.canonicalizeAs
	}
}

// TestIngest_RecordValidatorSeam_IsUsed: a wired RecordValidator replaces both
// steps — its rejection is the record's rejection, and its rewrite is what gets
// published.
func TestIngest_RecordValidatorSeam_IsUsed(t *testing.T) {
	t.Parallel()

	t.Run("rejection surfaces as a 400", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		v := &recordingValidator{validateErr: errors.New("seam says no")}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		h.Validator = v

		w := httptest.NewRecorder()
		h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/home"}))

		assert.Equal(t, http.StatusBadRequest, w.Code)
		testutil.AssertJSONErrorResponse(t, w)
		assert.Contains(t, w.Body.String(), "seam says no")
		assert.Equal(t, 1, v.validated)
		assert.Zero(t, v.canonicalized, "a rejected record never reaches canonicalization")
		assert.Empty(t, pub.Messages)
	})

	t.Run("canonicalization rewrites the published record", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		v := &recordingValidator{canonicalizeAs: "/rewritten"}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		h.Validator = v

		w := httptest.NewRecorder()
		h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/home"}))

		require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
		assert.Equal(t, 1, v.validated)
		assert.Equal(t, 1, v.canonicalized)
		require.Len(t, pub.Messages, 1)
		assert.Contains(t, string(pub.Messages[0].Data), "/rewritten")
	})
}

// TestIngest_DefaultValidator_WhenUnwired: a handler with no seam wired still
// validates against the schema — the nil case must not read as "allow".
func TestIngest_DefaultValidator_WhenUnwired(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	require.Nil(t, h.Validator)
	assert.IsType(t, discoveryValidator{}, h.validator())

	w := httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"nonexistent_field": 1}))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	testutil.AssertJSONErrorResponse(t, w)
	assert.Empty(t, pub.Messages)
}

// alwaysChecker answers every insert check the same way, so a test can tell
// which arm the handler consulted.
type alwaysChecker struct {
	matches bool
	inSet   bool
}

func (c alwaysChecker) Matches(_, _ any) bool     { return c.matches }
func (c alwaysChecker) InSet(_ any, _ []any) bool { return c.inSet }

// TestIngest_InsertCheckerSeam_IsUsed: a wired InsertChecker decides the check
// clause. The record here would fail the canonical comparison, so a 200 proves
// the seam — not the default — answered.
func TestIngest_InsertCheckerSeam_IsUsed(t *testing.T) {
	t.Parallel()
	required := "org-allowed"
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Insert: &policy.InsertPermissions{
			Check: map[string]policy.Filter{"org_id": {Eq: &required}},
		}}},
	}}

	for _, tt := range []struct {
		name    string
		matches bool
		want    int
	}{
		{"seam admits a value the canonical comparison would reject", true, http.StatusOK},
		{"seam rejects a value the canonical comparison would admit", false, http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
			h.PolicySource = policy.Static(p)
			h.Checker = alwaysChecker{matches: tt.matches}

			value := "org-something-else"
			if !tt.matches {
				value = required // the default checker would accept this
			}
			w := httptest.NewRecorder()
			h.Handle(w, viewerIngestRequest(t, "clicks", map[string]any{"page": "/a", "org_id": value}))
			assert.Equal(t, tt.want, w.Code, "body=%s", w.Body.String())
			if tt.want == http.StatusForbidden {
				testutil.AssertJSONErrorResponse(t, w)
			}
		})
	}
}

// TestIngest_DefaultChecker_WhenUnwired: with no seam wired the canonical
// comparison decides, and a mismatched value is still a 403.
func TestIngest_DefaultChecker_WhenUnwired(t *testing.T) {
	t.Parallel()
	required := "org-allowed"
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Insert: &policy.InsertPermissions{
			Check: map[string]policy.Filter{"org_id": {Eq: &required}},
		}}},
	}}
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(p)
	require.Nil(t, h.Checker)
	assert.IsType(t, canonicalChecker{}, h.checker())

	w := httptest.NewRecorder()
	h.Handle(w, viewerIngestRequest(t, "clicks", map[string]any{"page": "/a", "org_id": "wrong"}))
	assert.Equal(t, http.StatusForbidden, w.Code)
	testutil.AssertJSONErrorResponse(t, w)
	assert.Empty(t, pub.Messages)
}

// TestIngest_SeamOrdering_ChecksSitBetweenValidateAndCanonicalize: the two
// RecordValidator calls stay at their current positions with the check-clause
// block between them. Merging them would move check clauses onto canonicalized
// values, silently changing pre-#372 check semantics.
func TestIngest_SeamOrdering_ChecksSitBetweenValidateAndCanonicalize(t *testing.T) {
	t.Parallel()
	required := "org-allowed"
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Insert: &policy.InsertPermissions{
			Check: map[string]policy.Filter{"org_id": {Eq: &required}},
		}}},
	}}
	pub := &testutil.MockPublisher{}
	v := &recordingValidator{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(p)
	h.Validator = v

	// A failing check must land AFTER Validate and BEFORE canonicalization.
	w := httptest.NewRecorder()
	h.Handle(w, viewerIngestRequest(t, "clicks", map[string]any{"page": "/a", "org_id": "wrong"}))
	require.Equal(t, http.StatusForbidden, w.Code)
	testutil.AssertJSONErrorResponse(t, w)
	assert.Equal(t, 1, v.validated, "validation runs before the check clauses")
	assert.Zero(t, v.canonicalized, "canonicalization runs after them, so a failed check never reaches it")
}

// viewerIngestRequest is ingestRequest with the "viewer" role in context, for
// the policy-gated seam tests.
func viewerIngestRequest(t *testing.T, table string, body map[string]any) *http.Request {
	t.Helper()
	req := ingestRequest(t, table, body)
	return req.WithContext(auth.WithRole(req.Context(), "viewer"))
}
