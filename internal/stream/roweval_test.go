package stream

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// recordingEvaluator answers every row the same way and counts what the Hub
// does, so a test can prove both delivery paths reach row-level security through
// the seam — and that each path parses once and closes what it parsed.
type recordingEvaluator struct {
	visible  bool
	prepErr  error
	prepares int
	calls    int
	closes   int
}

func (e *recordingEvaluator) Prepare(string, []string, []byte) (RowView, error) {
	e.prepares++
	if e.prepErr != nil {
		return nil, e.prepErr
	}
	return &recordingView{e: e}, nil
}

type recordingView struct{ e *recordingEvaluator }

func (v *recordingView) Visible(*policy.ResolvedPermissions) (bool, string) {
	v.e.calls++
	if v.e.visible {
		return true, ""
	}
	return false, ReasonFilter
}

func (v *recordingView) Close() { v.e.closes++ }

// filteredPolicy grants "viewer" a row-filter, which is what puts the Hub on
// the per-subscriber admission path in the first place.
func filteredPolicy() *policy.Policy {
	tmpl := "{{ jwt.tenant }}"
	return &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"viewer": {Select: &policy.SelectPermissions{
					Filter: map[string]policy.Filter{"tenant_id": {Eq: &tmpl}},
				}},
			},
		},
	}
}

// TestHub_RowEvaluatorSeam_LiveBroadcast: a wired RowEvaluator decides delivery
// on the live fan-out. Its verdict overrides what the real predicate would say
// — the row here does not match the filter, so a delivered frame proves the
// seam answered.
func TestHub_RowEvaluatorSeam_LiveBroadcast(t *testing.T) {
	t.Parallel()
	// The tenant is chosen per case so each subtest builds the scenario its name
	// describes. With both cases sending a row the predicate withholds, the
	// withhold case proved nothing — a seam consulted only as a veto would have
	// passed it, since the policy withheld the row anyway.
	for _, tt := range []struct {
		name     string
		visible  bool
		tenantID string
	}{
		{"seam admits a row the predicate would withhold", true, "t2"},
		{"seam withholds a row the predicate would admit", false, "t1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			eval := &recordingEvaluator{visible: tt.visible}
			hub := NewHub(policy.Static(filteredPolicy()), nil, nil)
			hub.RowEvaluator = eval

			sub := NewSubscriber(map[string]any{"tenant": "t1"}, nil)
			hub.Add("ingest.clicks", "viewer", sub)

			// The claim is "t1", so tenant_id "t2" is a row the real predicate
			// withholds and "t1" is one it admits — the seam's verdict must win
			// either way.
			hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
				map[string]any{"tenant_id": tt.tenantID, "page": "/a"}))

			assert.Equal(t, 1, eval.calls, "the live path must consult the seam")
			assert.Equal(t, 1, eval.prepares, "the row is parsed once for the event")
			assert.Equal(t, 1, eval.closes, "and released after the fan-out")
			if tt.visible {
				f, _, _ := recvEvent(t, sub)
				assert.NotEmpty(t, f.Data)
			} else {
				assertNoFrame(t, sub)
			}
		})
	}
}

// TestHub_RowEvaluatorSeam_PreparesOncePerEvent: parsing the row is the
// expensive half of a row-filter decision, so it happens once per EVENT, not
// once per subscriber or once per role — that ratio is the whole reason the seam
// is split into Prepare and Visible.
func TestHub_RowEvaluatorSeam_PreparesOncePerEvent(t *testing.T) {
	t.Parallel()
	p := filteredPolicy()
	// A second filtered role on the same table: the parse must be shared across
	// roles too, not just across a role's subscribers.
	tmpl := "{{ jwt.tenant }}"
	p.Tables["clicks"]["editor"] = policy.RolePermissions{Select: &policy.SelectPermissions{
		Filter: map[string]policy.Filter{"tenant_id": {Eq: &tmpl}},
	}}

	eval := &recordingEvaluator{visible: true}
	hub := NewHub(policy.Static(p), nil, nil)
	hub.RowEvaluator = eval
	for _, role := range []string{"viewer", "viewer", "viewer", "editor"} {
		hub.Add("ingest.clicks", role, NewSubscriber(map[string]any{"tenant": "t1"}, nil))
	}

	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "t1", "page": "/a"}))

	assert.Equal(t, 1, eval.prepares, "one parse serves every role and subscriber")
	assert.Equal(t, 4, eval.calls, "visibility is still decided per subscriber")
	assert.Equal(t, 1, eval.closes)
}

// TestHub_RowEvaluatorSeam_PrepareError_WithholdsFilteredRolesOnly: when the row
// cannot be prepared at all — no compiled schema, a column list that is not the
// compiled generation's, an unparseable row — every row-filtered subscriber is
// withheld. Roles WITHOUT a row-filter never asked the type layer anything, so
// they must be unaffected: a table whose schema handle is briefly unavailable
// must not black out the streams that do not depend on it.
func TestHub_RowEvaluatorSeam_PrepareError_WithholdsFilteredRolesOnly(t *testing.T) {
	t.Parallel()
	p := filteredPolicy()
	p.Tables["clicks"]["public"] = policy.RolePermissions{Select: &policy.SelectPermissions{}}

	eval := &recordingEvaluator{visible: true, prepErr: errors.New("boom")}
	hub := NewHub(policy.Static(p), nil, nil)
	hub.RowEvaluator = eval

	filtered := NewSubscriber(map[string]any{"tenant": "t1"}, nil)
	unfiltered := NewSubscriber(nil, nil)
	hub.Add("ingest.clicks", "viewer", filtered)
	hub.Add("ingest.clicks", "public", unfiltered)

	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "t1", "page": "/a"}))

	assertNoFrame(t, filtered)
	f, _, _ := recvEvent(t, unfiltered)
	assert.NotEmpty(t, f.Data, "an unfiltered role never consults the type layer, so it is unaffected")
	assert.Zero(t, eval.calls, "a row that could not be prepared is never asked about")
}

// TestHub_RowEvaluatorSeam_Replay: the gap-fill path goes through the same seam
// as the live path, so the two can't drift on how row visibility is decided.
func TestHub_RowEvaluatorSeam_Replay(t *testing.T) {
	t.Parallel()
	eval := &recordingEvaluator{visible: false}
	hub := NewHub(policy.Static(filteredPolicy()), nil, nil)
	hub.RowEvaluator = eval

	project := hub.ReplayProjector("viewer", NewSubscriber(map[string]any{"tenant": "t1"}, nil))
	// tenant_id "t1" matches the claim: the real predicate would admit this row.
	frames := project(rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "t1", "page": "/a"}))

	assert.Empty(t, frames, "the seam's verdict decides replay too")
	assert.Equal(t, 1, eval.calls)
	assert.Equal(t, 1, eval.prepares)
	assert.Equal(t, 1, eval.closes, "a gap-fill of thousands of events must not accumulate parsed rows")
}

// TestHub_DefaultRowEvaluator_WhenUnwired: an un-wired Hub must never read as
// "everything is visible". With the type layer behind the seam there is nothing
// left in-process that can evaluate a filter, so the only safe default is to
// withhold every row of every row-filtered role — including the rows the
// predicate would have admitted, which is what makes this a real assertion
// rather than a restatement of the policy.
func TestHub_DefaultRowEvaluator_WhenUnwired(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.Static(filteredPolicy()), nil, nil)
	require.Nil(t, hub.RowEvaluator)
	assert.IsType(t, &engineEvaluator{}, hub.rowEvaluator(),
		"the default must be the engine-backed evaluator with no engine, not a permissive stub")

	sub := NewSubscriber(map[string]any{"tenant": "t1"}, nil)
	hub.Add("ingest.clicks", "viewer", sub)

	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "t2", "page": "/a"}))
	assertNoFrame(t, sub)

	// The row the filter WOULD admit is withheld too: no engine, no verdict.
	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"tenant_id": "t1", "page": "/a"}))
	assertNoFrame(t, sub)

	assert.Empty(t, hub.ReplayProjector("viewer", sub)(rawEvent(t, "clicks", "2026-06-26T00:00:02Z",
		map[string]any{"tenant_id": "t1", "page": "/a"})), "replay fails closed on the same grounds")
}

// TestNewRowEvaluator_NilEngineFailsClosed: the constructor accepts a nil Engine
// (a boot path that could not open one still has to build a Hub) and answers the
// same way the unwired default does, with the reason an operator needs. The
// reason matters: `unavailable` says the estate is broken, where `filter` would
// say the policy is working.
func TestNewRowEvaluator_NilEngineFailsClosed(t *testing.T) {
	t.Parallel()
	view, err := NewRowEvaluator(nil, nil).Prepare("clicks", []string{"page"}, []byte(`["/a"]`))
	require.Error(t, err)
	assert.Nil(t, view)
	assert.Equal(t, ReasonUnavailable, WithheldReason(err))
}

// TestWithheldReason_ClassifiesTypeLayerErrors: the three fault causes are
// operationally different and must not collapse into one label. An error from
// another evaluator that names no reason reads as "error" rather than being
// lost.
func TestWithheldReason_ClassifiesTypeLayerErrors(t *testing.T) {
	t.Parallel()
	assert.Equal(t, ReasonUnavailable,
		WithheldReason(classifyPrepare(&typelayer.Unavailable{Table: "clicks", Cause: "no artifact"})))
	assert.Equal(t, ReasonDrift,
		WithheldReason(classifyPrepare(fmt.Errorf("wrapped: %w", typelayer.ErrColumnsDrift))))
	assert.Equal(t, ReasonError, WithheldReason(classifyPrepare(errors.New("unparseable row"))))
	assert.Equal(t, ReasonError, WithheldReason(errors.New("an evaluator that names no reason")))
}
