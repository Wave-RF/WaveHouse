package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// recordingEvaluator answers every row the same way and counts the calls, so a
// test can prove both delivery paths reach row-level security through the seam.
type recordingEvaluator struct {
	visible bool
	calls   int
}

func (e *recordingEvaluator) Visible(*policy.ResolvedPermissions, map[string]any, map[string]policy.ColumnSpec) bool {
	e.calls++
	return e.visible
}

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
	// withhold case proved nothing — `seam.Visible(…) || perms.RowVisible(…)`
	// would have passed it, since the policy withheld the row anyway.
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
			if tt.visible {
				f, _, _ := recvEvent(t, sub)
				assert.NotEmpty(t, f.Data)
			} else {
				assertNoFrame(t, sub)
			}
		})
	}
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
}

// TestHub_DefaultRowEvaluator_WhenUnwired: an un-wired Hub still enforces
// row-level security. A nil seam must never read as "everything is visible".
func TestHub_DefaultRowEvaluator_WhenUnwired(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.Static(filteredPolicy()), nil, nil)
	require.Nil(t, hub.RowEvaluator)
	assert.IsType(t, policyRowEvaluator{}, hub.rowEvaluator())

	sub := NewSubscriber(map[string]any{"tenant": "t1"}, nil)
	hub.Add("ingest.clicks", "viewer", sub)
	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "t2", "page": "/a"}))
	assertNoFrame(t, sub)

	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"tenant_id": "t1", "page": "/a"}))
	f, _, _ := recvEvent(t, sub)
	assert.NotEmpty(t, f.Data)
}
