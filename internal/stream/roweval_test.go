package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
	"github.com/Wave-RF/WaveHouse/internal/typelayer/typelayertest"
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
	tenants  []tenant.ID
}

func (e *recordingEvaluator) Prepare(id tenant.ID, _ string, _ []string, _ json.RawMessage) (RowView, error) {
	e.prepares++
	e.tenants = append(e.tenants, id)
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
			hub := NewHub(staticPolicy(filteredPolicy()), nil, nil)
			hub.RowEvaluator = eval

			sub := NewSubscriber(map[string]any{"tenant": "t1"}, nil)
			hub.Add(topicOf("clicks"), "viewer", sub)

			// The claim is "t1", so tenant_id "t2" is a row the real predicate
			// withholds and "t1" is one it admits — the seam's verdict must win
			// either way.
			hub.Broadcast(topicOf("clicks"), rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
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
	hub := NewHub(staticPolicy(p), nil, nil)
	hub.RowEvaluator = eval
	for _, role := range []string{"viewer", "viewer", "viewer", "editor"} {
		hub.Add(topicOf("clicks"), role, NewSubscriber(map[string]any{"tenant": "t1"}, nil))
	}

	hub.Broadcast(topicOf("clicks"), rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "t1", "page": "/a"}))

	assert.Equal(t, 1, eval.prepares, "one parse serves every role and subscriber")
	assert.Equal(t, 4, eval.calls, "visibility is still decided per subscriber")
	assert.Equal(t, 1, eval.closes)
}

// TestHub_RowEvaluatorSeam_PreparesForTheTopicsTenant: the row is prepared
// against the tenant whose topic carried it on the live path, and against the
// connection's tenant on a gap-fill — the type layer keeps one schema per
// tenant, and two tenants' tables of one name are different tables.
func TestHub_RowEvaluatorSeam_PreparesForTheTopicsTenant(t *testing.T) {
	t.Parallel()
	eval := &recordingEvaluator{visible: true}
	hub := NewHub(staticPolicy(filteredPolicy()), nil, nil)
	hub.RowEvaluator = eval
	acme := topicOf("clicks")
	acme.Tenant = "acme"
	hub.Add(acme, "viewer", NewSubscriber(map[string]any{"tenant": "t1"}, nil))

	raw := rawEvent(t, "clicks", "2026-06-26T00:00:00Z", map[string]any{"tenant_id": "t1", "page": "/a"})
	hub.Broadcast(acme, raw)
	hub.ReplayProjector("globex", "viewer", NewSubscriber(map[string]any{"tenant": "t1"}, nil))(raw)

	assert.Equal(t, []tenant.ID{"acme", "globex"}, eval.tenants)
}

// TestHub_RowEvaluatorSeam_PrepareError_WithholdsFilteredRolesOnly: when the row
// cannot be prepared at all — no compiled schema, a column list the table
// cannot have — every row-filtered subscriber is withheld. Roles WITHOUT a
// row-filter never asked the type layer anything, so they must be unaffected: a
// table whose schema handle is briefly unavailable must not black out the
// streams that do not depend on it.
func TestHub_RowEvaluatorSeam_PrepareError_WithholdsFilteredRolesOnly(t *testing.T) {
	t.Parallel()
	p := filteredPolicy()
	p.Tables["clicks"]["public"] = policy.RolePermissions{Select: &policy.SelectPermissions{}}

	eval := &recordingEvaluator{visible: true, prepErr: errors.New("boom")}
	hub := NewHub(staticPolicy(p), nil, nil)
	hub.RowEvaluator = eval

	filtered := NewSubscriber(map[string]any{"tenant": "t1"}, nil)
	unfiltered := NewSubscriber(nil, nil)
	hub.Add(topicOf("clicks"), "viewer", filtered)
	hub.Add(topicOf("clicks"), "public", unfiltered)

	hub.Broadcast(topicOf("clicks"), rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
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
	hub := NewHub(staticPolicy(filteredPolicy()), nil, nil)
	hub.RowEvaluator = eval

	project := hub.ReplayProjector(tenant.Default, "viewer", NewSubscriber(map[string]any{"tenant": "t1"}, nil))
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
	hub := NewHub(staticPolicy(filteredPolicy()), nil, nil)
	require.Nil(t, hub.RowEvaluator)
	assert.IsType(t, &engineEvaluator{}, hub.rowEvaluator(),
		"the default must be the engine-backed evaluator with no engine, not a permissive stub")

	sub := NewSubscriber(map[string]any{"tenant": "t1"}, nil)
	hub.Add(topicOf("clicks"), "viewer", sub)

	hub.Broadcast(topicOf("clicks"), rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "t2", "page": "/a"}))
	assertNoFrame(t, sub)

	// The row the filter WOULD admit is withheld too: no engine, no verdict.
	hub.Broadcast(topicOf("clicks"), rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"tenant_id": "t1", "page": "/a"}))
	assertNoFrame(t, sub)

	assert.Empty(t, hub.ReplayProjector(tenant.Default, "viewer", sub)(rawEvent(t, "clicks", "2026-06-26T00:00:02Z",
		map[string]any{"tenant_id": "t1", "page": "/a"})), "replay fails closed on the same grounds")
}

// TestNewRowEvaluator_NilEngineFailsClosed: the constructor accepts a nil Engine
// (a process with no type layer still has to build a Hub) and answers the same
// way the unwired default does, with the reason an operator needs. The reason
// matters: `unavailable` says the estate is broken, where `filter` would say
// the policy is working.
func TestNewRowEvaluator_NilEngineFailsClosed(t *testing.T) {
	t.Parallel()
	view, err := NewRowEvaluator(nil).Prepare(tenant.Default, "clicks", []string{"page"}, json.RawMessage(`["/a"]`))
	require.Error(t, err)
	assert.Nil(t, view)
	assert.Equal(t, ReasonUnavailable, WithheldReason(err))
}

// TestWithheldReason_ClassifiesTypeLayerErrors: the fault causes are
// operationally different and must not collapse into one label. An error from
// another evaluator that names no reason reads as "error" rather than being
// lost.
func TestWithheldReason_ClassifiesTypeLayerErrors(t *testing.T) {
	t.Parallel()
	assert.Equal(t, ReasonUnavailable,
		WithheldReason(classifyPrepare(&typelayer.Unavailable{Tenant: tenant.Default, Table: "clicks", Cause: "no artifact"})))
	assert.Equal(t, ReasonDrift,
		WithheldReason(classifyPrepare(fmt.Errorf("wrapped: %w", typelayer.ErrColumnsDrift))))
	assert.Equal(t, ReasonError, WithheldReason(classifyPrepare(errors.New("the parse call was refused"))))
	assert.Equal(t, ReasonError, WithheldReason(errors.New("an evaluator that names no reason")))
}

// TestEngineEvaluator_UnreadableRowIsDeclined: a row the type layer cannot
// read is not a Prepare failure. Prepare succeeds, the filter does not answer
// for the row, and the withhold is labelled decline, whatever the shape: a
// value the column cannot read, a short row, a truncated array, an object, not
// JSON at all (measured on the 26.8 artifact). The hub refuses the last four
// before Prepare, as they do not pair with the column list; they are here to
// pin the label.
func TestEngineEvaluator_UnreadableRowIsDeclined(t *testing.T) {
	t.Parallel()
	eval := NewRowEvaluator(typelayertest.TestEngine(t,
		chtypesTable("clicks", col("page", "UInt32"), col("secret", "String"), col("tenant_id", "String"))))
	cols := []string{"page", "secret", "tenant_id"}
	perms := policy.Evaluate(filteredPolicy(), "viewer", "clicks", "select", map[string]any{"tenant": "acme"})

	view, err := eval.Prepare(tenant.Default, "clicks", cols, json.RawMessage(`[1,"x","acme"]`))
	require.NoError(t, err)
	visible, reason := view.Visible(perms)
	view.Close()
	require.True(t, visible, "control: a readable row is judged: %s", reason)

	for _, row := range []string{`["abc","x","acme"]`, `[-1,"x","acme"]`, `[1,"x"]`, `[1,"x","acme"`, `{"page":1}`, `not json`} {
		view, err := eval.Prepare(tenant.Default, "clicks", cols, json.RawMessage(row))
		require.NoError(t, err, row)
		visible, reason := view.Visible(perms)
		view.Close()
		assert.False(t, visible, row)
		assert.Equal(t, ReasonDecline, reason, row)
	}
}

// TestEngineEvaluator_TenantsAreIndependent: the production evaluator resolves
// the EVENT's tenant's table. A tenant the type layer has not bound is
// unavailable on its own, while a bound tenant's row of the same table name is
// judged normally.
func TestEngineEvaluator_TenantsAreIndependent(t *testing.T) {
	t.Parallel()
	eng := typelayertest.TestEngine(t, clicksTable())
	eng.Bind("acme", typelayertest.TestServerVersion, "UTC", []*discovery.TableSchema{clicksTable()})
	eval := NewRowEvaluator(eng)
	cols := []string{"page", "secret", "tenant_id"}
	row := json.RawMessage(`["/a","x","acme"]`)
	perms := policy.Evaluate(filteredPolicy(), "viewer", "clicks", "select", map[string]any{"tenant": "acme"})

	for _, id := range []tenant.ID{tenant.Default, "acme"} {
		view, err := eval.Prepare(id, "clicks", cols, row)
		require.NoError(t, err, id)
		visible, reason := view.Visible(perms)
		view.Close()
		assert.True(t, visible, "%s: %s", id, reason)
	}

	_, err := eval.Prepare("globex", "clicks", cols, row)
	require.Error(t, err)
	assert.Equal(t, ReasonUnavailable, WithheldReason(err), "an unbound tenant is unavailable, not a verdict")

	_, err = eval.Prepare("acme", "views", cols, row)
	assert.Equal(t, ReasonUnavailable, WithheldReason(err), "a table the tenant does not have is unavailable")
}

// roleWidthTable is a table some roles may write only part of. "region" has a
// DEFAULT the stream's parse fills in, which is exactly why a filter over it
// must still withhold when the event does not carry it.
func roleWidthTable() *discovery.TableSchema {
	return chtypesTable("clicks",
		col("page", "String"),
		col("secret", "String"),
		discovery.Column{Name: "region", Type: "String", DefaultKind: "DEFAULT", DefaultExpression: "'eu'"},
		col("tenant_id", "String"))
}

// TestHub_RowFilter_RoleWidthEnvelope: ingest publishes each row with the
// INSERTING role's columns, so a role that may not write "secret" or "region"
// publishes a narrower envelope than the table. Such a row is evaluated, not
// withheld as drift: it reaches the subscriber whose claim matches and is
// withheld from the one whose claim does not.
func TestHub_RowFilter_RoleWidthEnvelope(t *testing.T) {
	t.Parallel()
	hub := chtypesHub(t, staticPolicy(rowFilterPolicy()), nil, roleWidthTable())
	acme := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	globex := NewSubscriber(map[string]any{"tenant": "globex"}, nil)
	hub.Add(topicOf("clicks"), "viewer", acme)
	hub.Add(topicOf("clicks"), "viewer", globex)

	hub.Broadcast(topicOf("clicks"), rawEventCols(t, "clicks", "t1", []string{"page", "tenant_id"},
		map[string]any{"page": "/a", "tenant_id": "acme"}))

	_, cols, row := recvEvent(t, acme)
	assert.Equal(t, []string{"page"}, cols)
	assert.Equal(t, "/a", row["page"])
	assertNoFrame(t, globex)

	frames := hub.ReplayProjector(tenant.Default, "viewer", NewSubscriber(map[string]any{"tenant": "acme"}, nil))(
		rawEventCols(t, "clicks", "t1", []string{"page", "tenant_id"}, map[string]any{"page": "/a", "tenant_id": "acme"}))
	assert.Len(t, frames, 2, "replay evaluates the narrower envelope the same way")
}

// TestHub_RowFilter_PermutedEnvelope: the row is positional against the
// envelope's own list, whatever its order — the k-th cell is the k-th named
// column, full width or narrower. The announced list follows the envelope, so
// the client zips each value under its own name.
func TestHub_RowFilter_PermutedEnvelope(t *testing.T) {
	t.Parallel()
	p := rowFilterPolicy()
	p.Tables["clicks"]["auditor"] = policy.RolePermissions{Select: &policy.SelectPermissions{
		Filter: map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}},
	}}
	hub := chtypesHub(t, staticPolicy(p), nil, roleWidthTable())

	for _, cols := range [][]string{
		{"tenant_id", "region", "secret", "page"},
		{"tenant_id", "page"},
	} {
		acme := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
		globex := NewSubscriber(map[string]any{"tenant": "globex"}, nil)
		hub.Add(topicOf("clicks"), "auditor", acme)
		hub.Add(topicOf("clicks"), "auditor", globex)

		hub.Broadcast(topicOf("clicks"), rawEventCols(t, "clicks", "t1", cols,
			map[string]any{"page": "/a", "tenant_id": "acme", "secret": "s", "region": "us"}))

		_, announced, row := recvEvent(t, acme)
		assert.Equal(t, cols, announced, "the announcement follows the envelope's order")
		assert.Equal(t, "/a", row["page"])
		assert.Equal(t, "acme", row["tenant_id"])
		assertNoFrame(t, globex)

		hub.Remove(topicOf("clicks"), "auditor", acme)
		hub.Remove(topicOf("clicks"), "auditor", globex)
	}
}

// TestHub_RowFilter_FilterOnAbsentColumnWithholds: a filter over a column the
// event does not carry fails closed for that subscriber, even where the
// table's DEFAULT ("eu") would satisfy it — the stored row's DEFAULT is
// computed again at insert, and a stream verdict on its own copy could admit
// a row the query path excludes. The reason is decline, not filter: it is no
// policy verdict. The control delivers the same row once it carries the value.
func TestHub_RowFilter_FilterOnAbsentColumnWithholds(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Select: &policy.SelectPermissions{
			Filter: map[string]policy.Filter{"region": {Eq: new("eu")}},
		}}},
	}}
	eng := typelayertest.TestEngine(t, roleWidthTable())
	hub := NewHub(staticPolicy(p), nil, nil)
	hub.RowEvaluator = NewRowEvaluator(eng)
	sub := NewSubscriber(nil, nil)
	hub.Add(topicOf("clicks"), "viewer", sub)

	narrow := []string{"page", "tenant_id"}
	hub.Broadcast(topicOf("clicks"), rawEventCols(t, "clicks", "t1", narrow,
		map[string]any{"page": "/a", "tenant_id": "acme"}))
	assertNoFrame(t, sub)

	view, err := NewRowEvaluator(eng).Prepare(tenant.Default, "clicks", narrow, json.RawMessage(`["/a","acme"]`))
	require.NoError(t, err)
	visible, reason := view.Visible(policy.Evaluate(p, "viewer", "clicks", "select", nil))
	view.Close()
	assert.False(t, visible)
	assert.Equal(t, ReasonDecline, reason)

	hub.Broadcast(topicOf("clicks"), rawEventCols(t, "clicks", "t2", []string{"page", "region"},
		map[string]any{"page": "/b", "region": "eu"}))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, "/b", row["page"])
}

// TestHub_RowFilter_UnknownColumnIsDrift: an envelope naming a column the
// table does not have (dropped or renamed since it was published) cannot be
// read at all, so it is withheld from every row-filtered subscriber as drift.
// A role without a row filter never asks the type layer and still gets it.
func TestHub_RowFilter_UnknownColumnIsDrift(t *testing.T) {
	t.Parallel()
	p := rowFilterPolicy()
	p.Tables["clicks"]["public"] = policy.RolePermissions{Select: &policy.SelectPermissions{}}
	eng := typelayertest.TestEngine(t, roleWidthTable())
	hub := NewHub(staticPolicy(p), nil, nil)
	hub.RowEvaluator = NewRowEvaluator(eng)
	acme := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	public := NewSubscriber(nil, nil)
	hub.Add(topicOf("clicks"), "viewer", acme)
	hub.Add(topicOf("clicks"), "public", public)

	cols := []string{"page", "tenant_id", "bogus"}
	hub.Broadcast(topicOf("clicks"), rawEventCols(t, "clicks", "t1", cols,
		map[string]any{"page": "/a", "tenant_id": "acme", "bogus": "x"}))
	assertNoFrame(t, acme)
	_, announced, _ := recvEvent(t, public)
	assert.Equal(t, cols, announced)

	_, err := NewRowEvaluator(eng).Prepare(tenant.Default, "clicks", cols, json.RawMessage(`["/a","acme","x"]`))
	require.Error(t, err)
	assert.Equal(t, ReasonDrift, WithheldReason(err))
}
