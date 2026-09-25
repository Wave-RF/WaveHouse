// Package dedupetest is the conformance suite every dedupe backend runs: the
// Deduplicator contract (dedupe.go) as tests, driven through the production
// path — a backend's Factory and the Managed switch it returns — so a backend
// that passes here behaves the same under ingest as every other.
package dedupetest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Harness is one fresh backend under test.
type Harness struct {
	// Factory builds a tenant's store over the backend. The suite switches
	// each store it builds on and closes it at cleanup.
	Factory dedupe.Factory
	// Peer, if set, builds a tenant's store over the same data through a
	// second client — another process's view, for backends that have one.
	// nil uses Factory.
	Peer dedupe.Factory
	// Advance moves the backend's clock forward by d. nil makes the suite
	// sleep instead, which is why its leases and retentions are whole
	// seconds: a backend may store expiry at one-second resolution.
	Advance func(d time.Duration)
	// FailNextReserve, if set, makes the backend's next Reserve fail after it
	// has claimed n keys. nil skips the case that needs it.
	FailNextReserve func(n int)
}

// Run runs every case, each against a backend newHarness builds fresh.
func Run(t *testing.T, newHarness func(t *testing.T) Harness) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.run(t, &suite{Harness: newHarness(t)})
		})
	}
}

// Mark is the old check-and-mark in one call, for tests that only need an id
// seen: it reserves k and commits it with no expiry, reporting whether k was
// already committed. A key another request holds is an error.
func Mark(ctx context.Context, d dedupe.Deduplicator, k dedupe.Key) (duplicate bool, err error) {
	claims, err := d.Reserve(ctx, []dedupe.Key{k}, dedupe.DefaultLease)
	if err != nil {
		return false, err
	}
	switch claims[0].Status {
	case dedupe.Duplicate:
		return true, nil
	case dedupe.Claimed:
		return false, d.Commit(ctx, claims, 0)
	case dedupe.InFlight:
	}
	return false, fmt.Errorf("key %v is %s", k, claims[0].Status)
}

const (
	lease = time.Second
	// long outlives every case, so only a deliberate pass lapses it.
	long = time.Hour
)

type suite struct {
	Harness
}

func (s *suite) open(t *testing.T, build dedupe.Factory, id tenant.ID) dedupe.Deduplicator {
	t.Helper()
	m := build(id)
	require.NoError(t, m.Apply(true))
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// store opens tenant id's store; peer opens it through the second client.
func (s *suite) store(t *testing.T, id tenant.ID) dedupe.Deduplicator {
	t.Helper()
	return s.open(t, s.Factory, id)
}

func (s *suite) peer(t *testing.T, id tenant.ID) dedupe.Deduplicator {
	t.Helper()
	if s.Peer == nil {
		return s.store(t, id)
	}
	return s.open(t, s.Peer, id)
}

// pass lets d go by, plus a second's margin for a backend that stores expiry
// in whole seconds.
func (s *suite) pass(d time.Duration) {
	d += time.Second
	if s.Advance != nil {
		s.Advance(d)
		return
	}
	time.Sleep(d)
}

func reserve(t *testing.T, d dedupe.Deduplicator, lease time.Duration, keys ...dedupe.Key) []dedupe.Claim {
	t.Helper()
	claims, err := d.Reserve(t.Context(), keys, lease)
	require.NoError(t, err)
	require.Len(t, claims, len(keys))
	for i, c := range claims {
		require.Equal(t, keys[i], c.Key, "claim %d answers its own key, in input order", i)
	}
	return claims
}

func statuses(claims []dedupe.Claim) []dedupe.Status {
	out := make([]dedupe.Status, len(claims))
	for i, c := range claims {
		out[i] = c.Status
	}
	return out
}

func key(id string) dedupe.Key { return dedupe.Key{Table: "events", ID: id} }

var cases = []struct {
	name string
	run  func(t *testing.T, s *suite)
}{
	{"claim then commit is a duplicate", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		c := reserve(t, d, long, key("e1"))
		require.Equal(t, dedupe.Claimed, c[0].Status)
		assert.NotEmpty(t, c[0].Token)
		require.NoError(t, d.Commit(t.Context(), c, 0))
		assert.Equal(t, dedupe.Duplicate, reserve(t, d, long, key("e1"))[0].Status)
		assert.Equal(t, dedupe.Claimed, reserve(t, d, long, key("e2"))[0].Status, "distinct ids are independent")
	}},
	{"a released claim can be claimed again", func(t *testing.T, s *suite) {
		// #384: a publish that failed releases the id, and the client's retry
		// goes through.
		d := s.store(t, "acme")
		c := reserve(t, d, long, key("e1"))
		require.NoError(t, d.Release(t.Context(), c))
		c = reserve(t, d, long, key("e1"))
		assert.Equal(t, dedupe.Claimed, c[0].Status)
		require.NoError(t, d.Commit(t.Context(), c, 0))
		assert.Equal(t, dedupe.Duplicate, reserve(t, d, long, key("e1"))[0].Status)
	}},
	{"a live claim is in flight to everyone else", func(t *testing.T, s *suite) {
		d, p := s.store(t, "acme"), s.peer(t, "acme")
		c := reserve(t, d, long, key("e1"))
		assert.Equal(t, dedupe.InFlight, reserve(t, d, long, key("e1"))[0].Status)
		assert.Equal(t, dedupe.InFlight, reserve(t, p, long, key("e1"))[0].Status, "and to another client")
		require.NoError(t, d.Commit(t.Context(), c, 0))
		assert.Equal(t, dedupe.Duplicate, reserve(t, p, long, key("e1"))[0].Status, "the peer sees the commit")
	}},
	{"an abandoned claim lapses after its lease", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		reserve(t, d, lease, key("e1"))
		s.pass(lease)
		assert.Equal(t, dedupe.Claimed, reserve(t, d, long, key("e1"))[0].Status)
	}},
	{"a commit expires after its retention", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		c := reserve(t, d, long, key("brief"), key("kept"))
		require.NoError(t, d.Commit(t.Context(), c[:1], time.Second))
		require.NoError(t, d.Commit(t.Context(), c[1:], 0))
		assert.Equal(t, []dedupe.Status{dedupe.Duplicate, dedupe.Duplicate}, statuses(reserve(t, d, long, key("brief"), key("kept"))))
		s.pass(time.Second)
		assert.Equal(t, []dedupe.Status{dedupe.Claimed, dedupe.Duplicate}, statuses(reserve(t, d, long, key("brief"), key("kept"))),
			"retention 0 never expires")
	}},
	{"concurrent reserves of one key claim it once", func(t *testing.T, s *suite) {
		// #390: two requests carrying one id must not both publish.
		const n = 64
		d, p := s.store(t, "acme"), s.peer(t, "acme")
		race := func() []dedupe.Claim {
			out := make([]dedupe.Claim, n)
			var wg sync.WaitGroup
			for i := range n {
				store := d
				if i%2 == 1 {
					store = p
				}
				wg.Go(func() {
					c, err := store.Reserve(context.Background(), []dedupe.Key{key("e1")}, long)
					if assert.NoError(t, err) {
						out[i] = c[0]
					}
				})
			}
			wg.Wait()
			return out
		}
		var winner []dedupe.Claim
		for _, c := range race() {
			if c.Status == dedupe.Claimed {
				winner = append(winner, c)
			} else {
				assert.Equal(t, dedupe.InFlight, c.Status)
			}
		}
		require.Len(t, winner, 1, "exactly one reserve claims the key")
		require.NoError(t, d.Commit(t.Context(), winner, 0))
		for _, c := range race() {
			assert.Equal(t, dedupe.Duplicate, c.Status)
		}
	}},
	{"a key repeated in one call is claimed once", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		c := reserve(t, d, long, key("a"), key("b"), key("a"), key("a"))
		assert.Equal(t, []dedupe.Status{dedupe.Claimed, dedupe.Claimed, dedupe.Duplicate, dedupe.Duplicate}, statuses(c))
		require.NoError(t, d.Commit(t.Context(), c, 0), "commit ignores the repeats")
		assert.Equal(t, []dedupe.Status{dedupe.Duplicate, dedupe.Duplicate}, statuses(reserve(t, d, long, key("a"), key("b"))))
	}},
	{"answers keep input order in a large call", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		keys := make([]dedupe.Key, 300)
		for i := range keys {
			keys[i] = key(fmt.Sprint(i))
		}
		var odd []dedupe.Key
		for i := 1; i < len(keys); i += 2 {
			odd = append(odd, keys[i])
		}
		require.NoError(t, d.Commit(t.Context(), reserve(t, d, long, odd...), 0))
		for i, c := range reserve(t, d, long, keys...) {
			want := dedupe.Claimed
			if i%2 == 1 {
				want = dedupe.Duplicate
			}
			assert.Equal(t, want, c.Status, "key %d", i)
		}
	}},
	{"tables and tenants have their own keyspace", func(t *testing.T, s *suite) {
		acme, globex := s.store(t, "acme"), s.store(t, "globex")
		// "ab"+"c" and "a"+"bc" would be one key were table and id just
		// joined; tenants "a"/"ab" likewise.
		first := []dedupe.Key{{Table: "clicks", ID: "e1"}, {Table: "ab", ID: "c"}}
		require.NoError(t, acme.Commit(t.Context(), reserve(t, acme, long, first...), 0))
		assert.Equal(t, []dedupe.Status{dedupe.Claimed, dedupe.Claimed},
			statuses(reserve(t, acme, long, dedupe.Key{Table: "views", ID: "e1"}, dedupe.Key{Table: "a", ID: "bc"})), "#222: another table's id")
		assert.Equal(t, []dedupe.Status{dedupe.Claimed, dedupe.Claimed},
			statuses(reserve(t, globex, long, first...)), "another tenant's ids")
		a, ab := s.store(t, "a"), s.store(t, "ab")
		require.NoError(t, a.Commit(t.Context(), reserve(t, a, long, dedupe.Key{Table: "bt", ID: "e1"}), 0))
		assert.Equal(t, dedupe.Claimed, reserve(t, ab, long, dedupe.Key{Table: "t", ID: "e1"})[0].Status)
		assert.Equal(t, []dedupe.Status{dedupe.Duplicate, dedupe.Duplicate},
			statuses(reserve(t, acme, long, first...)), "and still duplicates in their own")
	}},
	{"long ids and ids that look hashed stay distinct", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		base := strings.Repeat("x", 2*dedupe.MaxIDBytes)
		longA, longB := key(base+"a"), key(base+"b")
		hashLike := key("\xff" + strings.Repeat("0", 32))
		require.NoError(t, d.Commit(t.Context(), reserve(t, d, long, longA, hashLike), 0))
		assert.Equal(t, []dedupe.Status{dedupe.Duplicate, dedupe.Claimed, dedupe.Duplicate},
			statuses(reserve(t, d, long, longA, longB, hashLike)))
	}},
	{"a late commit after a re-claim still lands", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		first := reserve(t, d, lease, key("e1"))
		s.pass(lease)
		second := reserve(t, d, long, key("e1"))
		require.Equal(t, dedupe.Claimed, second[0].Status)
		require.NoError(t, d.Commit(t.Context(), first, 0), "the first request did publish")
		require.NoError(t, d.Release(t.Context(), second), "the second gives up; the commit stands")
		assert.Equal(t, dedupe.Duplicate, reserve(t, d, long, key("e1"))[0].Status)
	}},
	{"a stale release leaves the new claimant alone", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		first := reserve(t, d, lease, key("e1"))
		s.pass(lease)
		second := reserve(t, d, long, key("e1"))
		require.NoError(t, d.Release(t.Context(), first))
		assert.Equal(t, dedupe.InFlight, reserve(t, d, long, key("e1"))[0].Status, "the second claim is still live")
		require.NoError(t, d.Commit(t.Context(), second, 0))
		assert.Equal(t, dedupe.Duplicate, reserve(t, d, long, key("e1"))[0].Status)
	}},
	{"a failed reserve leaves nothing claimed", func(t *testing.T, s *suite) {
		if s.FailNextReserve == nil {
			t.Skip("the backend has no failure hook")
		}
		d := s.store(t, "acme")
		s.FailNextReserve(1)
		_, err := d.Reserve(t.Context(), []dedupe.Key{key("a"), key("b"), key("c")}, long)
		require.Error(t, err)
		assert.Equal(t, []dedupe.Status{dedupe.Claimed, dedupe.Claimed, dedupe.Claimed},
			statuses(reserve(t, d, long, key("a"), key("b"), key("c"))))
	}},
	{"empty calls are no-ops", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		c, err := d.Reserve(t.Context(), nil, long)
		require.NoError(t, err)
		assert.Empty(t, c)
		require.NoError(t, d.Commit(t.Context(), nil, 0))
		require.NoError(t, d.Release(t.Context(), nil))
		dup := []dedupe.Claim{{Key: key("e1"), Status: dedupe.Duplicate}, {Key: key("e2"), Status: dedupe.InFlight}}
		require.NoError(t, d.Commit(t.Context(), dup, 0), "only Claimed claims commit")
		assert.Equal(t, dedupe.Claimed, reserve(t, d, long, key("e1"))[0].Status)
	}},
	{"any table name is its own keyspace", func(t *testing.T, s *suite) {
		d := s.store(t, "acme")
		// seen[i] and fresh[i] differ only in where table ends and id
		// begins, or in a byte an escaped key could confuse with its
		// escape: each pair would share one key under a layout that
		// separated the fields without escaping them.
		seen := []dedupe.Key{
			{Table: "a", ID: "b\x00c"},
			{Table: "a\x00", ID: "b"},
			{Table: "", ID: "\x01a"},
			{Table: "\xff\xfe", ID: "e1"},
			{Table: "tab\tle \n", ID: "e1"},
			{Table: "a/b", ID: "c"},
			{Table: "t", ID: "%23x"},
		}
		fresh := []dedupe.Key{
			{Table: "a\x00b", ID: "c"},
			{Table: "a", ID: "\x00b"},
			{Table: "\x01", ID: "a"},
			{Table: "\xff", ID: "\xfee1"},
			{Table: "tab\tle", ID: " \ne1"},
			{Table: "a%2Fb", ID: "c"},
			{Table: "t", ID: "#x"},
		}
		first := reserve(t, d, long, seen...)
		for _, c := range first {
			require.Equal(t, dedupe.Claimed, c.Status, "%q shares a key with another seen key", c.Key)
		}
		require.NoError(t, d.Commit(t.Context(), first, 0))
		for _, c := range reserve(t, d, long, fresh...) {
			assert.Equal(t, dedupe.Claimed, c.Status, "%q", c.Key)
		}
		for _, c := range reserve(t, d, long, seen...) {
			assert.Equal(t, dedupe.Duplicate, c.Status, "%q", c.Key)
		}
	}},
}
