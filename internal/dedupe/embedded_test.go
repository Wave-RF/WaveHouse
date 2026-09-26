package dedupe

import (
	"context"
	"maps"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// switchedOn returns tenant id's store over e, switched on, and switches it
// off at cleanup.
func switchedOn(t *testing.T, e *Embedded, id tenant.ID) *Managed {
	t.Helper()
	m := e.Tenant(id)
	require.NoError(t, m.Apply(true))
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestEmbedded_FirstSeenThenDuplicate(t *testing.T) {
	t.Parallel()
	m := switchedOn(t, NewEmbedded(t.TempDir()), "acme")
	ctx := context.Background()

	dup, err := mark(ctx, m, "event-1")
	require.NoError(t, err)
	assert.False(t, dup, "first occurrence must not be a duplicate")

	dup, err = mark(ctx, m, "event-1")
	require.NoError(t, err)
	assert.True(t, dup, "second occurrence of the same id must be a duplicate")

	dup, err = mark(ctx, m, "event-2")
	require.NoError(t, err)
	assert.False(t, dup, "distinct ids are independent")
}

// Every tenant's seen ids live in one instance and never meet: the tenant
// leads each key, ended by a byte no tenant id holds, so tenant "a" with id
// "bc" and tenant "ab" with id "c" — one key, were the two just joined — are
// two.
func TestEmbedded_TenantsDoNotShareSeenIDs(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	ctx := context.Background()
	a, ab := switchedOn(t, e, "a"), switchedOn(t, e, "ab")

	dup, err := mark(ctx, a, "bc")
	require.NoError(t, err)
	assert.False(t, dup)
	dup, err = mark(ctx, ab, "c")
	require.NoError(t, err)
	assert.False(t, dup, "another tenant's key, however the two would join")
	dup, err = mark(ctx, ab, "bc")
	require.NoError(t, err)
	assert.False(t, dup, "an id tenant a has seen is new to tenant ab")
	dup, err = mark(ctx, a, "bc")
	require.NoError(t, err)
	assert.True(t, dup, "and still a duplicate within its own tenant")
}

// The instance is open exactly while some tenant's store is: nothing is
// opened, nor its directory created, until the first store switches on, and
// it closes with the last one, its files kept for the next open.
func TestEmbedded_OpenWhileAnyTenantStoreIs(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	ctx := context.Background()
	acme, globex := e.Tenant("acme"), e.Tenant("globex")
	assert.False(t, e.Open())
	assert.NoDirExists(t, e.Dir(), "a store built but never switched on opens nothing")

	require.NoError(t, acme.Apply(true))
	require.NoError(t, globex.Apply(true))
	assert.True(t, e.Open())
	_, err := mark(ctx, acme, "e1")
	require.NoError(t, err)

	require.NoError(t, acme.Apply(false))
	assert.True(t, e.Open(), "globex's store still holds the instance open")
	require.NoError(t, globex.Apply(false))
	assert.False(t, e.Open(), "closed with the last store")
	entries, err := os.ReadDir(e.Dir())
	require.NoError(t, err)
	assert.NotEmpty(t, entries, "its files stay")

	require.NoError(t, acme.Apply(true))
	t.Cleanup(func() { _ = acme.Close() })
	dup, err := mark(ctx, acme, "e1")
	require.NoError(t, err)
	assert.True(t, dup, "a tenant switched off keeps its seen ids")
}

// The gauges read the one instance: nil while it is closed, and its own
// figures — one set, not one per tenant — while two tenants' stores are open.
func TestEmbedded_StatsAreTheInstances(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	assert.Nil(t, e.Stats(), "closed: the scraper skips the gauges")

	acme := switchedOn(t, e, "acme")
	switchedOn(t, e, "globex")
	_, err := mark(context.Background(), acme, "e1")
	require.NoError(t, err)
	stats := e.Stats()
	m := e.db.Metrics()
	require.Positive(t, stats["pebble_wal_size"])
	assert.EqualValues(t, m.WAL.Size, stats["pebble_wal_size"], "the instance's, not summed per tenant")
	assert.Equal(t, m.Total().NumFiles, stats["pebble_table_count"])
	assert.Len(t, stats, 2)
}

// An instance that cannot open fails every tenant's store — they share it —
// and stays closed with no store holding it; the next open retries.
func TestEmbedded_OpenFailure(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	// A regular file where the instance's directory should be is what Pebble
	// refuses to open.
	require.NoError(t, os.WriteFile(e.Dir(), nil, 0o600))
	acme, globex := e.Tenant("acme"), e.Tenant("globex")
	require.Error(t, acme.Apply(true))
	require.Error(t, globex.Apply(true), "one instance: its failure is every tenant's")
	assert.False(t, e.Open())
	_, err := mark(context.Background(), acme, "e1")
	require.ErrorIs(t, err, ErrUnavailable)

	require.NoError(t, os.Remove(e.Dir()))
	require.NoError(t, acme.Apply(true), "the next apply retries the open")
	t.Cleanup(func() { _ = acme.Close() })
	assert.True(t, e.Open())
}

// Keys from before the table joined the key (#222) are never read: an id
// seen then is accepted once more after the upgrade, the documented cost of
// the new layout.
func TestEmbedded_VersionZeroKeysAreNotRead(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	m := switchedOn(t, e, "acme")
	require.NoError(t, e.db.Set([]byte("acme\x00e1"), make([]byte, 8), pebble.Sync))
	dup, err := mark(context.Background(), m, "e1")
	require.NoError(t, err)
	assert.False(t, dup)
}

// v0.1.0 stored a bare id as the key with an 8-byte value, so a v0.1.0 id
// that happens to spell a key the current layout would also write —
// tenant "0", table "events", id "e1" join to "0/events/e1", which a v0.1.0
// record could have used as its own id — must not read as a live duplicate:
// only a value of exactly valueLen bytes leading with committedMark is ours.
// The planted value leads with committedMark, so only the length check can
// refuse it (and keeps a short value from being decoded as an expiry).
// The reserve below claims the key despite the stale value, and only the
// commit it makes turns a second reserve of the same id into a duplicate.
func TestEmbedded_PreJoinValueUnderACollidingKeyIsNotLive(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	m := switchedOn(t, e, "0")
	key := AppendKey(nil, KeyPrefix("0"), Key{Table: "events", ID: "e1"})
	stale := make([]byte, 8)
	stale[0] = committedMark
	require.NoError(t, e.db.Set(key, stale, pebble.Sync))

	dup, err := mark(context.Background(), m, "e1")
	require.NoError(t, err)
	assert.False(t, dup, "an 8-byte v0.1.0 value is not this layout's commit")

	dup, err = mark(context.Background(), m, "e1")
	require.NoError(t, err)
	assert.True(t, dup, "the mark above overwrote it with a real commit")
}

// Same colliding key, a 9-byte value that committedMark did not write: the
// mark byte, not just the length, is what says a value is ours.
func TestEmbedded_WrongMarkByteIsNotLive(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	m := switchedOn(t, e, "0")
	key := AppendKey(nil, KeyPrefix("0"), Key{Table: "events", ID: "e1"})
	val := make([]byte, valueLen)
	val[0] = committedMark + 1
	require.NoError(t, e.db.Set(key, val, pebble.Sync))

	dup, err := mark(context.Background(), m, "e1")
	require.NoError(t, err)
	assert.False(t, dup, "a value not led by committedMark is not a live commit")
}

// A claim nobody commits, releases or reserves again leaves memory at the
// next sweep past its lease, not never.
func TestPendingShard_SweepDropsLapsedClaims(t *testing.T) {
	t.Parallel()
	now := time.Now()
	sh := &pendingShard{m: map[string]pending{
		"lapsed": {token: "1", expires: now.Add(-time.Second)},
		"live":   {token: "2", expires: now.Add(time.Hour)},
	}}
	sh.sweep(now)
	assert.Equal(t, []string{"live"}, slices.Collect(maps.Keys(sh.m)))

	sh.m["lapsed"] = pending{token: "3", expires: now.Add(-time.Second)}
	sh.sweep(now.Add(time.Second))
	assert.Len(t, sh.m, 2, "at most one sweep per DefaultLease")
	sh.sweep(now.Add(DefaultLease))
	assert.Len(t, sh.m, 1)
}
