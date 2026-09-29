package ingest

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func unitNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("WH_INGEST_%d/wh-ingest-%d", i/8, i%8)
	}
	return out
}

func slotsUpTo(m int) []int {
	out := make([]int, m)
	for i := range out {
		out[i] = i
	}
	return out
}

func loads(assignment map[string]int) map[int]int {
	out := map[int]int{}
	for _, s := range assignment {
		out[s]++
	}
	return out
}

// Every process derives the same owners from the same members and units,
// in whatever order it lists them, and every unit gets one.
func TestAssignUnits_Deterministic(t *testing.T) {
	t.Parallel()
	units, slots := unitNames(32), []int{0, 3, 4, 9}
	want := assignUnits(units, slots)
	assert.Len(t, want, 32)
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: a shuffle, not a secret
	for range 20 {
		u, s := slices.Clone(units), slices.Clone(slots)
		rng.Shuffle(len(u), func(i, j int) { u[i], u[j] = u[j], u[i] })
		rng.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
		assert.Equal(t, want, assignUnits(u, s))
	}
	assert.Empty(t, assignUnits(units, nil), "no members, no owners")
}

// No member owns more than an even share, rounded up, and every unit is owned.
func TestAssignUnits_Cap(t *testing.T) {
	t.Parallel()
	for _, u := range []int{16, 32, 64} {
		for m := 1; m <= 12; m++ {
			got := assignUnits(unitNames(u), slotsUpTo(m))
			assert.Len(t, got, u)
			limit := (u + m - 1) / m
			for s, n := range loads(got) {
				assert.LessOrEqual(t, n, limit, "units %d, members %d, slot %d", u, m, s)
			}
		}
	}
}

// A member joining moves close to the fewest units it can while the even
// share's cap holds: at most 1.5x the share the newcomer takes, up to six
// members. Where the cap steps down (⌈32/10⌉=4 to ⌈32/11⌉=3) units re-flow to
// fit it, so the bound for any count is range assignment's, half the units,
// which it never exceeds. A member leaving moves about its own units, at most
// twice as many.
func TestAssignUnits_Movement(t *testing.T) {
	t.Parallel()
	const u = 32
	units := unitNames(u)
	for w := 1; w < u; w++ {
		before, after := assignUnits(units, slotsUpTo(w)), assignUnits(units, slotsUpTo(w+1))
		moved := 0
		for _, unit := range units {
			if before[unit] != after[unit] {
				moved++
			}
		}
		ideal := float64(u) / float64(w+1)
		t.Logf("%2d → %2d members: %2d of %d units moved (ideal %.1f)", w, w+1, moved, u, ideal)
		assert.LessOrEqual(t, moved, u/2, "%d → %d members", w, w+1)
		if w <= 6 {
			assert.LessOrEqual(t, float64(moved), 1.5*ideal+1, "%d → %d members", w, w+1)
		}
	}
	five := assignUnits(units, []int{0, 1, 2, 3, 4})
	four := assignUnits(units, []int{0, 1, 3, 4})
	own, moved := loads(five)[2], 0
	for _, unit := range units {
		if five[unit] != four[unit] {
			moved++
		}
	}
	t.Logf("slot 2 of 5 leaves: %d units moved, %d of them its own", moved, own)
	assert.LessOrEqual(t, moved, 2*own)
}

// The mapping is pinned, so a change to it is deliberate: every process of
// a rolling upgrade must compute the same owners.
func TestAssignUnits_Golden(t *testing.T) {
	t.Parallel()
	got := assignUnits(unitNames(32), []int{0, 1, 2})
	var owners []int
	for _, u := range unitNames(32) {
		owners = append(owners, got[u])
	}
	assert.Equal(t, goldenOwners, owners)
}

// goldenOwners was derived apart from this code, by the same algorithm in
// another language (FNV-1a 64, splitmix64's finalizer, capped rendezvous).
var goldenOwners = []int{1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 0, 0, 0, 2, 2, 2, 0, 1, 0, 1, 0, 2, 0, 1, 1, 1, 2, 0, 2, 1, 1, 2}

// Extras are assigned apart from the configured units, so a process that
// lists one extra more or less than another agrees with it on every
// configured unit's owner.
func TestClaims_ExtrasNeverMoveConfiguredUnits(t *testing.T) {
	t.Parallel()
	units, slots := unitNames(32), slotsUpTo(5)
	without := assignUnits(units, slots)
	for _, extras := range [][]string{{"X/a"}, {"X/a", "X/b", "X/c"}} {
		with := assignUnits(units, slots)
		maps.Copy(with, assignUnits(extras, slots))
		for _, u := range units {
			assert.Equal(t, without[u], with[u], "%s with extras %v", u, extras)
		}
	}
}
