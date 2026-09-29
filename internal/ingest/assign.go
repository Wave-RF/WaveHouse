package ingest

import (
	"cmp"
	"hash/fnv"
	"slices"
	"strconv"
)

// assignUnits gives each unit to one of the live membership slots, by
// rendezvous hashing capped at an even share: each unit ranks the slots by a
// score of its own, the units are taken best score first, and each goes to
// its highest-ranked slot still under ⌈units/slots⌉. The result depends only
// on its inputs' contents, not their order, so every process that sees the
// same slots and units derives the same owners. A slot joining or leaving
// moves close to the fewest units it can, and no slot owns more than the cap.
func assignUnits(units []string, slots []int) map[string]int {
	out := make(map[string]int, len(units))
	if len(slots) == 0 {
		return out
	}
	slots = slices.Sorted(slices.Values(slots))
	type ranked struct {
		unit  string
		order []int // slots, best score first
		best  uint64
	}
	rs := make([]ranked, len(units))
	for i, u := range units {
		scores := make(map[int]uint64, len(slots))
		for _, s := range slots {
			scores[s] = unitScore(u, s)
		}
		order := slices.Clone(slots)
		slices.SortFunc(order, func(a, b int) int {
			if c := cmp.Compare(scores[b], scores[a]); c != 0 {
				return c
			}
			return cmp.Compare(a, b)
		})
		rs[i] = ranked{unit: u, order: order, best: scores[order[0]]}
	}
	slices.SortFunc(rs, func(a, b ranked) int {
		if c := cmp.Compare(b.best, a.best); c != 0 {
			return c
		}
		return cmp.Compare(a.unit, b.unit)
	})
	limit := (len(units) + len(slots) - 1) / len(slots)
	load := make(map[int]int, len(slots))
	for _, r := range rs {
		for _, s := range r.order {
			if load[s] < limit {
				out[r.unit] = s
				load[s]++
				break
			}
		}
	}
	return out
}

// unitScore is slot's rendezvous score for unit: FNV-1a 64 of the two,
// through splitmix64's finalizer so that nearby slots score independently.
func unitScore(unit string, slot int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(unit))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.Itoa(slot)))
	z := h.Sum64()
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
