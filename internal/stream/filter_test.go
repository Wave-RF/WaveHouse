package stream

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
)

// TestProjectIndices: the projection now yields POSITIONS as well as names —
// the row is a positional array, so a dropped column must remove its slot, and
// the surviving names must line up with the surviving slots.
func TestProjectIndices(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cols      []string
		perms     *policy.ResolvedPermissions
		wantIdx   []int
		wantNames []string
	}{
		{
			name:      "nil perms keeps every column",
			cols:      []string{"a", "b"},
			wantIdx:   []int{0, 1},
			wantNames: []string{"a", "b"},
		},
		{
			// An explicitly RESOLVED but empty select side: unrestricted. Distinct
			// from a nil Select, which means the grant was never resolved for
			// reads and is denied — the case below.
			name:      "no lists keeps every column",
			cols:      []string{"a", "b"},
			perms:     &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{}},
			wantIdx:   []int{0, 1},
			wantNames: []string{"a", "b"},
		},
		{
			// An insert-resolved grant reaching the stream path is a caller bug.
			// projectIndices goes through IsColumnAllowed, which denies on the
			// unresolved side, so nothing is projected rather than passed through.
			name:      "unresolved select side projects nothing",
			cols:      []string{"page", "secret"},
			perms:     &policy.ResolvedPermissions{Allowed: true, Insert: &policy.ResolvedInsert{}},
			wantIdx:   []int{},
			wantNames: []string{},
		},
		{
			name:      "allow list keeps only allowed columns, in schema order",
			cols:      []string{"page", "secret", "button"},
			perms:     &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"button", "page"}}},
			wantIdx:   []int{0, 2},
			wantNames: []string{"page", "button"},
		},
		{
			name:      "deny list drops denied columns and their slots",
			cols:      []string{"page", "secret_col", "button"},
			perms:     &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{DenyColumns: []string{"secret_col"}}},
			wantIdx:   []int{0, 2},
			wantNames: []string{"page", "button"},
		},
		{
			name:      "a denied role projects nothing",
			cols:      []string{"page", "button"},
			perms:     &policy.ResolvedPermissions{Allowed: false},
			wantIdx:   []int{},
			wantNames: []string{},
		},
		{
			name:      "no columns projects nothing",
			cols:      nil,
			perms:     &policy.ResolvedPermissions{Allowed: true},
			wantIdx:   []int{},
			wantNames: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			idx, names := projectIndices(tt.cols, tt.perms)
			assert.Equal(t, tt.wantIdx, idx)
			assert.Equal(t, tt.wantNames, names)
			assert.Len(t, names, len(idx), "every kept position must have a name")
		})
	}
}

// TestSchemaSignature_DistinguishesLists: drift detection keys on this, so two
// different column lists — or the same list on a different table — must never
// produce the same signature.
func TestSchemaSignature_DistinguishesLists(t *testing.T) {
	t.Parallel()
	seen := map[string]string{}
	cases := []struct {
		label string
		table string
		cols  []string
	}{
		{"clicks a,b", "clicks", []string{"a", "b"}},
		{"clicks b,a", "clicks", []string{"b", "a"}},
		{"clicks a", "clicks", []string{"a"}},
		{"clicks none", "clicks", nil},
		{"events a,b", "events", []string{"a", "b"}},
		{"clicks ab (one column)", "clicks", []string{"ab"}},
	}
	for _, c := range cases {
		sig := schemaSignature(c.table, c.cols)
		prev, dup := seen[sig]
		assert.False(t, dup, "%s collides with %s", c.label, prev)
		seen[sig] = c.label
	}
}
