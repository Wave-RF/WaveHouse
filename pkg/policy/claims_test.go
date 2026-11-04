package policy

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestResolveClaim pins the one resolution a policy _in value and a pipe's
// {{ jwt.<path> }} reference share: canonical scalars, an array as a set, and
// fail-closed on everything that has no bindable form.
func TestResolveClaim(t *testing.T) {
	t.Parallel()
	claims := map[string]any{
		"org_id":   "org-123",
		"nested":   map[string]any{"val": "deep", "ids": []any{"a", "b"}},
		"tids":     []any{"t1", "t2"},
		"nums":     []any{json.Number("1.0"), json.Number("12345678901234567890")},
		"mixed":    []any{"a", nil},
		"objs":     []any{map[string]any{"k": "v"}},
		"empty":    []any{},
		"null":     nil,
		"blank":    "",
		"is_admin": true,
		"price":    json.Number("1.0"),
		"huge":     json.Number("1e400"),
	}
	tests := []struct {
		name    string
		path    string
		want    []string
		wantSet bool
		wantOK  bool
	}{
		{"scalar", "org_id", []string{"org-123"}, false, true},
		{"nested scalar", "nested.val", []string{"deep"}, false, true},
		{"array binds as a set", "tids", []string{"t1", "t2"}, true, true},
		{"nested array", "nested.ids", []string{"a", "b"}, true, true},
		{"numeric elements bind canonically", "nums", []string{"1", "12345678901234567890"}, true, true},
		{"boolean", "is_admin", []string{"true"}, false, true},
		{"numeric claim binds canonically", "price", []string{"1"}, false, true},
		// A present-but-empty string is a value the token vouches for, as on
		// the filter path; only an empty ARRAY has nothing to be a member of.
		{"present but empty string binds", "blank", []string{""}, false, true},
		{"absent fails closed", "missing", nil, false, false},
		{"null fails closed", "null", nil, false, false},
		{"object fails closed", "nested", nil, false, false},
		{"empty array fails closed", "empty", nil, false, false},
		{"non-scalar element fails the whole set", "mixed", nil, false, false},
		{"object element fails the whole set", "objs", nil, false, false},
		{"no canonical form fails closed", "huge", nil, false, false},
		{"path through a scalar fails closed", "org_id.x", nil, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, set, ok := ResolveClaim(claims, tt.path)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
			if ok {
				assert.Equal(t, tt.wantSet, set)
			}
		})
	}
}

func TestResolveClaim_NilClaims(t *testing.T) {
	t.Parallel()
	got, _, ok := ResolveClaim(nil, "org_id")
	assert.False(t, ok)
	assert.Nil(t, got)
}
