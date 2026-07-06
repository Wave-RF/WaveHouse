package stream

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
)

func TestFilterColumns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		data  map[string]any
		perms *policy.ResolvedPermissions
		want  map[string]any
	}{
		{
			name: "nil perms returns the original data",
			data: map[string]any{"a": 1, "b": 2},
			want: map[string]any{"a": 1, "b": 2},
		},
		{
			name:  "nil data returns nil",
			perms: &policy.ResolvedPermissions{Allowed: true},
			want:  nil,
		},
		{
			name:  "allow list keeps only allowed columns",
			data:  map[string]any{"page": "/home", "secret": "xyz", "button": "signup"},
			perms: &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"page", "button"}},
			want:  map[string]any{"page": "/home", "button": "signup"},
		},
		{
			name:  "deny list drops denied columns",
			data:  map[string]any{"page": "/home", "secret_col": "xyz"},
			perms: &policy.ResolvedPermissions{Allowed: true, DenyColumns: []string{"secret_col"}},
			want:  map[string]any{"page": "/home"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, filterColumns(tt.data, tt.perms))
		})
	}
}

func TestFilterColumns_DoesNotMutateOriginal(t *testing.T) {
	t.Parallel()
	data := map[string]any{"a": 1, "b": 2, "c": 3}
	original := map[string]any{"a": 1, "b": 2, "c": 3}
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"a"}}
	_ = filterColumns(data, perms)
	assert.Equal(t, original, data, "the input map must be left unmodified (keys and values)")
}
