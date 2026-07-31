package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	t.Parallel()
	v := Defaults()
	assert.Equal(t, "event_id", v.Dedupe.IDField)
	assert.False(t, v.Dedupe.RequireID)
	assert.NoError(t, v.Validate(), "defaults must always validate — they're the fallback for every section")
}

func TestValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*Values)
		wantErr string // substring expected in the error; "" means valid
	}{
		{"defaults are valid", func(*Values) {}, ""},
		{"custom id_field is valid", func(v *Values) { v.Dedupe.IDField = "org_id" }, ""},
		{"require_id with id_field is valid", func(v *Values) { v.Dedupe.RequireID = true }, ""},
		// "" pauses dedup lookups — legal on its own...
		{"empty id_field alone is valid", func(v *Values) { v.Dedupe.IDField = "" }, ""},
		// ...but requiring an id while naming no field is a contradiction.
		{
			"require_id without id_field is rejected",
			func(v *Values) { v.Dedupe.IDField = ""; v.Dedupe.RequireID = true },
			"dedupe.id_field",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := Defaults()
			tt.mutate(&v)
			err := v.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
