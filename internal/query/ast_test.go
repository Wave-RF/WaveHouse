package query

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumns_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		json    string
		want    Columns // checked only when wantErr is false
		wantErr bool
	}{
		{name: "single string", json: `"ts"`, want: Columns{"ts"}},
		{name: "array of strings", json: `["a","b"]`, want: Columns{"a", "b"}},
		{name: "empty string is nothing", json: `""`},
		{name: "empty array is nothing", json: `[]`},
		{name: "null is nothing", json: `null`},
		{name: "whitespace name kept", json: `[" "]`, want: Columns{" "}},
		{name: "single whitespace string kept", json: `" "`, want: Columns{" "}},
		{name: "literal star kept", json: `["*"]`, want: Columns{"*"}},
		{name: "empty element rejected", json: `[""]`, wantErr: true},
		{name: "mixed empty element rejected", json: `["a",""]`, wantErr: true},
		{name: "number rejected", json: `123`, wantErr: true},
		{name: "bool rejected", json: `true`, wantErr: true},
		{name: "object rejected", json: `{}`, wantErr: true},
		{name: "non-string array element rejected", json: `["a",1]`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var c Columns
			err := json.Unmarshal([]byte(tt.json), &c)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if len(tt.want) == 0 {
				assert.Empty(t, c)
			} else {
				assert.Equal(t, tt.want, c)
			}
		})
	}
}

// An omitted columns field decodes to an empty Columns (a request for no
// columns), and select_all defaults to false.
func TestStructuredQuery_OmittedColumnsAndSelectAll(t *testing.T) {
	t.Parallel()
	var sq StructuredQuery
	require.NoError(t, json.Unmarshal([]byte(`{"limit":5}`), &sq))
	assert.Empty(t, sq.Columns)
	assert.False(t, sq.SelectAll)

	require.NoError(t, json.Unmarshal([]byte(`{"select_all":true}`), &sq))
	assert.True(t, sq.SelectAll)
	assert.Empty(t, sq.Columns)
}
