package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		raw      string
		expected string
	}{
		{"safe string", "my_table123", "my_table123"},
		{"with dots", "default.clicks", "default%2Eclicks"},
		{"with spaces", "my table", "my%20table"},
		{"with dashes and slashes", "a-b/c", "a%2Db%2Fc"},
		{"empty string", "", ""},
		{"only safe characters", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := SafeEncodeNATS(tt.raw)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestDecodeTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		safe     string
		expected string
		wantErr  bool
	}{
		{"safe string", "my_table123", "my_table123", false},
		{"encoded dots", "default%2Eclicks", "default.clicks", false},
		{"encoded spaces", "my%20table", "my table", false},
		{"invalid percent encoding", "default%2Gclicks", "", true}, // %2G is not valid hex
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := SafeDecodeNATS(tt.safe)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	rawTables := []string{
		"normal_table",
		"db.schema.table",
		"table-with-dashes",
		"table with spaces and / slashes !!",
		"~weird_chars_@#$%",
	}

	for _, raw := range rawTables {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			encoded := SafeEncodeNATS(raw)
			decoded, err := SafeDecodeNATS(encoded)
			require.NoError(t, err)
			assert.Equal(t, raw, decoded)
		})
	}
}
