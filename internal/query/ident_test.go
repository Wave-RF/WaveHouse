package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
			got := SafeEncodeToken(tt.raw)
			assert.Equal(t, tt.expected, got)
		})
	}
}
