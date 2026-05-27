package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryTimeToTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryTime time.Duration
		want      time.Duration
	}{
		{
			name:      "very fast query clamps to minTTL",
			queryTime: 1 * time.Millisecond, // 1ms * 1000 = 1s
			want:      10 * time.Second,     // Clamped to 10s
		},
		{
			name:      "query at exact min boundary",
			queryTime: 10 * time.Millisecond, // 10ms * 1000 = 10s
			want:      10 * time.Second,
		},
		{
			name:      "normal scaling query",
			queryTime: 50 * time.Millisecond, // 50ms * 1000 = 50s
			want:      50 * time.Second,
		},
		{
			name:      "query at exact max boundary",
			queryTime: 3600 * time.Millisecond, // 3.6s * 1000 = 3600s (1h)
			want:      1 * time.Hour,
		},
		{
			name:      "very slow query clamps to maxTTL",
			queryTime: 10 * time.Second, // 10s * 1000 = 10,000s
			want:      1 * time.Hour,    // Clamped down to 1h
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable for parallel testing
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, QueryTimeToTTL(tt.queryTime))
		})
	}
}

func TestGenerateVersionKey(t *testing.T) {
	t.Parallel()

	// Test empty scope
	assert.Equal(t, "users", generateVersionKey("users", ""))

	// Test populated scope
	assert.Equal(t, "users.org_123", generateVersionKey("users", "org_123"))
}
