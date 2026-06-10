package api

import (
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// TestChReadSettings locks the policy-cap → ClickHouse-setting mapping that
// enforces resource budgets server-side (#316). It is the regression guard for
// the mapping itself; the handler-level enforcement (settings actually reach
// ClickHouse and are honored) is proven by the integration + e2e suites.
func TestChReadSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		perms   *policy.ResolvedPermissions
		timeout time.Duration
		// want is the exact settings map expected; nil means chReadSettings
		// must return nil (no caps → no context wrapping).
		want map[string]any
	}{
		{
			name:  "nil perms",
			perms: nil,
			want:  nil,
		},
		{
			name:  "no caps set",
			perms: &policy.ResolvedPermissions{Allowed: true},
			want:  nil,
		},
		{
			name:    "sub-second execution time is a fractional max_execution_time",
			perms:   &policy.ResolvedPermissions{MaxExecutionTimeMs: 500},
			timeout: 500 * time.Millisecond,
			// The driver only auto-derives max_execution_time for deadlines > 1s,
			// so a 500ms cap MUST be emitted explicitly or it reaches CH unbounded.
			want: map[string]any{"max_execution_time": 0.5},
		},
		{
			name:    "multi-second execution time",
			perms:   &policy.ResolvedPermissions{MaxExecutionTimeMs: 3000},
			timeout: 3 * time.Second,
			want:    map[string]any{"max_execution_time": 3.0},
		},
		{
			name:  "max_rows caps result rows with throw mode",
			perms: &policy.ResolvedPermissions{MaxRows: 1000},
			want: map[string]any{
				"max_result_rows":      1000,
				"result_overflow_mode": "throw",
			},
		},
		{
			name:  "max_rows_to_read caps rows scanned with throw mode",
			perms: &policy.ResolvedPermissions{MaxRowsToRead: 1_000_000},
			want: map[string]any{
				"max_rows_to_read":   int64(1_000_000),
				"read_overflow_mode": "throw",
			},
		},
		{
			name:  "max_memory_usage_bytes caps peak query memory",
			perms: &policy.ResolvedPermissions{MaxMemoryUsageBytes: 4 << 30}, // 4 GiB > int32
			want:  map[string]any{"max_memory_usage": int64(4 << 30)},
		},
		{
			name: "all caps together",
			perms: &policy.ResolvedPermissions{
				MaxExecutionTimeMs:  2000,
				MaxRows:             500,
				MaxRowsToRead:       2_000_000,
				MaxMemoryUsageBytes: 8 << 30,
			},
			timeout: 2 * time.Second,
			want: map[string]any{
				"max_execution_time":   2.0,
				"max_result_rows":      500,
				"result_overflow_mode": "throw",
				"max_rows_to_read":     int64(2_000_000),
				"read_overflow_mode":   "throw",
				"max_memory_usage":     int64(8 << 30),
			},
		},
		{
			name: "zero caps are omitted even when others are set",
			perms: &policy.ResolvedPermissions{
				MaxRowsToRead: 42,
				// MaxRows / MaxExecutionTimeMs / MaxMemoryUsageBytes left at 0.
			},
			want: map[string]any{
				"max_rows_to_read":   int64(42),
				"read_overflow_mode": "throw",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := chReadSettings(tt.perms, tt.timeout)

			if tt.want == nil {
				if got != nil {
					t.Fatalf("expected nil settings, got %#v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected settings %#v, got nil", tt.want)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("settings key count mismatch: got %#v, want %#v", map[string]any(got), tt.want)
			}
			for k, wantV := range tt.want {
				gotV, ok := got[k]
				if !ok {
					t.Errorf("missing setting %q (got %#v)", k, map[string]any(got))
					continue
				}
				if gotV != wantV {
					t.Errorf("setting %q = %#v (%T), want %#v (%T)", k, gotV, gotV, wantV, wantV)
				}
			}
		})
	}
}
