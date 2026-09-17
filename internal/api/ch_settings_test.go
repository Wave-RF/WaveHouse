package api

import (
	"testing"
	"time"
)

// TestChReadSettings locks the resource-budget → ClickHouse-setting mapping that
// enforces caps server-side (#316). It is the regression guard for the mapping
// itself; the handler-level enforcement (settings actually reach ClickHouse and
// are honored) is proven by the integration + e2e suites.
func TestChReadSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		limits chQueryLimits
		// want is the exact settings map expected; nil means chReadSettings
		// must return nil (no caps → no context wrapping).
		want map[string]string
	}{
		{
			name:   "no caps set",
			limits: chQueryLimits{},
			want:   nil,
		},
		{
			name:   "sub-second execution time is a fractional max_execution_time",
			limits: chQueryLimits{ExecutionTime: 500 * time.Millisecond},
			// A request deadline is not a server-side bound, so a sub-second cap
			// MUST be emitted explicitly — and as fractional seconds, which a
			// whole-second spelling would round away to "no cap at all".
			want: map[string]string{"max_execution_time": "0.5"},
		},
		{
			name:   "multi-second execution time",
			limits: chQueryLimits{ExecutionTime: 3 * time.Second},
			want:   map[string]string{"max_execution_time": "3"},
		},
		{
			name:   "max_result_rows caps result rows with throw mode",
			limits: chQueryLimits{MaxResultRows: 1000},
			want: map[string]string{
				"max_result_rows":      "1000",
				"result_overflow_mode": "throw",
			},
		},
		{
			name:   "max_rows_to_read caps rows scanned with throw mode",
			limits: chQueryLimits{MaxRowsToRead: 1_000_000},
			want: map[string]string{
				"max_rows_to_read":   "1000000",
				"read_overflow_mode": "throw",
			},
		},
		{
			name:   "max_memory_usage caps peak query memory",
			limits: chQueryLimits{MaxMemoryBytes: 4 << 30}, // 4 GiB > int32
			want:   map[string]string{"max_memory_usage": "4294967296"},
		},
		{
			name: "all caps together",
			limits: chQueryLimits{
				ExecutionTime:  2 * time.Second,
				MaxResultRows:  500,
				MaxRowsToRead:  2_000_000,
				MaxMemoryBytes: 8 << 30,
			},
			want: map[string]string{
				"max_execution_time":   "2",
				"max_result_rows":      "500",
				"result_overflow_mode": "throw",
				"max_rows_to_read":     "2000000",
				"read_overflow_mode":   "throw",
				"max_memory_usage":     "8589934592",
			},
		},
		{
			name:   "zero caps are omitted even when others are set",
			limits: chQueryLimits{MaxRowsToRead: 42},
			want: map[string]string{
				"max_rows_to_read":   "42",
				"read_overflow_mode": "throw",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := chReadSettings(tt.limits)

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
				t.Fatalf("settings key count mismatch: got %#v, want %#v", got, tt.want)
			}
			for k, wantV := range tt.want {
				gotV, ok := got[k]
				if !ok {
					t.Errorf("missing setting %q (got %#v)", k, got)
					continue
				}
				if gotV != wantV {
					t.Errorf("setting %q = %#v (%T), want %#v (%T)", k, gotV, gotV, wantV, wantV)
				}
			}
		})
	}
}
