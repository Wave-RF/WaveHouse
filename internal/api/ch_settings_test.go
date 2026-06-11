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
		want map[string]any
	}{
		{
			name:   "no caps set",
			limits: chQueryLimits{},
			want:   nil,
		},
		{
			name:   "sub-second execution time is a fractional max_execution_time",
			limits: chQueryLimits{ExecutionTime: 500 * time.Millisecond},
			// The driver only auto-derives max_execution_time for deadlines > 1s,
			// so a 500ms cap MUST be emitted explicitly or it reaches CH unbounded.
			want: map[string]any{"max_execution_time": 0.5},
		},
		{
			name:   "multi-second execution time",
			limits: chQueryLimits{ExecutionTime: 3 * time.Second},
			want:   map[string]any{"max_execution_time": 3.0},
		},
		{
			name:   "max_result_rows caps result rows with throw mode",
			limits: chQueryLimits{MaxResultRows: 1000},
			want: map[string]any{
				"max_result_rows":      1000,
				"result_overflow_mode": "throw",
			},
		},
		{
			name:   "max_rows_to_read caps rows scanned with throw mode",
			limits: chQueryLimits{MaxRowsToRead: 1_000_000},
			want: map[string]any{
				"max_rows_to_read":   int64(1_000_000),
				"read_overflow_mode": "throw",
			},
		},
		{
			name:   "max_memory_usage caps peak query memory",
			limits: chQueryLimits{MaxMemoryBytes: 4 << 30}, // 4 GiB > int32
			want:   map[string]any{"max_memory_usage": int64(4 << 30)},
		},
		{
			name: "all caps together",
			limits: chQueryLimits{
				ExecutionTime:  2 * time.Second,
				MaxResultRows:  500,
				MaxRowsToRead:  2_000_000,
				MaxMemoryBytes: 8 << 30,
			},
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
			name:   "zero caps are omitted even when others are set",
			limits: chQueryLimits{MaxRowsToRead: 42},
			want: map[string]any{
				"max_rows_to_read":   int64(42),
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

// TestResolveReadBudget covers the per-role-override → global-default → admin-
// bypass precedence that both read handlers share.
func TestResolveReadBudget(t *testing.T) {
	t.Parallel()
	defaults := QueryLimits{DefaultMaxRowsToRead: 100, DefaultMaxMemoryBytes: 200}

	tests := []struct {
		name                    string
		perRoleRows, perRoleMem int64
		isAdmin                 bool
		wantRows, wantMem       int64
	}{
		{name: "admin bypasses everything", perRoleRows: 5, perRoleMem: 5, isAdmin: true, wantRows: 0, wantMem: 0},
		{name: "no per-role cap falls back to defaults", wantRows: 100, wantMem: 200},
		{name: "per-role override wins", perRoleRows: 7, perRoleMem: 9, wantRows: 7, wantMem: 9},
		{name: "per-role wins for one, default for the other", perRoleRows: 7, wantRows: 7, wantMem: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotRows, gotMem := defaults.resolveReadBudget(tt.perRoleRows, tt.perRoleMem, tt.isAdmin)
			if gotRows != tt.wantRows || gotMem != tt.wantMem {
				t.Errorf("resolveReadBudget = (%d, %d), want (%d, %d)", gotRows, gotMem, tt.wantRows, tt.wantMem)
			}
		})
	}
}
