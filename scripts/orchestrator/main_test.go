package main

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSuppressLogDumpReason(t *testing.T) {
	cases := []struct {
		name        string
		dump        string
		otelEnabled string
		otelAddr    string
		// reachable spins a local listener and overrides otelAddr to its
		// address so the probe succeeds.
		reachable bool
		want      string // exact match if non-empty; "" means dump should fire
	}{
		{"default off-collector → dump fires", "", "", "", false, ""},
		{"WH_E2E_LOG_DUMP=0 → suppress", "0", "", "", false, "WH_E2E_LOG_DUMP=0"},
		{"WH_E2E_LOG_DUMP=false → suppress", "false", "", "", false, "WH_E2E_LOG_DUMP=false"},
		{"WH_E2E_LOG_DUMP=no → suppress", "no", "", "", false, "WH_E2E_LOG_DUMP=no"},
		{"WH_E2E_LOG_DUMP=off → suppress", "off", "", "", false, "WH_E2E_LOG_DUMP=off"},
		{"otel enabled but no addr → dump fires", "", "true", "", false, ""},
		{"otel enabled, untrimmed value → ok with trim", "", " true ", "", false, ""},
		{"otel disabled with reachable addr → dump fires", "", "false", "", true, ""},
		{"otel enabled with unreachable addr → dump fires", "", "true", "127.0.0.1:1", false, ""},
		{"otel enabled with reachable addr → suppress", "", "true", "", true, "OTel collector reachable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WH_E2E_LOG_DUMP", tc.dump)
			t.Setenv("WH_OTEL_ENABLED", tc.otelEnabled)
			addr := tc.otelAddr
			if tc.reachable {
				var lc net.ListenConfig
				l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("listen: %v", err)
				}
				defer func() { _ = l.Close() }()
				addr = l.Addr().String()
			}
			t.Setenv("WH_OTEL_ADDR", addr)

			got := suppressLogDumpReason()
			switch {
			case tc.want == "":
				assert.Equal(t, "", got)
			case strings.HasPrefix(tc.want, "WH_E2E_LOG_DUMP="):
				assert.Equal(t, tc.want, got)
			default:
				assert.True(t, strings.HasPrefix(got, tc.want), "want prefix %q, got %q", tc.want, got)
			}
		})
	}
}
