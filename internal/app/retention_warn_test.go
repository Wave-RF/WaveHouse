package app

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Under mq.backend=nats the operator's duplicate_window can exceed the 2m
// floor settings enforces, so boot and every reload warn about each served
// tenant with dedupe on whose finite retention, default or per table, is
// under it. Forever ("0"), a longer retention and a tenant with dedupe off
// are quiet.
func TestWarnShortRetention(t *testing.T) {
	guardGlobals(t)
	dedupe := func(enabled bool, retention string, tables map[string]any) map[string]any {
		return map[string]any{"dedupe": map[string]any{
			"enabled": enabled, "id_field": "event_id", "require_id": false, "retention": retention, "tables": tables,
		}}
	}
	root := writeNestedSettings(t, map[string]map[string]any{
		"acme":    dedupe(true, "5m", map[string]any{"clicks": map[string]any{"retention": "10m"}, "views": map[string]any{"retention": "3m"}}),
		"globex":  dedupe(true, "0", nil),
		"initech": dedupe(false, "3m", nil),
	})
	tenants, findings := settings.Open(root)
	require.NotNil(t, tenants, "findings: %v", findings)

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	(&App{tenants: tenants}).warnShortRetention(8 * time.Minute)

	out := buf.String()
	assert.Equal(t, 2, bytes.Count(buf.Bytes(), []byte("dedupe retention is shorter")), out)
	assert.Contains(t, out, `tenant=acme table="" retention=5m0s duplicate_window=8m0s`)
	assert.Contains(t, out, `tenant=acme table=views retention=3m0s`)
	assert.NotContains(t, out, "globex")
	assert.NotContains(t, out, "initech")
}
