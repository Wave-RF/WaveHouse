package main

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/stretchr/testify/assert"
)

// TestApplyHotFields pins the wiring half of the hot-reload invariant: every
// entry in the hotFields whitelist (internal/config/reload.go) must land in
// its consumer when the apply callback runs — an entry with no push here is
// reported as applied without taking effect. When extending the whitelist,
// extend this test with the new field alongside the push in applyHotFields.
func TestApplyHotFields(t *testing.T) {
	t.Parallel()
	ingest := &api.IngestHandler{}

	cfg := &config.Config{}
	cfg.Dedupe.IDField = "view_id"
	cfg.Dedupe.RequireID = true
	applyHotFields(ingest)(cfg)

	assert.Equal(t, api.DedupeSettings{IDField: "view_id", RequireID: true},
		ingest.DedupeSettings(), "every hotFields entry must reach the handler")
}
