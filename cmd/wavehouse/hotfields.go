package main

import (
	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/config"
)

// applyHotFields returns the apply callback for config.NewReloader: the single
// mapping from the hotFields whitelist (internal/config/reload.go) onto the
// running process, shared by boot and reload so the two can't drift apart.
// Every whitelist entry must be pushed into its consumer here — an entry with
// no push would be reported as applied without taking effect.
func applyHotFields(ingest *api.IngestHandler) func(*config.Config) {
	return func(c *config.Config) {
		ingest.SetDedupeSettings(api.DedupeSettings{
			IDField:   c.Dedupe.IDField,
			RequireID: c.Dedupe.RequireID,
		})
	}
}
