package config

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/stretchr/testify/assert"
)

// config must not import internal/mq (it would pull NATS into every
// importer of config), so the lease cap mirrors the window; this pins them.
func TestEmbeddedDuplicateWindow_MatchesMQ(t *testing.T) {
	t.Parallel()
	assert.Equal(t, mq.EmbeddedDuplicateWindow, embeddedDuplicateWindow)
}
