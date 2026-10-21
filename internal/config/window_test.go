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

// The same for the ingest worker's consumer settings: mq.Default* is their
// one definition, and defaults() must hand boot the same values (#691).
func TestMQConsumerDefaults_MatchMQ(t *testing.T) {
	t.Parallel()
	d := defaults().MQ
	assert.Equal(t, mq.DefaultAckWait, d.AckWait)
	assert.Equal(t, mq.DefaultMaxAckPending, d.MaxAckPending)
	assert.Equal(t, mq.DefaultPrefetch, d.Prefetch)
}
