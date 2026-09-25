package app

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// TestMain turns off the embedded broker's fsync per write, which every boot
// here pays for opening its queues.
func TestMain(m *testing.M) {
	mq.EmbeddedSyncAlways = false
	m.Run()
}
