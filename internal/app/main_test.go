package app

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// TestMain turns off the embedded broker's fsync per write, which every boot
// here pays for opening its queues, and silences the default logger once, so
// the tests that leave it alone can run in parallel.
func TestMain(m *testing.M) {
	mq.EmbeddedSyncAlways = false
	logtest.Silence()
	m.Run()
}
