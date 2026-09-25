package mq

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// TestMain silences the default logger, which the embedded server logs
// through, and turns off the embedded server's fsync per write.
func TestMain(m *testing.M) {
	logtest.Silence()
	EmbeddedSyncAlways = false
	m.Run()
}
