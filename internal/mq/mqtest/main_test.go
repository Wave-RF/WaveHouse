package mqtest_test

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// TestMain silences the default logger, which the embedded server logs
// through, and turns off the embedded server's fsync per write: the suite
// asserts nothing across a crash, and on a work queue every ack is a write.
func TestMain(m *testing.M) {
	logtest.Silence()
	mq.EmbeddedSyncAlways = false
	m.Run()
}
