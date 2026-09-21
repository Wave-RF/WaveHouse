package mq

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// TestMain silences the default logger, which the embedded server logs
// through.
func TestMain(m *testing.M) {
	logtest.Silence()
	m.Run()
}
