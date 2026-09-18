package api

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// TestMain silences the default logger the package logs through, so a
// failing test's output is not buried; tests that assert on log output
// capture it (logtest.Capture).
func TestMain(m *testing.M) {
	logtest.Silence()
	m.Run()
}
