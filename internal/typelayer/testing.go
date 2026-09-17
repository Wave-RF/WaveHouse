package typelayer

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// TestServerVersion is the ClickHouse version TestEngine binds to — a real
// 26.6 patch release, so the registry resolves the 26.6 artifact line.
const TestServerVersion = "26.6.3.62"

// missingArtifact is what a developer without the artifact needs to read: the
// exact command that installs it.
const missingArtifact = "chtypes artifact for 26.6 not installed: run `go run github.com/wave-rf/chtypes/go/cmd/chtypes@v0.2.1 fetch 26.6`"

// TestEngine opens an Engine on the SDK's default search path and binds the
// given tables at TestServerVersion in UTC. It skips the test when the artifact
// is absent — unless WAVEHOUSE_TEST_REQUIRE_CHTYPES=1, which CI sets, so a runner with a
// broken artifact cache fails loudly instead of quietly testing nothing.
func TestEngine(t testing.TB, tables ...*discovery.TableSchema) *Engine {
	t.Helper()
	eng, err := NewEngine(Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		skipUnlessRequired(t, err.Error())
	}
	t.Cleanup(eng.Close)

	eng.Bind(TestServerVersion, "UTC", tables)
	eng.mu.RLock()
	global := eng.global
	eng.mu.RUnlock()
	if global != "" {
		skipUnlessRequired(t, global)
	}
	return eng
}

func skipUnlessRequired(t testing.TB, cause string) {
	t.Helper()
	if os.Getenv("WAVEHOUSE_TEST_REQUIRE_CHTYPES") == "1" {
		t.Fatalf("%s\n%s", missingArtifact, cause)
	}
	t.Skipf("%s\n%s", missingArtifact, cause)
}
