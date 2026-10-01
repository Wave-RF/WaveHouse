package typelayer

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// TestServerVersion is the ClickHouse version TestEngine binds to — the patch
// the locked 26.8 artifact is built from, so its line resolves to that
// artifact and the bound library is an exact match for the server.
const TestServerVersion = "26.8.15.10"

// testLine is TestServerVersion's version line, the one artifact tests need.
const testLine = "26.8"

// RequireEnv set to "1" turns every artifact skip into a failure. CI sets it,
// so a runner with a broken artifact cache fails loudly instead of quietly
// testing nothing.
const RequireEnv = "WAVEHOUSE_TEST_REQUIRE_CHTYPES"

// missingArtifact is what a developer without the artifact needs to read: the
// exact command that installs it.
const missingArtifact = "chtypes artifact for " + testLine + " (ABI revision 6) not installed: run " +
	"`scripts/fetch-chtypes.sh`, which installs the build chtypes.lock pins into the SDK's per-user cache"

// SkipWithoutArtifact skips t when the chtypes artifact for TestServerVersion's
// line is not on the SDK's search path — or fails it when
// WAVEHOUSE_TEST_REQUIRE_CHTYPES=1. It opens no library. A test that boots
// anything constructing an Engine (the API process role) calls it first, since
// NewEngine refuses to start without an artifact.
func SkipWithoutArtifact(t testing.TB) {
	t.Helper()
	if cause := artifactMissing(); cause != "" {
		skipUnlessRequired(t, cause)
	}
}

var artifactProbe struct {
	once  sync.Once
	cause string
}

// artifactMissing reports why the test line's artifact is not available, ""
// when it is. Probed once per test binary: it reads manifests only.
func artifactMissing() string {
	artifactProbe.once.Do(func() {
		reg, err := chtypes.NewRegistry("", chtypes.WithAutoFetch(false))
		if err != nil {
			artifactProbe.cause = err.Error()
			return
		}
		if !slices.Contains(reg.Versions(), testLine) {
			artifactProbe.cause = fmt.Sprintf("no %s artifact on the registry search path (looked in: %s)",
				testLine, strings.Join(reg.SearchPath(), ", "))
		}
	})
	return artifactProbe.cause
}

// TestEngine opens an Engine on the SDK's default search path and binds the
// given tables for tenant.Default at TestServerVersion in UTC. It skips the
// test when the artifact is absent or does not load (fails it under
// WAVEHOUSE_TEST_REQUIRE_CHTYPES=1), and closes the Engine when the test ends.
func TestEngine(t testing.TB, tables ...*discovery.TableSchema) *Engine {
	t.Helper()
	SkipWithoutArtifact(t)
	eng, err := NewEngine(Config{})
	if err != nil {
		skipUnlessRequired(t, err.Error())
	}
	t.Cleanup(eng.Close)

	eng.Bind(tenant.Default, TestServerVersion, "UTC", tables)
	if cause := eng.tenantCause(tenant.Default); cause != "" {
		skipUnlessRequired(t, cause)
	}
	return eng
}

func skipUnlessRequired(t testing.TB, cause string) {
	t.Helper()
	if os.Getenv(RequireEnv) == "1" {
		t.Fatalf("%s\n%s", missingArtifact, cause)
	}
	t.Skipf("%s\n%s", missingArtifact, cause)
}
