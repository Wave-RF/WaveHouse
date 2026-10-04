// Package typelayertest opens a typelayer.Engine on the locked chtypes
// artifact for tests in other packages, and skips a test that needs one when
// it is not installed. It is test-only: nothing the wavehouse binary links
// imports it, so the testing package stays out of production builds.
package typelayertest

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
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
const missingArtifact = "chtypes v1 artifact for " + testLine + " not installed: run " +
	"`scripts/fetch-chtypes.sh`, which installs the build chtypes.lock pins into the SDK's per-user cache " +
	"(~/.cache/chtypes/v1, or $CHTYPES_CACHE)"

// SkipWithoutArtifact skips t when no chtypes artifact for TestServerVersion's
// line is installed for this host — or fails it when
// WAVEHOUSE_TEST_REQUIRE_CHTYPES=1. It opens no library. A test that boots
// anything constructing an Engine (the API process role) calls it first, since
// typelayer.NewEngine refuses to start without an artifact.
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
// when it is. Probed once per test binary: it reads install records only.
func artifactMissing() string {
	artifactProbe.once.Do(func() {
		reg, err := chtypes.NewRegistry(chtypes.WithAutoFetch(false))
		if err != nil {
			artifactProbe.cause = err.Error()
			return
		}
		installed, err := reg.Installed()
		if err != nil {
			artifactProbe.cause = err.Error()
			return
		}
		platform := runtime.GOOS + "-" + runtime.GOARCH
		if !slices.ContainsFunc(installed, func(r chtypes.Resolved) bool {
			return r.Platform == platform && strings.HasPrefix(r.Version, testLine+".")
		}) {
			artifactProbe.cause = fmt.Sprintf("no %s artifact installed for %s", testLine, platform)
		}
	})
	return artifactProbe.cause
}

// TestEngine opens an Engine on the SDK's default layouts and binds the given
// tables for tenant.Default at TestServerVersion in UTC. The first bind in a
// process fixes its image zone, so a test binary that binds here first runs a
// UTC image. It skips the
// test when the artifact is absent or does not load (fails it under
// WAVEHOUSE_TEST_REQUIRE_CHTYPES=1), and closes the Engine when the test ends.
func TestEngine(t testing.TB, tables ...*discovery.TableSchema) *typelayer.Engine {
	t.Helper()
	SkipWithoutArtifact(t)
	eng, err := typelayer.NewEngine(typelayer.Config{})
	if err != nil {
		skipUnlessRequired(t, err.Error())
	}
	t.Cleanup(eng.Close)

	eng.Bind(tenant.Default, TestServerVersion, "UTC", tables)
	if cause := eng.TenantCause(tenant.Default); cause != "" {
		skipUnlessRequired(t, cause)
	}
	return eng
}

// skipUnlessRequired skips t because the artifact cannot serve it, naming the command that
// installs it — or fails t under WAVEHOUSE_TEST_REQUIRE_CHTYPES=1.
func skipUnlessRequired(t testing.TB, cause string) {
	t.Helper()
	if os.Getenv(RequireEnv) == "1" {
		t.Fatalf("%s\n%s", missingArtifact, cause)
	}
	t.Skipf("%s\n%s", missingArtifact, cause)
}
