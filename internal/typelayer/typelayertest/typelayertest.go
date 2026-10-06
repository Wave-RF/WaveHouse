// Package typelayertest opens a typelayer.Engine on the chtypes artifact for
// the pinned test ClickHouse (chversion.Test), for tests in other packages. A
// missing artifact is fetched into the SDK's per-user cache, the one a
// developer's other checkouts and CI share, and a test that needs one is
// skipped when that fails. It is test-only: nothing the wavehouse binary links
// imports it, so the testing package stays out of production builds.
package typelayertest

import (
	"os"
	"sync"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/chversion"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// TestServerVersion is the ClickHouse version TestEngine binds to: the pinned
// test ClickHouse, so its line resolves to the artifact the suites' server
// runs. The line's newest installed build answers it, usually that patch.
const TestServerVersion = chversion.Test

// RequireEnv set to "1" turns every artifact skip into a failure. CI sets it,
// so a runner that cannot fetch the artifact fails loudly instead of testing
// nothing.
const RequireEnv = "WAVEHOUSE_TEST_REQUIRE_CHTYPES"

// probeTenant is the tenant SkipWithoutArtifact binds, with no tables.
const probeTenant tenant.ID = "typelayertest-probe"

// SkipWithoutArtifact makes sure the chtypes artifact for TestServerVersion's
// line is installed for this host, fetching it into the per-user cache
// (the SDK default, or $CHTYPES_CACHE) on a miss, and skips t when it
// cannot — or fails it under WAVEHOUSE_TEST_REQUIRE_CHTYPES=1. It opens the
// line's library the way a UTC tenant's first bind does, so the process's
// image zone is UTC from then on. A test that boots an API process with
// autofetch off calls it first.
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
// when it is. Probed once per test binary.
func artifactMissing() string {
	artifactProbe.once.Do(func() {
		eng, err := typelayer.NewEngine(typelayer.Config{AutoFetch: true})
		if err != nil {
			artifactProbe.cause = err.Error()
			return
		}
		defer eng.Close()
		eng.Bind(probeTenant, TestServerVersion, "UTC", nil)
		artifactProbe.cause = eng.TenantCause(probeTenant)
	})
	return artifactProbe.cause
}

// TestEngine opens an Engine on the SDK's default layouts, with autofetch, and
// binds the given tables for tenant.Default at TestServerVersion in UTC. The
// first bind in a process fixes its image zone, so a test binary that binds
// here first runs a UTC image. It skips the test when the artifact is absent
// and cannot be fetched, or does not load (fails it under
// WAVEHOUSE_TEST_REQUIRE_CHTYPES=1), and closes the Engine when the test ends.
func TestEngine(t testing.TB, tables ...*discovery.TableSchema) *typelayer.Engine {
	t.Helper()
	SkipWithoutArtifact(t)
	eng, err := typelayer.NewEngine(typelayer.Config{AutoFetch: true})
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

// skipUnlessRequired skips t because the artifact cannot serve it — or fails
// t under WAVEHOUSE_TEST_REQUIRE_CHTYPES=1.
func skipUnlessRequired(t testing.TB, cause string) {
	t.Helper()
	const msg = "the chtypes artifact for the test ClickHouse line is not available " +
		"(`scripts/fetch-chtypes.sh` fetches it into the per-user cache, or shows why it cannot)"
	if os.Getenv(RequireEnv) == "1" {
		t.Fatalf("%s\n%s", msg, cause)
	}
	t.Skipf("%s\n%s", msg, cause)
}
