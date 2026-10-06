package typelayer

import (
	"go/build"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// This package's own tests cannot import typelayertest (it imports this
// package), so they carry the same three pieces: the version they bind, its
// line, and the engine helper. Kept in step with typelayertest.TestEngine.
const (
	testServerVersion = "26.8.15.10"
	testLine          = "26.8"
)

// TestMain commits a UTC image zone before any test binds, so the zone every
// test reads in does not depend on which test happens to bind first. The
// subprocesses of TestImage_DerivedFromTheFirstTenantServed commit none, and
// derive it through Bind as production does. Every process first gets the
// ZONEINFO that names goOnlyZone.
func TestMain(m *testing.M) {
	cleanup, err := withGoOnlyZone()
	if err != nil {
		panic(err)
	}
	if os.Getenv(imageZoneEnv) != imageUnset {
		image.mu.Lock()
		err := chtypes.Setup(chtypes.SetupOptions{Timezone: "UTC"})
		image.zone = "UTC"
		image.mu.Unlock()
		if err != nil {
			panic(err)
		}
	}
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// testEngine opens an Engine on the SDK's default layouts and binds tables
// for tenant.Default at testServerVersion in UTC, skipping the test when the
// artifact is absent or does not load — failing it under
// WAVEHOUSE_TEST_REQUIRE_CHTYPES=1.
func testEngine(t testing.TB, tables ...*discovery.TableSchema) *Engine {
	t.Helper()
	eng, err := NewEngine(Config{})
	if err != nil {
		skipWithoutArtifact(t, err.Error())
	}
	t.Cleanup(eng.Close)
	eng.Bind(tenant.Default, testServerVersion, "UTC", tables)
	if cause := eng.TenantCause(tenant.Default); cause != "" {
		skipWithoutArtifact(t, cause)
	}
	return eng
}

// TestPackageDoesNotImportTesting: the API process links this package, so a
// test helper in a non-test file drags the testing package (and its flags)
// into the production binary. Test helpers for other packages live in
// typelayertest.
func TestPackageDoesNotImportTesting(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	require.NoError(t, err)
	assert.NotContains(t, pkg.Imports, "testing")
}

func skipWithoutArtifact(t testing.TB, cause string) {
	t.Helper()
	const msg = "chtypes artifact for " + testLine + " not installed: run `scripts/fetch-chtypes.sh`"
	if os.Getenv("WAVEHOUSE_TEST_REQUIRE_CHTYPES") == "1" {
		t.Fatalf("%s\n%s", msg, cause)
	}
	t.Skipf("%s\n%s", msg, cause)
}
