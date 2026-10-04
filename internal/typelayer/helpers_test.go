package typelayer

import (
	"go/build"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestMain commits the image zone before any test binds, so the zone every
// test reads in does not depend on which test happens to bind first: UTC,
// unless the subprocess of TestImage_FirstTenantInAnotherZone names another.
func TestMain(m *testing.M) {
	if zone := testImageZone(); zone != "unset" {
		if _, err := setupImage(zone); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
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
