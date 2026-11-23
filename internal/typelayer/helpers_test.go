package typelayer

import (
	"go/build"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/v2/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/chversion"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// This package's own tests cannot import typelayertest (it imports this
// package), so they carry the same pieces: the version they bind, its line,
// and the engine helper. Kept in step with typelayertest.TestEngine.
const testServerVersion = chversion.Test

var testLine = chversion.Line(testServerVersion)

// TestMain sets up the image zone imageZoneEnv names, before any library
// loads; without it the process keeps chtypes' default, UTC, as production
// does.
func TestMain(m *testing.M) {
	if zone := os.Getenv(imageZoneEnv); zone != "" {
		if err := chtypes.Setup(chtypes.SetupOptions{Timezone: zone}); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

// testEngine opens an Engine on the SDK's default layouts, with autofetch, and
// binds tables for tenant.Default at testServerVersion in UTC, skipping the
// test when the artifact is absent and cannot be fetched, or does not load —
// failing it under WAVEHOUSE_TEST_REQUIRE_CHTYPES=1.
func testEngine(t testing.TB, tables ...*discovery.TableSchema) *Engine {
	t.Helper()
	eng, err := NewEngine(Config{AutoFetch: true})
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
	msg := "the chtypes artifact for " + testLine + " is not available " +
		"(`scripts/fetch-chtypes.sh` fetches it into the per-user cache, or shows why it cannot)"
	if os.Getenv("WAVEHOUSE_TEST_REQUIRE_CHTYPES") == "1" {
		t.Fatalf("%s\n%s", msg, cause)
	}
	t.Skipf("%s\n%s", msg, cause)
}
