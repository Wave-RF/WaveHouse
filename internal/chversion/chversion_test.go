package chversion

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLine(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "26.8", Line("26.8.15.10"))
	assert.Equal(t, "24.10", Line("24.10.1.2"))
}

// TestImageCopies_MatchThePin: YAML cannot import a Go constant, so the
// compose files and workflows carry their own copy of the image tag.
func TestImageCopies_MatchThePin(t *testing.T) {
	t.Parallel()
	ref := regexp.MustCompile(`clickhouse/clickhouse-server:[0-9][0-9.]*`)
	var files []string
	for _, glob := range []string{"../../deployments/compose/*.yaml", "../../.github/workflows/*.yml"} {
		m, err := filepath.Glob(glob)
		require.NoError(t, err)
		files = append(files, m...)
	}
	found := 0
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a tracked repo file
		require.NoError(t, err)
		for _, got := range ref.FindAllString(string(b), -1) {
			found++
			assert.Equal(t, TestImage, got, "%s", f)
		}
	}
	assert.Positive(t, found, "the compose files pin the image")
}

// TestFetchScript_DerivesTheLine: scripts/fetch-chtypes.sh reads Test from
// this file, so a reformatted declaration must not silently break it.
func TestFetchScript_DerivesTheLine(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("no bash")
	}
	out, err := exec.CommandContext(t.Context(), "bash", "../../scripts/fetch-chtypes.sh", "--print-line").Output()
	require.NoError(t, err)
	assert.Equal(t, Line(Test), strings.TrimSpace(string(out)))
}
