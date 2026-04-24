package dedupe

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnsureSchema_ConnectionError verifies the error path of EnsureSchema
// when no ScyllaDB node is reachable. Integration tests cover the happy path
// with a real Scylla container.
func TestEnsureSchema_ConnectionError(t *testing.T) {
	t.Parallel()

	err := EnsureSchema([]string{"127.0.0.1:1"}, "wavehouse_dedupe_test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect")
}
