package dedupe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEnsureSchema_ConnectionError verifies the error path of EnsureSchema
// when no ScyllaDB node is reachable. Integration tests cover the happy path
// with a real Scylla container.
func TestEnsureSchema_ConnectionError(t *testing.T) {
	t.Parallel()

	// Don't match on err.Error() content — gocql reformats its
	// connection-failure messages between versions. The invariant that
	// matters here is just that the error path runs.
	err := EnsureSchema([]string{"127.0.0.1:1"}, "wavehouse_dedupe_test")
	require.Error(t, err)
}
