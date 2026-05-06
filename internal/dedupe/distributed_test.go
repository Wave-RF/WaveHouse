package dedupe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewDistributed_ConnectionError exercises the error path of
// NewDistributed. Unit tests don't run against a real ScyllaDB cluster — see
// integration tests for happy-path coverage of CheckAndMark.
func TestNewDistributed_ConnectionError(t *testing.T) {
	t.Parallel()

	// Port 1 is privileged on Linux and never has a ScyllaDB node listening,
	// so CreateSession will fail quickly.
	_, err := NewDistributed([]string{"127.0.0.1:1"}, "wavehouse_dedupe_test")
	require.Error(t, err)
}
