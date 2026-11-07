package api

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil/mutationtest"
	"github.com/Wave-RF/WaveHouse/pkg/chsql"
	"github.com/stretchr/testify/assert"
)

// TestIsMutation runs the shared cases against the classifier the pipes and
// the query span route by; the integration suite checks the same cases
// against ClickHouse's parser. The cases import internal/testutil, which
// pkg/chsql's own tests cannot, so they run here.
func TestIsMutation(t *testing.T) {
	t.Parallel()
	for _, tc := range mutationtest.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.Mutation, chsql.IsMutation(tc.SQL))
		})
	}
}
