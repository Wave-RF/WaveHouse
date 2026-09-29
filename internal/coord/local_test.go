package coord_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/coord/coordtest"
)

func TestLocal_Conformance(t *testing.T) {
	var mu sync.Mutex
	holders := map[string]*coord.Local{}
	coordtest.Conformance(t, func(t *testing.T) (a, b coord.Coordinator) {
		l := coord.NewLocal()
		mu.Lock()
		holders[t.Name()] = l
		mu.Unlock()
		return l, l.Peer()
	}, coordtest.WithLoss(func(t *testing.T, name string) {
		mu.Lock()
		defer mu.Unlock()
		holders[t.Name()].Revoke(name)
	}))
}

func TestLocal_TablesAreIndependent(t *testing.T) {
	a, err := coord.NewLocal().TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
	b, err := coord.NewLocal().TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err, "two processes on local coordination never see each other")
	assert.Equal(t, a.Token(), b.Token())
}
