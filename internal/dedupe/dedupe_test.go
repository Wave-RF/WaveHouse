package dedupe

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatus_String(t *testing.T) {
	t.Parallel()
	for s, want := range map[Status]string{
		Claimed:   "claimed",
		Duplicate: "duplicate",
		InFlight:  "in_flight",
		Status(0): "unknown",
	} {
		assert.Equal(t, want, s.String())
	}
}
