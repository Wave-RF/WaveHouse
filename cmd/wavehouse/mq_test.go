package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shipped manifests are the generator's output for four partitions.
// Regenerate with: go run ./cmd/wavehouse mq manifests --partitions 4 > deployments/nats/jetstream.yaml
func TestRunMQManifests_MatchesShipped(t *testing.T) {
	want, err := os.ReadFile("../../deployments/nats/jetstream.yaml")
	require.NoError(t, err)
	var out, errOut bytes.Buffer
	require.Equal(t, 0, runMQ([]string{"manifests", "--partitions", "4"}, &out, &errOut), errOut.String())
	assert.Equal(t, string(want), out.String(), "deployments/nats/jetstream.yaml is stale; regenerate it")
}

func TestRunMQ_ExitCodes(t *testing.T) {
	cases := map[string]struct {
		args []string
		code int
	}{
		"no command":        {nil, 2},
		"unknown command":   {[]string{"frobnicate"}, 2},
		"help":              {[]string{"help"}, 0},
		"manifests help":    {[]string{"manifests", "-h"}, 0},
		"stray argument":    {[]string{"manifests", "extra"}, 2},
		"unknown flag":      {[]string{"manifests", "--nope"}, 2},
		"zero replicas":     {[]string{"manifests", "--replicas", "0"}, 2},
		"bad prefix":        {[]string{"manifests", "--prefix", "a.b"}, 1},
		"bad partitions":    {[]string{"manifests", "--partitions", "-1"}, 1},
		"bad coord bucket":  {[]string{"manifests", "--coord-bucket", "a.b"}, 1},
		"defaults generate": {[]string{"manifests"}, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			assert.Equal(t, tc.code, runMQ(tc.args, &out, &errOut), errOut.String())
		})
	}
}

func TestRunMQManifests_NamesTheLeaseBucket(t *testing.T) {
	var out, errOut bytes.Buffer
	require.Equal(t, 0, runMQ([]string{"manifests", "--prefix", "acme", "--coord-bucket", "acme_leases"}, &out, &errOut), errOut.String())
	assert.Contains(t, out.String(), "kind: KeyValue\nmetadata:\n  name: acme-coord\nspec:\n  bucket: acme_leases\n")
}
