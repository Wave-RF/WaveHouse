package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shipped manifests are the generator's output for four partitions of eight shards.
// Regenerate with: go run ./cmd/wavehouse mq manifests --partitions 4 --shards 8 > deployments/nats/jetstream.yaml
func TestRunMQManifests_MatchesShipped(t *testing.T) {
	want, err := os.ReadFile("../../deployments/nats/jetstream.yaml")
	require.NoError(t, err)
	var out, errOut bytes.Buffer
	require.Equal(t, 0, runMQ([]string{"manifests", "--partitions", "4", "--shards", "8"}, &out, &errOut), errOut.String())
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
		"negative lease":    {[]string{"manifests", "--dedupe-lease", "-1s"}, 2},
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

// A lease past the shipped 2m window's reach widens every partition's window
// to cover it (90s + 90s + 1s), so the manifests pass the boot check.
func TestRunMQManifests_CoversTheDedupeLease(t *testing.T) {
	var out, errOut bytes.Buffer
	require.Equal(t, 0, runMQ([]string{"manifests", "--partitions", "1", "--dedupe-lease", "90s"}, &out, &errOut), errOut.String())
	assert.Contains(t, out.String(), "duplicateWindow: 3m1s\n")
}

// The permissions name every shard's durable, and match what the verifier's
// test holds the shipped values to.
func TestRunMQPermissions(t *testing.T) {
	var out, errOut bytes.Buffer
	require.Equal(t, 0, runMQ([]string{"permissions", "--shards", "3", "--prefix", "acme"}, &out, &errOut), errOut.String())
	for _, want := range []string{"$JS.API.CONSUMER.MSG.NEXT.*.wh-ingest-2", "$JS.API.CONSUMER.UNPIN.*.wh-ingest-0", "$JS.API.CONSUMER.RESET.*.wh-ingest-1", "acme.hist.>", "_INBOX_acme.>"} {
		assert.Contains(t, out.String(), want)
	}
	assert.NotContains(t, out.String(), "wh-ingest-3")
	out.Reset()
	errOut.Reset()
	assert.Equal(t, 2, runMQ([]string{"permissions", "--shards", "0"}, &out, &errOut))
	assert.Contains(t, errOut.String(), "--shards must be at least 1")
	errOut.Reset()
	assert.Equal(t, 1, runMQ([]string{"permissions", "--shards", "257"}, &out, &errOut))
	assert.Contains(t, errOut.String(), "shards must be from 1 to 256")
}
