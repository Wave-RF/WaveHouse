package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// runMQ implements `wavehouse mq <command>`: tooling for the message queue.
// Exit codes: 0 ok, 1 failed, 2 usage.
func runMQ(args []string, stdout, stderr io.Writer) int {
	usage := func(w io.Writer) {
		_, _ = fmt.Fprint(w, `usage: wavehouse mq <command>

commands:
  manifests   print the nack resources for an external NATS JetStream
`)
	}
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "manifests":
		return runMQManifests(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "wavehouse mq: unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}
}

// runMQManifests implements `wavehouse mq manifests`: print the nack
// Stream, Consumer and KeyValue resources for the topology WaveHouse checks at boot
// under mq.backend: nats, for the operator to apply.
func runMQManifests(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("mq manifests", flag.ContinueOnError)
	fs.SetOutput(stderr)
	partitions := fs.Int("partitions", mq.DefaultNATSPartitions, "number of ingest partition streams (mq.nats.partitions)")
	prefix := fs.String("prefix", mq.DefaultNATSSubjectPrefix, "subject prefix (mq.nats.subject_prefix)")
	replicas := fs.Int("replicas", 3, "replicas for every stream and the lease bucket")
	bucket := fs.String("coord-bucket", "", "the lease KV bucket (coord.nats.bucket); empty is <prefix>_coord")
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), `usage: wavehouse mq manifests [--partitions N] [--prefix wh] [--replicas 3] [--coord-bucket B]

Print the nack (jetstream.nats.io/v1beta2) Stream, Consumer and KeyValue
resources for the JetStream topology WaveHouse needs under mq.backend: nats
and coord.backend: nats, as YAML for kubectl apply. WaveHouse never creates these itself; it checks them at boot.

`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: unexpected argument %q\n", fs.Arg(0))
		fs.Usage()
		return 2
	}
	if *replicas < 1 {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: --replicas must be at least 1\n")
		return 2
	}
	err := mq.WriteNATSManifests(stdout, mq.NATSManifestOptions{
		Topology: mq.NATSTopology{Prefix: *prefix, Partitions: *partitions, CoordBucket: *bucket},
		Replicas: *replicas,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: %v\n", err)
		return 1
	}
	return 0
}
