package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// runMQ implements `wavehouse mq <command>`: tooling for the message queue.
// Exit codes: 0 ok, 1 failed, 2 usage.
func runMQ(args []string, stdout, stderr io.Writer) int {
	usage := func(w io.Writer) {
		_, _ = fmt.Fprint(w, `usage: wavehouse mq <command>

commands:
  manifests     print the nack resources for an external NATS JetStream
  permissions   print the wavehouse NATS user's permissions for it
`)
	}
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "manifests":
		return runMQManifests(args[1:], stdout, stderr)
	case "permissions":
		return runMQPermissions(args[1:], stdout, stderr)
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
	shards := fs.Int("shards", mq.DefaultNATSShards, "shards of every partition, one durable each (mq.nats.shards)")
	prefix := fs.String("prefix", mq.DefaultNATSSubjectPrefix, "subject prefix (mq.nats.subject_prefix)")
	replicas := fs.Int("replicas", 3, "replicas for every stream and the lease bucket")
	consumer := fs.String("ingest-consumer", mq.DefaultNATSIngestConsumer, "the shard durables' name prefix (mq.nats.ingest_consumer)")
	history := fs.String("history-stream", "", "the history stream (mq.nats.history_stream); empty is <PREFIX>_HISTORY")
	publishTimeout := fs.Duration("publish-timeout", mq.DefaultNATSPublishTimeout, "one publish attempt's bound (mq.nats.publish_timeout); the partitions' duplicate window covers every attempt")
	bucket := fs.String("coord-bucket", "", "the lease KV bucket (coord.nats.bucket); empty is <prefix>_coord")
	lease := fs.Duration("dedupe-lease", dedupe.DefaultLease, "the dedupe.lease the partitions' duplicate window must cover")
	fileStore := fs.String("file-store", mq.FormatStoreSize(mq.DefaultNATSFileStore), "every server's JetStream max_file_store (the Helm chart's fileStore.maxSize, else its PVC size), which the streams' maxBytes must fit in")
	partitionBytes := fs.String("partition-max-bytes", "", "each ingest partition's maxBytes; empty is 15% of --file-store")
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), `usage: wavehouse mq manifests [--partitions N] [--shards V] [--prefix wh] [--replicas 3] [--ingest-consumer wh-ingest] [--history-stream S] [--publish-timeout 5s] [--coord-bucket B] [--dedupe-lease 30s] [--file-store 100Gi] [--partition-max-bytes 15Gi]

Print the nack (jetstream.nats.io/v1beta2) Stream, Consumer and KeyValue
resources for the JetStream topology WaveHouse needs under mq.backend: nats
and coord.backend: nats, as YAML for kubectl apply. WaveHouse never creates these itself; it checks them at boot.
The streams' maxBytes are sized from --file-store: each partition 15%, the
history 10% and the DLQ 5%, so four partitions leave a quarter of it spare.
It refuses streams that together reserve more than --file-store.

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
	if *lease < 0 {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: --dedupe-lease must not be negative\n")
		return 2
	}
	if *shards < 1 {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: --shards must be at least 1\n")
		return 2
	}
	if *replicas < 1 {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: --replicas must be at least 1\n")
		return 2
	}
	if *publishTimeout <= 0 {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: --publish-timeout must be positive\n")
		return 2
	}
	store, err := positiveSize(*fileStore)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: --file-store: %v\n", err)
		return 2
	}
	var partitionMax int64
	if *partitionBytes != "" {
		if partitionMax, err = positiveSize(*partitionBytes); err != nil {
			_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: --partition-max-bytes: %v\n", err)
			return 2
		}
	}
	err = mq.WriteNATSManifests(stdout, mq.NATSManifestOptions{
		Topology: mq.NATSTopology{
			Prefix: *prefix, Partitions: *partitions, Shards: *shards, IngestConsumer: *consumer, HistoryStream: *history,
			PublishTimeout: *publishTimeout, CoordBucket: *bucket, DedupeLease: *lease,
		},
		Replicas:          *replicas,
		FileStore:         store,
		PartitionMaxBytes: partitionMax,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq manifests: %v\n", err)
		return 1
	}
	return 0
}

// runMQPermissions implements `wavehouse mq permissions`: print the
// wavehouse NATS user's permissions for the topology, which name every shard
// durable, so an operator regenerates them with the shard count.
func runMQPermissions(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("mq permissions", flag.ContinueOnError)
	fs.SetOutput(stderr)
	shards := fs.Int("shards", mq.DefaultNATSShards, "shards of every partition (mq.nats.shards); while a lower count's durables drain, the higher")
	prefix := fs.String("prefix", mq.DefaultNATSSubjectPrefix, "subject prefix (mq.nats.subject_prefix)")
	consumer := fs.String("ingest-consumer", mq.DefaultNATSIngestConsumer, "the shard durables' name prefix (mq.nats.ingest_consumer)")
	history := fs.String("history-stream", "", "the history stream (mq.nats.history_stream); empty is <PREFIX>_HISTORY")
	bucket := fs.String("coord-bucket", "", "the lease KV bucket (coord.nats.bucket); empty is <prefix>_coord")
	domain := fs.String("js-domain", "", "the JetStream domain WaveHouse connects to (mq.nats.js_domain); its API requests go to $JS.<domain>.API.>")
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), `usage: wavehouse mq permissions [--shards V] [--prefix wh] [--ingest-consumer wh-ingest] [--history-stream S] [--coord-bucket B] [--js-domain D]

Print the permissions block for the wavehouse user of a NATS Helm values
file (config.merge.accounts.<account>.users[]), for the topology that
wavehouse mq manifests prints with the same flags.

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
		_, _ = fmt.Fprintf(stderr, "wavehouse mq permissions: unexpected argument %q\n", fs.Arg(0))
		fs.Usage()
		return 2
	}
	if *shards < 1 {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq permissions: --shards must be at least 1\n")
		return 2
	}
	err := mq.WriteNATSPermissions(stdout, mq.NATSTopology{
		Prefix: *prefix, Shards: *shards, IngestConsumer: *consumer, HistoryStream: *history, CoordBucket: *bucket,
	}, *domain)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "wavehouse mq permissions: %v\n", err)
		return 1
	}
	return 0
}

// positiveSize reads a byte count of at least one byte (mq.ParseStoreSize).
func positiveSize(s string) (int64, error) {
	n, err := mq.ParseStoreSize(s)
	if err == nil && n == 0 {
		err = errors.New("must be positive")
	}
	return n, err
}
