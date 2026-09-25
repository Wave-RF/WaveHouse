package mq

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// NATSManifestOptions sizes the nack CRs WriteNATSManifests renders. Zero
// fields take the defaults below, which are starting points to tune, not
// recommendations for any particular load.
type NATSManifestOptions struct {
	Topology NATSTopology
	// Replicas is every stream's replica count (default 3).
	Replicas int
	// PartitionMaxBytes caps each ingest partition (default 50 GiB): the
	// backlog of unwritten rows its tenants share.
	PartitionMaxBytes int64
	// MaxMsgsPerSubject caps one topic's backlog in a partition (default
	// 1,000,000).
	MaxMsgsPerSubject int64
	// HistoryMaxAge is how long SSE can replay (default 15m, the seed's
	// stream.gap_window_minutes); at least the longest tenant gap window.
	HistoryMaxAge time.Duration
	// HistoryMaxBytes caps the history (default 20 GiB).
	HistoryMaxBytes int64
	// DLQMaxBytes caps the dead-letter stream (default 5 GiB).
	DLQMaxBytes int64
	// DLQMaxMsgsPerSubject caps one topic's parked rows (default 100,000).
	DLQMaxMsgsPerSubject int64
}

func (o NATSManifestOptions) withDefaults() NATSManifestOptions {
	o.Topology = o.Topology.withDefaults()
	if o.Replicas == 0 {
		o.Replicas = 3
	}
	if o.PartitionMaxBytes == 0 {
		o.PartitionMaxBytes = 50 << 30
	}
	if o.MaxMsgsPerSubject == 0 {
		o.MaxMsgsPerSubject = 1_000_000
	}
	if o.HistoryMaxAge == 0 {
		o.HistoryMaxAge = 15 * time.Minute
	}
	if o.HistoryMaxBytes == 0 {
		o.HistoryMaxBytes = 20 << 30
	}
	if o.DLQMaxBytes == 0 {
		o.DLQMaxBytes = 5 << 30
	}
	if o.DLQMaxMsgsPerSubject == 0 {
		o.DLQMaxMsgsPerSubject = 100_000
	}
	return o
}

// The nack (jetstream.nats.io/v1beta2) custom resources, only the fields the
// topology sets. nack's own defaults are not JetStream's (storage memory,
// ackWait 1ns), so every field the verifier checks is written out.
type nackObject struct {
	APIVersion string       `yaml:"apiVersion"`
	Kind       string       `yaml:"kind"`
	Metadata   nackMetadata `yaml:"metadata"`
	Spec       any          `yaml:"spec"`
}

type nackMetadata struct {
	Name string `yaml:"name"`
}

type nackStream struct {
	Name              string            `yaml:"name"`
	Subjects          []string          `yaml:"subjects,omitempty"`
	Sources           []nackSource      `yaml:"sources,omitempty"`
	Retention         string            `yaml:"retention"`
	Discard           string            `yaml:"discard"`
	DiscardPerSubject bool              `yaml:"discardPerSubject,omitempty"`
	MaxBytes          int64             `yaml:"maxBytes"`
	MaxAge            string            `yaml:"maxAge,omitempty"`
	MaxMsgsPerSubject int64             `yaml:"maxMsgsPerSubject,omitempty"`
	Storage           string            `yaml:"storage"`
	Replicas          int               `yaml:"replicas"`
	DuplicateWindow   string            `yaml:"duplicateWindow,omitempty"`
	DenyPurge         bool              `yaml:"denyPurge,omitempty"`
	DenyDelete        bool              `yaml:"denyDelete,omitempty"`
	Metadata          map[string]string `yaml:"metadata,omitempty"`
	PreventDelete     bool              `yaml:"preventDelete,omitempty"`
}

// nackKeyValue is nack's KeyValue spec. nack creates the bucket the way
// `nats kv add` does, which sets allow_direct.
type nackKeyValue struct {
	Bucket   string `yaml:"bucket"`
	History  int    `yaml:"history"`
	Storage  string `yaml:"storage"`
	Replicas int    `yaml:"replicas"`
}

type nackSource struct {
	Name string `yaml:"name"`
}

type nackConsumer struct {
	StreamName    string `yaml:"streamName"`
	DurableName   string `yaml:"durableName"`
	DeliverPolicy string `yaml:"deliverPolicy"`
	AckPolicy     string `yaml:"ackPolicy"`
	AckWait       string `yaml:"ackWait"`
	MaxDeliver    int    `yaml:"maxDeliver"`
	MaxAckPending int    `yaml:"maxAckPending"`
	FilterSubject string `yaml:"filterSubject,omitempty"`
	PreventDelete bool   `yaml:"preventDelete,omitempty"`
}

// nackDuration renders d in the largest whole unit, as an operator would
// write it ("2h", not "2h0m0s"); nack parses it with time.ParseDuration.
func nackDuration(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	case d%time.Minute == 0:
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	default:
		return d.String()
	}
}

// natsManifestObjects is the topology as nack CRs: each partition, its
// durable, then the history, the dead-letter stream and the lease bucket.
func natsManifestObjects(o NATSManifestOptions) []nackObject {
	o = o.withDefaults()
	t := o.Topology
	lower := func(kind string) string { return t.Prefix + "-" + kind }
	var objs []nackObject
	var sources []nackSource
	for p := range t.Partitions {
		stream := t.streamName("INGEST_" + strconv.Itoa(p))
		name := lower("ingest-" + strconv.Itoa(p))
		sources = append(sources, nackSource{Name: stream})
		objs = append(objs, nackObject{
			APIVersion: "jetstream.nats.io/v1beta2", Kind: "Stream", Metadata: nackMetadata{Name: name},
			Spec: nackStream{
				Name:              stream,
				Subjects:          []string{natsIngestPartition(t.Prefix, p)},
				Retention:         "interest",
				Discard:           "new",
				DiscardPerSubject: true,
				MaxBytes:          o.PartitionMaxBytes,
				MaxMsgsPerSubject: o.MaxMsgsPerSubject,
				Storage:           "file",
				Replicas:          o.Replicas,
				DuplicateWindow:   nackDuration(max(2*time.Minute, t.minDuplicateWindow())),
				DenyPurge:         true,
				DenyDelete:        true,
				Metadata: map[string]string{
					"wavehouse.dev/partition":  strconv.Itoa(p),
					"wavehouse.dev/partitions": strconv.Itoa(t.Partitions),
				},
				PreventDelete: true,
			},
		}, nackObject{
			APIVersion: "jetstream.nats.io/v1beta2", Kind: "Consumer", Metadata: nackMetadata{Name: name},
			Spec: nackConsumer{
				StreamName:    stream,
				DurableName:   t.IngestConsumer,
				DeliverPolicy: "all",
				AckPolicy:     "explicit",
				AckWait:       nackDuration(t.AckWait),
				MaxDeliver:    -1,
				MaxAckPending: t.MaxAckPending,
				FilterSubject: natsIngestPartition(t.Prefix, p),
				PreventDelete: true,
			},
		})
	}
	objs = append(objs, nackObject{
		APIVersion: "jetstream.nats.io/v1beta2", Kind: "Stream", Metadata: nackMetadata{Name: lower("history")},
		Spec: nackStream{
			Name:      t.HistoryStream,
			Sources:   sources,
			Retention: "limits",
			Discard:   "old",
			MaxBytes:  o.HistoryMaxBytes,
			MaxAge:    nackDuration(o.HistoryMaxAge),
			Storage:   "file",
			Replicas:  o.Replicas,
		},
	}, nackObject{
		APIVersion: "jetstream.nats.io/v1beta2", Kind: "Stream", Metadata: nackMetadata{Name: lower("dlq")},
		Spec: nackStream{
			Name:              t.streamName("DLQ"),
			Subjects:          []string{natsDLQSubjects(t.Prefix)},
			Retention:         "limits",
			Discard:           "old",
			MaxBytes:          o.DLQMaxBytes,
			MaxMsgsPerSubject: o.DLQMaxMsgsPerSubject,
			Storage:           "file",
			Replicas:          o.Replicas,
		},
	}, nackObject{
		APIVersion: "jetstream.nats.io/v1beta2", Kind: "KeyValue", Metadata: nackMetadata{Name: lower("coord")},
		// No ttl: a lease expires on its candidates' clocks, not the server's.
		Spec: nackKeyValue{Bucket: t.coordBucket(), History: 1, Storage: "file", Replicas: o.Replicas},
	})
	return objs
}

// WriteNATSManifests writes the nack CRs for the topology WaveHouse checks at
// boot, as one multi-document YAML stream.
func WriteNATSManifests(w io.Writer, o NATSManifestOptions) error {
	o = o.withDefaults()
	t := o.Topology
	if err := t.validate(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, `# WaveHouse's JetStream topology as nack (jetstream.nats.io/v1beta2) resources:
# %d ingest partition(s) with interest retention, each with the %s durable,
# the %s history stream sourcing them, the dead-letter stream, and the
# %s KV bucket that coord.backend=nats holds its leases in.
# Generated by: wavehouse mq manifests --partitions %d --prefix %s --replicas %d
# Sizes (maxBytes, maxAge, maxMsgsPerSubject) are starting points to tune.
# Set the history's maxAge (%s) to at least the longest
# stream.gap_window_minutes among the tenants served.
# WaveHouse publishes nothing until all of it exists, so apply order is free;
# but never let a partition take publishes without its durable: with only the
# history's source on it, a row leaves the partition once the history has it.
`, t.Partitions, t.IngestConsumer, t.HistoryStream, t.coordBucket(), t.Partitions, t.Prefix, o.Replicas, nackDuration(o.HistoryMaxAge)); err != nil {
		return err
	}
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	for _, obj := range natsManifestObjects(o) {
		if err := enc.Encode(obj); err != nil {
			return err
		}
	}
	return enc.Close()
}

// natsPermissionSet is a NATS user's permission set, as the server config
// writes it.
type natsPermissionSet struct {
	PublishAllow   []string
	PublishDeny    []string
	SubscribeAllow []string
}

// natsInboxPrefix is the reply-subject prefix WaveHouse's connection uses, so
// its subscribe permission can be narrowed to its own replies.
func natsInboxPrefix(prefix string) string { return "_INBOX_" + prefix }

// natsPermissions is exactly what WaveHouse's NATS user needs under t:
// publish to its subjects, read stream and consumer state, pull from the
// ingest durable, create, pull from and delete the auto-expiring consumers it
// reads the history through, and read and write the lease keys in the coord
// bucket (a KV write is a publish to the key's subject; a read, a direct
// get). It cannot create, change, purge or delete a stream, nor create a
// durable on a partition.
func natsPermissions(t NATSTopology) natsPermissionSet {
	t = t.withDefaults()
	h, kv := t.HistoryStream, t.coordBucket()
	return natsPermissionSet{
		PublishAllow: []string{
			t.Prefix + ".ingest.>",
			t.Prefix + ".dlq.>",
			"$JS.API.INFO",
			"$JS.API.STREAM.NAMES",
			"$JS.API.STREAM.INFO.*",
			"$JS.API.CONSUMER.INFO.*.*",
			"$JS.API.CONSUMER.MSG.NEXT.*." + t.IngestConsumer,
			"$JS.ACK.>",
			"$JS.API.CONSUMER.CREATE." + h + ".>",
			"$JS.API.CONSUMER.MSG.NEXT." + h + ".>",
			"$JS.API.CONSUMER.DELETE." + h + ".>",
			"$KV." + kv + "." + leaseKeyPrefix + ">",
			"$JS.API.DIRECT.GET.KV_" + kv + ".$KV." + kv + "." + leaseKeyPrefix + ">",
		},
		PublishDeny: []string{
			"$JS.API.STREAM.CREATE.>",
			"$JS.API.STREAM.UPDATE.>",
			"$JS.API.STREAM.DELETE.>",
			"$JS.API.STREAM.PURGE.>",
			"$JS.API.CONSUMER.DURABLE.CREATE.>",
		},
		SubscribeAllow: []string{natsInboxPrefix(t.Prefix) + ".>"},
	}
}
