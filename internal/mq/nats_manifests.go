package mq

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
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
	// FileStore is each server's JetStream file store, max_file_store
	// (default 100 GiB, the shipped Helm values' PVC; the NATS chart sets
	// max_file_store to fileStore.maxSize, else the PVC's size). A server
	// reserves every stream replica's max_bytes it holds against it and
	// refuses a stream that does not fit (JetStream error 10047), so the
	// streams' max_bytes together must fit in it.
	FileStore int64
	// PartitionMaxBytes caps each ingest partition: the backlog of unwritten
	// rows its tenants share (default 15% of FileStore). It does not follow
	// the partition count, so lowering N (a larger partition while the
	// removed ones drain) never needs more store than before; at the defaults
	// four partitions, the history and the DLQ reserve three quarters of
	// FileStore, the quarter left being headroom for the Raft logs, the lease
	// bucket and the store's own overhead.
	PartitionMaxBytes int64
	// MaxMsgsPerSubject caps one topic's backlog in a partition (default
	// 1,000,000).
	MaxMsgsPerSubject int64
	// HistoryMaxAge is how long SSE can replay (default 15m, the seed's
	// stream.gap_window_minutes); at least the longest tenant gap window.
	HistoryMaxAge time.Duration
	// HistoryMaxBytes caps the history (default a tenth of FileStore).
	HistoryMaxBytes int64
	// DLQMaxBytes caps the dead-letter stream (default a twentieth of
	// FileStore).
	DLQMaxBytes int64
	// DLQMaxMsgsPerSubject caps one topic's parked rows (default 100,000).
	DLQMaxMsgsPerSubject int64
	// PinnedTTL is how long a shard's pin outlives its holder's last pull
	// (default 10s): at least minPinnedTTL, and under the membership lease.
	PinnedTTL time.Duration
}

// DefaultNATSFileStore is the file store the generated manifests are sized
// for by default: the shipped Helm values' JetStream PVC.
const DefaultNATSFileStore int64 = 100 << 30

// reserved is what the streams reserve on each server holding a replica of
// every one: their max_bytes together.
func (o NATSManifestOptions) reserved() int64 {
	return int64(o.Topology.Partitions)*o.PartitionMaxBytes + o.HistoryMaxBytes + o.DLQMaxBytes
}

// fits reports streams that together reserve more than the file store, which
// the server would refuse one of.
func (o NATSManifestOptions) fits() error {
	if o.FileStore < 0 || o.PartitionMaxBytes <= 0 || o.HistoryMaxBytes <= 0 || o.DLQMaxBytes <= 0 {
		return errors.New("the file store and every stream's max bytes must be positive")
	}
	if r := o.reserved(); r > o.FileStore {
		return fmt.Errorf("the streams reserve %s (%d partitions of %s, history %s, dlq %s), more than the %s file store, and JetStream refuses a stream that does not fit: raise --file-store with the servers' volumes, or lower --partition-max-bytes",
			FormatStoreSize(r), o.Topology.Partitions, FormatStoreSize(o.PartitionMaxBytes), FormatStoreSize(o.HistoryMaxBytes), FormatStoreSize(o.DLQMaxBytes), FormatStoreSize(o.FileStore))
	}
	return nil
}

// storeUnits are the Kubernetes quantity suffixes ParseStoreSize takes, as
// the NATS config parser reads them too.
var storeUnits = []struct {
	suffix string
	n      int64
}{
	{"Ti", 1 << 40},
	{"Gi", 1 << 30},
	{"Mi", 1 << 20},
	{"Ki", 1 << 10},
	{"T", 1e12},
	{"G", 1e9},
	{"M", 1e6},
	{"k", 1e3},
}

// ParseStoreSize reads a byte count written as a Kubernetes quantity: a
// whole number with an optional k, M, G, T or Ki, Mi, Gi, Ti suffix (100Gi).
func ParseStoreSize(s string) (int64, error) {
	num, unit := s, int64(1)
	for _, u := range storeUnits {
		if strings.HasSuffix(s, u.suffix) {
			num, unit = strings.TrimSuffix(s, u.suffix), u.n
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 || n > (1<<62)/unit {
		return 0, fmt.Errorf("size %q must be a whole number of bytes, optionally with a k, M, G, T, Ki, Mi, Gi or Ti suffix", s)
	}
	return n * unit, nil
}

// FormatStoreSize writes n in the largest binary unit that divides it.
func FormatStoreSize(n int64) string {
	for _, u := range storeUnits[:4] {
		if n != 0 && n%u.n == 0 {
			return strconv.FormatInt(n/u.n, 10) + u.suffix
		}
	}
	return strconv.FormatInt(n, 10)
}

func (o NATSManifestOptions) withDefaults() NATSManifestOptions {
	o.Topology = o.Topology.withDefaults()
	if o.Replicas == 0 {
		o.Replicas = 3
	}
	if o.MaxMsgsPerSubject == 0 {
		o.MaxMsgsPerSubject = 1_000_000
	}
	if o.HistoryMaxAge == 0 {
		o.HistoryMaxAge = 15 * time.Minute
	}
	if o.FileStore == 0 {
		o.FileStore = DefaultNATSFileStore
	}
	if o.HistoryMaxBytes == 0 {
		o.HistoryMaxBytes = o.FileStore / 10
	}
	if o.DLQMaxBytes == 0 {
		o.DLQMaxBytes = o.FileStore / 20
	}
	if o.PartitionMaxBytes == 0 {
		o.PartitionMaxBytes = o.FileStore / 100 * 15
	}
	if o.DLQMaxMsgsPerSubject == 0 {
		o.DLQMaxMsgsPerSubject = 100_000
	}
	if o.PinnedTTL == 0 {
		o.PinnedTTL = 10 * time.Second
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
	Republish         *nackRepublish    `yaml:"republish,omitempty"`
}

// nackRepublish is nack's stream republish: every row the stream stores is
// also published, by the server, to the destination.
type nackRepublish struct {
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
}

// nackKeyValue is nack's KeyValue spec. nack creates the bucket the way
// `nats kv add` does, which sets allow_direct.
type nackKeyValue struct {
	Bucket   string `yaml:"bucket"`
	History  int    `yaml:"history"`
	Storage  string `yaml:"storage"`
	Replicas int    `yaml:"replicas"`
}

type nackConsumer struct {
	StreamName     string   `yaml:"streamName"`
	DurableName    string   `yaml:"durableName"`
	DeliverPolicy  string   `yaml:"deliverPolicy"`
	AckPolicy      string   `yaml:"ackPolicy"`
	AckWait        string   `yaml:"ackWait"`
	MaxDeliver     int      `yaml:"maxDeliver"`
	MaxAckPending  int      `yaml:"maxAckPending"`
	FilterSubject  string   `yaml:"filterSubject,omitempty"`
	PriorityPolicy string   `yaml:"priorityPolicy,omitempty"`
	PriorityGroups []string `yaml:"priorityGroups,omitempty"`
	PinnedTTL      string   `yaml:"pinnedTtl,omitempty"`
	PreventDelete  bool     `yaml:"preventDelete,omitempty"`
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

// natsManifestObjects is the topology as nack CRs: each partition and its
// shard durables, then the history, the dead-letter stream and the lease
// bucket.
func natsManifestObjects(o NATSManifestOptions) []nackObject {
	o = o.withDefaults()
	t := o.Topology
	lower := func(kind string) string { return t.Prefix + "-" + kind }
	var objs []nackObject
	for p := range t.Partitions {
		stream := t.streamName("INGEST_" + strconv.Itoa(p))
		name := lower("ingest-" + strconv.Itoa(p))
		rp := natsRepublish(t.Prefix, p)
		objs = append(objs, nackObject{
			APIVersion: "jetstream.nats.io/v1beta2", Kind: "Stream", Metadata: nackMetadata{Name: name},
			Spec: nackStream{
				Name:              stream,
				Subjects:          []string{natsIngestPartition(t.Prefix, p)},
				Retention:         "workqueue",
				Discard:           "new",
				DiscardPerSubject: true,
				MaxBytes:          o.PartitionMaxBytes,
				MaxMsgsPerSubject: o.MaxMsgsPerSubject,
				Storage:           "file",
				Replicas:          o.Replicas,
				DuplicateWindow:   nackDuration(max(2*time.Minute, t.minDuplicateWindow(), t.dedupeDuplicateWindow())),
				DenyPurge:         true,
				DenyDelete:        true,
				Metadata: map[string]string{
					"wavehouse.dev/partition":  strconv.Itoa(p),
					"wavehouse.dev/partitions": strconv.Itoa(t.Partitions),
					"wavehouse.dev/shards":     strconv.Itoa(t.Shards),
				},
				PreventDelete: true,
				Republish:     &nackRepublish{Source: rp.Source, Destination: rp.Destination},
			},
		})
		for sh := range t.Shards {
			objs = append(objs, nackObject{
				APIVersion: "jetstream.nats.io/v1beta2", Kind: "Consumer",
				Metadata: nackMetadata{Name: name + "-" + strconv.Itoa(sh)},
				Spec: nackConsumer{
					StreamName:     stream,
					DurableName:    natsShardDurable(t.IngestConsumer, sh),
					DeliverPolicy:  "all",
					AckPolicy:      "explicit",
					AckWait:        nackDuration(t.AckWait),
					MaxDeliver:     -1,
					MaxAckPending:  t.MaxAckPending,
					FilterSubject:  natsShardFilter(t.Prefix, p, sh),
					PriorityPolicy: "pinned_client",
					PriorityGroups: []string{natsPriorityGroup},
					PinnedTTL:      nackDuration(o.PinnedTTL),
					PreventDelete:  true,
				},
			})
		}
	}
	objs = append(objs, nackObject{
		APIVersion: "jetstream.nats.io/v1beta2", Kind: "Stream", Metadata: nackMetadata{Name: lower("history")},
		Spec: nackStream{
			Name:      t.HistoryStream,
			Subjects:  []string{natsHistorySubjects(t.Prefix)},
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

// manifestFlags is the `wavehouse mq manifests` command line that renders o:
// the flags that always shape it, then any other set to other than its
// default.
func manifestFlags(o NATSManifestOptions) string {
	t := o.Topology
	flags := fmt.Sprintf("--partitions %d --shards %d --prefix %s --replicas %d --file-store %s", t.Partitions, t.Shards, t.Prefix, o.Replicas, FormatStoreSize(o.FileStore))
	if o.PartitionMaxBytes != o.FileStore/100*15 {
		flags += " --partition-max-bytes " + FormatStoreSize(o.PartitionMaxBytes)
	}
	if t.IngestConsumer != DefaultNATSIngestConsumer {
		flags += " --ingest-consumer " + t.IngestConsumer
	}
	if t.HistoryStream != t.streamName("HISTORY") {
		flags += " --history-stream " + t.HistoryStream
	}
	if t.PublishTimeout != DefaultNATSPublishTimeout {
		flags += " --publish-timeout " + t.PublishTimeout.String()
	}
	if t.CoordBucket != "" && t.CoordBucket != DefaultNATSCoordBucket(t.Prefix) {
		flags += " --coord-bucket " + t.CoordBucket
	}
	return flags
}

// WriteNATSManifests writes the nack CRs for the topology WaveHouse checks at
// boot, as one multi-document YAML stream.
func WriteNATSManifests(w io.Writer, o NATSManifestOptions) error {
	o = o.withDefaults()
	t := o.Topology
	if err := t.validate(); err != nil {
		return err
	}
	if o.PinnedTTL < minPinnedTTL {
		return fmt.Errorf("pinned ttl %s must be at least %s, twice the pull expiry", o.PinnedTTL, minPinnedTTL)
	}
	if err := o.fits(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, `# WaveHouse's JetStream topology as nack (jetstream.nats.io/v1beta2) resources:
# %d ingest partition(s) with work-queue retention, each split into %d shards
# with a pinned-client durable per shard (%s-<shard>), and republishing every
# row to the %s history stream; the dead-letter stream; and the %s KV
# bucket that coord.backend=nats holds its leases in.
# Generated by: wavehouse mq manifests %s
# Sized for a JetStream file store (max_file_store) of %s on every server:
# the streams' maxBytes reserve %s of it, and JetStream refuses a stream that
# does not fit. The NATS Helm chart sets max_file_store to
# config.jetstream.fileStore.maxSize, else to the JetStream PVC's size.
# Sizes (maxBytes, maxAge, maxMsgsPerSubject) are starting points to tune.
# Set the history's maxAge (%s) to at least the longest
# stream.gap_window_minutes among the tenants served.
# WaveHouse publishes nothing until all of it exists, so apply order is free:
# a partition keeps every row until the ingest worker acks it, even while its
# durable is missing, and the history is best effort (a row republished while
# it is unavailable reaches ClickHouse but not SSE replay).
`, t.Partitions, t.Shards, t.IngestConsumer, t.HistoryStream, t.coordBucket(), manifestFlags(o), FormatStoreSize(o.FileStore), FormatStoreSize(o.reserved()), nackDuration(o.HistoryMaxAge)); err != nil {
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

// natsAPIPrefix is the JetStream API prefix a client of domain uses:
// $JS.API., or $JS.<domain>.API. under a JetStream domain.
func natsAPIPrefix(domain string) string {
	if domain == "" {
		return "$JS.API."
	}
	return "$JS." + domain + ".API."
}

// natsPermissions is exactly what WaveHouse's NATS user needs under t:
// publish to its ingest and dead-letter subjects (never the history's), read
// stream and consumer state and list consumer names, pull from, unpin and
// reset each shard durable and ack what it delivers, create, pull from and
// delete the auto-expiring consumers it reads the history through, and read
// and write the lease keys in the coord bucket (a KV write is a publish to
// the key's subject; a read, a direct get). It cannot create, change, purge
// or delete a stream, nor create a durable on a partition.
//
// Under a JetStream domain the client sends every API request, and each KV
// write, under $JS.<domain>.API. A server in the domain maps that to the
// plain subject before it checks permissions, and a server outside it (a
// leafnode) checks it as sent, so each is allowed and denied in both forms.
func natsPermissions(t NATSTopology, domain string) natsPermissionSet {
	t = t.withDefaults()
	h, kv := t.HistoryStream, t.coordBucket()
	api := func(subjects ...string) []string {
		var out []string
		for _, s := range subjects {
			out = append(out, "$JS.API."+s)
			if domain != "" {
				out = append(out, natsAPIPrefix(domain)+s)
			}
		}
		return out
	}
	allow := []string{t.Prefix + ".ingest.>", t.Prefix + ".dlq.>"}
	allow = append(allow, api("INFO", "STREAM.NAMES", "STREAM.INFO.*", "CONSUMER.INFO.*.*", "CONSUMER.NAMES.*")...)
	// One entry per shard durable: a NATS wildcard is a whole token, so
	// wh-ingest-* would name no durable at all. The stream is a wildcard, so
	// a partition a lower N left behind is covered too.
	for _, verb := range []string{"MSG.NEXT", "UNPIN", "RESET"} {
		for s := range t.Shards {
			allow = append(allow, api("CONSUMER."+verb+".*."+natsShardDurable(t.IngestConsumer, s))...)
		}
	}
	// An ack goes to the reply subject the server delivered the row with:
	// $JS.ACK.<stream>.<consumer>.… (v1, the server's default), or
	// $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.… (v2, the
	// js_ack_fc_v2 feature flag). The history consumers ack nothing.
	for s := range t.Shards {
		allow = append(allow, "$JS.ACK.*."+natsShardDurable(t.IngestConsumer, s)+".>")
	}
	for s := range t.Shards {
		allow = append(allow, "$JS.ACK.*.*.*."+natsShardDurable(t.IngestConsumer, s)+".>")
	}
	allow = append(allow, api("CONSUMER.CREATE."+h+".>", "CONSUMER.MSG.NEXT."+h+".>", "CONSUMER.DELETE."+h+".>")...)
	lease := "$KV." + kv + "." + leaseKeyPrefix + ">"
	allow = append(allow, lease)
	if domain != "" {
		allow = append(allow, natsAPIPrefix(domain)+lease)
	}
	allow = append(allow, api("DIRECT.GET.KV_"+kv+"."+lease)...)
	return natsPermissionSet{
		PublishAllow: allow,
		// Only the partitions' republish writes the history.
		PublishDeny: append([]string{natsHistorySubjects(t.Prefix)},
			api("STREAM.CREATE.>", "STREAM.UPDATE.>", "STREAM.DELETE.>", "STREAM.PURGE.>", "CONSUMER.DURABLE.CREATE.>")...),
		SubscribeAllow: []string{natsInboxPrefix(t.Prefix) + ".>"},
	}
}

// WriteNATSPermissions writes the wavehouse user's permissions under t, for
// a connection to JetStream domain jsDomain ("" for none, mq.nats.js_domain),
// as the permissions block of a NATS Helm values file's user entry.
func WriteNATSPermissions(w io.Writer, t NATSTopology, jsDomain string) error {
	t = t.withDefaults()
	if err := t.validate(); err != nil {
		return err
	}
	if jsDomain != "" && !natsDomainName.MatchString(jsDomain) {
		return fmt.Errorf("js domain %q must be one token of [a-zA-Z0-9_-]", jsDomain)
	}
	p := natsPermissions(t, jsDomain)
	doc := map[string]any{"permissions": map[string]any{
		"publish":   map[string][]string{"allow": p.PublishAllow, "deny": p.PublishDeny},
		"subscribe": map[string][]string{"allow": p.SubscribeAllow},
	}}
	domain := ""
	if jsDomain != "" {
		domain = ", JetStream domain " + jsDomain
	}
	if _, err := fmt.Fprintf(w, "# The wavehouse user's permissions for prefix %s, %d shard(s), history stream %s\n# and lease bucket %s%s. Generated by: wavehouse mq permissions\n", t.Prefix, t.Shards, t.HistoryStream, t.coordBucket(), domain); err != nil {
		return err
	}
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return err
	}
	return enc.Close()
}

// natsDomainName is the grammar WriteNATSPermissions takes for a JetStream
// domain: one subject token.
var natsDomainName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
