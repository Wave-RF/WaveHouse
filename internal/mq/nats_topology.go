package mq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// NATSTopology is what WaveHouse needs of an operator-owned JetStream: N
// ingest partition streams with interest retention, each with a durable pull
// consumer; a history stream with limits retention that sources every
// partition, for SSE replay and the live hub; and one dead-letter stream. The
// operator creates all of it (WriteNATSManifests renders it as nack CRs);
// WaveHouse only checks it (verifyNATSTopology) and never repairs it.
type NATSTopology struct {
	// Prefix leads every subject: <prefix>.ingest.<p>.… and <prefix>.dlq.….
	Prefix string
	// Partitions is N, the number of ingest partition streams.
	Partitions int
	// IngestConsumer is the durable on every partition the ingest worker
	// consumes.
	IngestConsumer string
	// HistoryStream names the history stream. It has no subjects of its own,
	// so unlike the partitions and the dead-letter stream it cannot be found
	// by subject.
	HistoryStream string
	// PublishTimeout bounds one publish attempt; a partition's duplicate
	// window must cover every attempt (minDuplicateWindow), so a retried
	// publish is not stored twice.
	PublishTimeout time.Duration
	// AckWait, MaxAckPending and Prefetch are what the ingest worker asks of
	// the durable (internal/ingest/worker.go, which imports this package).
	AckWait       time.Duration
	MaxAckPending int
	Prefetch      int
}

// Defaults for a NATSTopology's zero fields.
const (
	DefaultNATSSubjectPrefix  = "wh"
	DefaultNATSPartitions     = 1
	DefaultNATSIngestConsumer = "wh-ingest"
	defaultNATSPublishTimeout = 5 * time.Second
	defaultNATSAckWait        = 60 * time.Second
	defaultNATSMaxAckPending  = 10_000
	defaultNATSPrefetch       = 500
)

// withDefaults fills t's zero fields.
func (t NATSTopology) withDefaults() NATSTopology {
	if t.Prefix == "" {
		t.Prefix = DefaultNATSSubjectPrefix
	}
	if t.Partitions == 0 {
		t.Partitions = DefaultNATSPartitions
	}
	if t.IngestConsumer == "" {
		t.IngestConsumer = DefaultNATSIngestConsumer
	}
	if t.HistoryStream == "" {
		t.HistoryStream = t.streamName("HISTORY")
	}
	if t.PublishTimeout == 0 {
		t.PublishTimeout = defaultNATSPublishTimeout
	}
	if t.AckWait == 0 {
		t.AckWait = defaultNATSAckWait
	}
	if t.MaxAckPending == 0 {
		t.MaxAckPending = defaultNATSMaxAckPending
	}
	if t.Prefetch == 0 {
		t.Prefetch = defaultNATSPrefetch
	}
	return t
}

// validate reports a topology no deployment could satisfy.
func (t NATSTopology) validate() error {
	if err := validSubjectPrefix(t.Prefix); err != nil {
		return err
	}
	if t.Partitions < 1 {
		return fmt.Errorf("partitions must be at least 1, got %d", t.Partitions)
	}
	return nil
}

// streamName is the name the generated manifests give a stream of kind. Only
// the history's is binding; the others are found by subject.
func (t NATSTopology) streamName(kind string) string {
	return strings.ToUpper(t.Prefix) + "_" + kind
}

// minDuplicateWindow is the shortest duplicate window that stores a publish
// once however many of its attempts were stored: ExternalNATS sends the last
// retry this long after the first attempt.
func (t NATSTopology) minDuplicateWindow() time.Duration {
	return (publishRetries+1)*t.PublishTimeout + publishRetries*publishRetryWait
}

// partitionShare is the worker's prefetch share of one partition, at least one.
func (t NATSTopology) partitionShare() int {
	return max(1, t.Prefetch/t.Partitions)
}

// FindingSeverity says whether a finding stops WaveHouse from serving.
type FindingSeverity int

const (
	// FindingRequired refuses boot.
	FindingRequired FindingSeverity = iota
	// FindingRecommended is logged as a warning.
	FindingRecommended
)

func (s FindingSeverity) String() string {
	if s == FindingRequired {
		return "required"
	}
	return "recommended"
}

// Finding is one way the operator's topology differs from what WaveHouse
// needs.
type Finding struct {
	Severity FindingSeverity
	// Object is what the finding is about, e.g. "stream WH_INGEST_0".
	Object string
	// Field is the setting, e.g. "retention", in the stream or consumer
	// config's own (JSON) names.
	Field string
	// Problem says what is wrong and what is needed.
	Problem string
	// transient marks a finding that clears on its own, such as a history
	// source re-attaching after a NATS restart (~10s): boot waits it out, and
	// the periodic re-check reports it on its own gauge rather than as a
	// topology fault.
	transient bool
}

func (f Finding) String() string {
	return fmt.Sprintf("%s: %s: %s: %s", f.Severity, f.Object, f.Field, f.Problem)
}

// ErrTopology is what a TopologyError matches.
var ErrTopology = errors.New("nats topology does not match what WaveHouse needs")

// TopologyError lists every finding from the last check, required ones first.
type TopologyError struct {
	Findings []Finding
}

func (e *TopologyError) Error() string {
	var b strings.Builder
	b.WriteString(ErrTopology.Error())
	for _, f := range e.Findings {
		b.WriteString("\n  - ")
		b.WriteString(f.String())
	}
	return b.String()
}

func (e *TopologyError) Unwrap() error { return ErrTopology }

// hasRequired reports whether any finding refuses boot.
func hasRequired(findings []Finding) bool {
	return slices.ContainsFunc(findings, func(f Finding) bool { return f.Severity == FindingRequired })
}

// minNATSServer is the oldest server whose features the topology relies on:
// stream metadata, subject-filtered stream info and discard_new_per_subject.
var minNATSServer = [3]int{2, 10, 0}

// recommendedNATSMinor is the server line the embedded broker runs.
const recommendedNATSMinor = "2.14."

// topologyVerifier accumulates the findings of one check.
type topologyVerifier struct {
	js       jetstream.JetStream
	t        NATSTopology
	findings []Finding
}

func (v *topologyVerifier) add(sev FindingSeverity, object, field, format string, args ...any) {
	v.findings = append(v.findings, Finding{Severity: sev, Object: object, Field: field, Problem: fmt.Sprintf(format, args...)})
}

// verifyNATSTopology checks the operator's JetStream against t and returns
// every finding at once. The error is for a check that could not run (the
// server unreachable, a request refused); a missing stream or consumer is a
// finding.
func verifyNATSTopology(ctx context.Context, js jetstream.JetStream, t NATSTopology) ([]Finding, error) {
	t = t.withDefaults()
	if err := t.validate(); err != nil {
		return nil, err
	}
	v := &topologyVerifier{js: js, t: t}
	v.serverVersion(js.Conn().ConnectedServerVersion())

	partitions := make([]string, t.Partitions)
	for p := range t.Partitions {
		name, err := v.partition(ctx, p)
		if err != nil {
			return nil, err
		}
		if q := slices.Index(partitions[:p], name); name != "" && q >= 0 {
			v.add(FindingRequired, "stream "+name, "subjects", "holds partitions %d and %d; each partition needs a stream of its own", q, p)
		}
		partitions[p] = name
	}
	if err := v.extraPartitions(ctx, partitions); err != nil {
		return nil, err
	}
	if err := v.history(ctx, partitions); err != nil {
		return nil, err
	}
	if err := v.dlq(ctx); err != nil {
		return nil, err
	}
	slices.SortStableFunc(v.findings, func(a, b Finding) int { return int(a.Severity) - int(b.Severity) })
	return v.findings, nil
}

// awaitNATSTopology repeats the check until nothing required is missing or
// wait runs out — on Kubernetes the operator's CRs roll out with the pods —
// and then returns the warnings, or a *TopologyError with every finding of
// the last check. A check that could not run is retried the same way.
func awaitNATSTopology(ctx context.Context, js jetstream.JetStream, t NATSTopology, wait time.Duration) ([]Finding, error) {
	deadline := time.Now().Add(wait)
	backoff := 250 * time.Millisecond
	for {
		findings, err := verifyNATSTopology(ctx, js, t)
		if err == nil && !hasRequired(findings) {
			return findings, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if err != nil {
				return nil, fmt.Errorf("check nats topology: %w", err)
			}
			return nil, &TopologyError{Findings: findings}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(backoff, remaining)):
		}
		backoff = min(2*backoff, 5*time.Second)
	}
}

func (v *topologyVerifier) serverVersion(version string) {
	const object, field = "server", "version"
	var got [3]int
	parts := strings.SplitN(strings.TrimPrefix(version, "v"), ".", 3)
	parsed := len(parts) == 3
	for i := 0; parsed && i < 3; i++ {
		// Only the leading digits: a pre-release suffix rides on the patch.
		end := strings.IndexFunc(parts[i], func(r rune) bool { return r < '0' || r > '9' })
		if end < 0 {
			end = len(parts[i])
		}
		n, err := strconv.Atoi(parts[i][:end])
		parsed = err == nil
		got[i] = n
	}
	switch {
	case !parsed:
		v.add(FindingRequired, object, field, "cannot read server version %q; need at least %d.%d.%d", version, minNATSServer[0], minNATSServer[1], minNATSServer[2])
	case slices.Compare(got[:], minNATSServer[:]) < 0:
		v.add(FindingRequired, object, field, "is %s; need at least %d.%d.%d", version, minNATSServer[0], minNATSServer[1], minNATSServer[2])
	case !strings.HasPrefix(strings.TrimPrefix(version, "v"), recommendedNATSMinor):
		v.add(FindingRecommended, object, field, "is %s; WaveHouse is tested against %sx", version, recommendedNATSMinor)
	}
}

// streamsHolding lists the streams whose subjects match subject.
func (v *topologyVerifier) streamsHolding(ctx context.Context, subject string) ([]string, error) {
	lister := v.js.StreamNames(ctx, jetstream.WithStreamListSubject(subject))
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("list streams holding %s: %w", subject, err)
	}
	slices.Sort(names)
	return names, nil
}

// findBySubject finds the one stream holding probe, or adds a finding and
// returns nil.
func (v *topologyVerifier) findBySubject(ctx context.Context, object, probe string) (jetstream.Stream, error) {
	names, err := v.streamsHolding(ctx, probe)
	if err != nil {
		return nil, err
	}
	switch len(names) {
	case 0:
		v.add(FindingRequired, object, "subjects", "no stream holds %s", probe)
		return nil, nil
	case 1:
	default:
		v.add(FindingRequired, object, "subjects", "streams %s all hold %s; exactly one may", strings.Join(names, ", "), probe)
		return nil, nil
	}
	return v.stream(ctx, object, names[0])
}

// stream looks a stream up by name, adding a finding when it does not exist.
func (v *topologyVerifier) stream(ctx context.Context, object, name string) (jetstream.Stream, error) {
	s, err := v.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		v.add(FindingRequired, object, "name", "stream %s does not exist", name)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", name, err)
	}
	return s, nil
}

// partition checks ingest partition p and its durable, returning the stream's
// name ("" when there is none to check).
func (v *topologyVerifier) partition(ctx context.Context, p int) (string, error) {
	t := v.t
	filter := natsIngestPartition(t.Prefix, p)
	s, err := v.findBySubject(ctx, fmt.Sprintf("ingest partition %d", p), fmt.Sprintf("%s.ingest.%d.x", t.Prefix, p))
	if s == nil || err != nil {
		return "", err
	}
	cfg := s.CachedInfo().Config
	obj := "stream " + cfg.Name
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }
	rec := func(field, format string, args ...any) { v.add(FindingRecommended, obj, field, format, args...) }

	if !slices.Contains(cfg.Subjects, filter) {
		req("subjects", "are %q; must include %q", cfg.Subjects, filter)
	}
	if cfg.Retention != jetstream.InterestPolicy {
		req("retention", "is %s; must be interest, so a row is deleted once it is written", cfg.Retention)
	}
	if cfg.Discard != jetstream.DiscardNew {
		req("discard", "is %s; must be new, so a full partition refuses rather than dropping unwritten rows", cfg.Discard)
	}
	if cfg.MaxBytes <= 0 {
		req("max_bytes", "is unlimited; must be set, to bound the disk and signal backpressure")
	}
	if cfg.MaxAge != 0 {
		req("max_age", "is %s; must be unset, since an age limit drops unwritten rows", cfg.MaxAge)
	}
	if cfg.Storage != jetstream.FileStorage {
		req("storage", "is %s; must be file", cfg.Storage)
	}
	if cfg.Duplicates < t.minDuplicateWindow() {
		req("duplicate_window", "is %s; must be at least %s (every attempt of a retried publish), so it is stored once", cfg.Duplicates, t.minDuplicateWindow())
	}
	if cfg.NoAck {
		req("no_ack", "is set; publishes must be acknowledged")
	}
	if cfg.Sealed {
		req("sealed", "is set; the partition must take publishes")
	}
	if cfg.Mirror != nil {
		req("mirror", "is set; a partition must not be a mirror")
	}
	switch {
	case cfg.MaxMsgsPerSubject > 0 && !cfg.DiscardNewPerSubject:
		// Without it the server keeps the cap by evicting the topic's oldest rows.
		req("discard_new_per_subject", "is unset while max_msgs_per_subject is %d; must be set, or a topic at its cap loses its oldest unwritten rows", cfg.MaxMsgsPerSubject)
	case cfg.MaxMsgsPerSubject <= 0:
		rec("max_msgs_per_subject", "set it with discard_new_per_subject, so one topic cannot fill the partition for every tenant in it")
	}
	if !cfg.DenyPurge || !cfg.DenyDelete {
		rec("deny_purge", "set deny_purge and deny_delete; nothing should remove unwritten rows")
	}
	if cfg.Replicas < 3 {
		rec("num_replicas", "is %d; 3 survives losing a server", cfg.Replicas)
	}
	gotP, hasP := cfg.Metadata["wavehouse.dev/partition"]
	gotN, hasN := cfg.Metadata["wavehouse.dev/partitions"]
	switch {
	case !hasP || !hasN:
		rec("metadata", "set wavehouse.dev/partition and wavehouse.dev/partitions, so a partition count mismatch is caught by name")
	case gotP != strconv.Itoa(p) || gotN != strconv.Itoa(t.Partitions):
		req("metadata", "says partition %s of %s; WaveHouse is configured for partition %d of %d (mq.nats.partitions must match the operator's)", gotP, gotN, p, t.Partitions)
	}

	return cfg.Name, v.durable(ctx, s, filter)
}

func (v *topologyVerifier) durable(ctx context.Context, s jetstream.Stream, filter string) error {
	t := v.t
	stream := s.CachedInfo().Config.Name
	obj := "consumer " + stream + "/" + t.IngestConsumer
	c, err := s.Consumer(ctx, t.IngestConsumer)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		v.add(FindingRequired, obj, "durable_name", "does not exist")
		return nil
	}
	if errors.Is(err, jetstream.ErrNotPullConsumer) {
		v.add(FindingRequired, obj, "deliver_subject", "is set; must be a pull consumer")
		return nil
	}
	if err != nil {
		return fmt.Errorf("consumer %s/%s: %w", stream, t.IngestConsumer, err)
	}
	cfg := c.CachedInfo().Config
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }

	if cfg.AckPolicy != jetstream.AckExplicitPolicy {
		req("ack_policy", "is %s; must be explicit", cfg.AckPolicy)
	}
	if cfg.AckWait < t.AckWait {
		req("ack_wait", "is %s; must be at least %s, the ingest worker's", cfg.AckWait, t.AckWait)
	}
	if cfg.MaxDeliver != -1 {
		req("max_deliver", "is %d; must be -1, or a row that is never written stays in the partition undelivered", cfg.MaxDeliver)
	}
	switch {
	case cfg.MaxAckPending <= 0:
		req("max_ack_pending", "is %d; must be set", cfg.MaxAckPending)
	case cfg.MaxAckPending < t.MaxAckPending:
		v.add(FindingRecommended, obj, "max_ack_pending", "is %d; the ingest worker expects %d", cfg.MaxAckPending, t.MaxAckPending)
	}
	if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		req("deliver_policy", "is %s; must be all", cfg.DeliverPolicy)
	}
	filters := cfg.FilterSubjects
	if cfg.FilterSubject != "" {
		filters = append(filters, cfg.FilterSubject)
	}
	if len(filters) > 0 && !slices.Equal(filters, []string{filter}) {
		req("filter_subject", "is %q; must be empty or %q", filters, filter)
	}
	if cfg.HeadersOnly {
		req("headers_only", "is set; the worker needs the bodies, and acking an empty one deletes the row")
	}
	if cfg.ReplayPolicy != jetstream.ReplayInstantPolicy {
		req("replay_policy", "is %s; must be instant, or a backlog drains at the rate it arrived", cfg.ReplayPolicy)
	}
	if cfg.InactiveThreshold != 0 {
		req("inactive_threshold", "is %s; a durable must not expire", cfg.InactiveThreshold)
	}
	if cfg.MaxRequestBatch != 0 && cfg.MaxRequestBatch < t.partitionShare() {
		req("max_request_batch", "is %d; must be 0 or at least %d, the worker's prefetch per partition", cfg.MaxRequestBatch, t.partitionShare())
	}
	if cfg.PriorityPolicy != jetstream.PriorityPolicyNone {
		req("priority_policy", "must be none; WaveHouse assigns partitions to workers itself")
	}
	return nil
}

// extraPartitions warns about streams holding ingest subjects beyond the N
// partitions — left over from a smaller or larger N, and drained until the
// operator deletes them.
func (v *topologyVerifier) extraPartitions(ctx context.Context, partitions []string) error {
	names, err := v.streamsHolding(ctx, v.t.Prefix+".ingest.>")
	if err != nil {
		return err
	}
	for _, name := range names {
		if !slices.Contains(partitions, name) {
			v.add(FindingRecommended, "stream "+name, "subjects",
				"holds %s.ingest subjects outside partitions 0-%d; delete it once it is empty if the partition count changed", v.t.Prefix, v.t.Partitions-1)
		}
	}
	return nil
}

func (v *topologyVerifier) history(ctx context.Context, partitions []string) error {
	t := v.t
	obj := "stream " + t.HistoryStream
	s, err := v.stream(ctx, obj, t.HistoryStream)
	if s == nil || err != nil {
		return err
	}
	info := s.CachedInfo()
	cfg := info.Config
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }

	if len(cfg.Subjects) > 0 {
		req("subjects", "are %q; the history must have none, only sources", cfg.Subjects)
	}
	for p, name := range partitions {
		if name == "" {
			continue // the partition's own finding says why
		}
		i := slices.IndexFunc(cfg.Sources, func(src *jetstream.StreamSource) bool { return src.Name == name })
		if i < 0 {
			req("sources", "do not include %s (partition %d)", name, p)
			continue
		}
		src := cfg.Sources[i]
		if filter := natsIngestPartition(t.Prefix, p); src.FilterSubject != "" && src.FilterSubject != filter {
			req("sources", "filter %s by %q; must be unfiltered or %q", name, src.FilterSubject, filter)
		}
		if len(src.SubjectTransforms) > 0 {
			req("sources", "transform %s's subjects; they must arrive unchanged", name)
		}
		if src.External != nil {
			req("sources", "take %s from another domain or account; it must be local", name)
		}
		// A row wh-ingest acks before the source attaches never reaches the
		// history; the server reports active -1 until then.
		j := slices.IndexFunc(info.Sources, func(si *jetstream.StreamSourceInfo) bool { return si.Name == name })
		if j < 0 || info.Sources[j].Active < 0 {
			req("sources", "%s is not attached yet", name)
			v.findings[len(v.findings)-1].transient = true
		}
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		req("retention", "is %s; must be limits", cfg.Retention)
	}
	// discard: new would stall the source when the history is full, and the
	// source holds every partition's rows until it has copied them.
	if cfg.Discard != jetstream.DiscardOld {
		req("discard", "is %s; must be old, or a full history holds every partition's rows", cfg.Discard)
	}
	if cfg.MaxAge <= 0 {
		req("max_age", "is unlimited; must be set, at least the longest gap window a tenant replays")
	}
	if cfg.MaxBytes <= 0 {
		v.add(FindingRecommended, obj, "max_bytes", "is unlimited; set it to bound the disk")
	}
	return nil
}

func (v *topologyVerifier) dlq(ctx context.Context) error {
	s, err := v.findBySubject(ctx, "dead-letter stream", v.t.Prefix+".dlq.x")
	if s == nil || err != nil {
		return err
	}
	cfg := s.CachedInfo().Config
	obj := "stream " + cfg.Name
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }

	if filter := natsDLQSubjects(v.t.Prefix); !slices.Contains(cfg.Subjects, filter) {
		req("subjects", "are %q; must include %q", cfg.Subjects, filter)
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		req("retention", "is %s; must be limits", cfg.Retention)
	}
	if cfg.Discard != jetstream.DiscardOld {
		req("discard", "is %s; must be old", cfg.Discard)
	}
	if cfg.Storage != jetstream.FileStorage {
		req("storage", "is %s; must be file", cfg.Storage)
	}
	if cfg.MaxBytes <= 0 {
		req("max_bytes", "is unlimited; must be set")
	}
	if cfg.MaxMsgsPerSubject <= 0 {
		v.add(FindingRecommended, obj, "max_msgs_per_subject", "set it, so one topic's parked rows evict only its own")
	}
	return nil
}
