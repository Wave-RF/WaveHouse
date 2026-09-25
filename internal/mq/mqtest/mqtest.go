// Package mqtest is the conformance suite for mq.Broker: the behavior the
// rest of the process relies on, stated once and run by every implementation
// from its own tests. The cases address events by mq.Topic alone and assume
// no layout — no stream, subject or partition names — so a backend passes by
// behaving, not by being built like the embedded one. Where backends
// legitimately differ, a Caps flag says which way; nothing else is optional.
package mqtest

import (
	"context"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	// Acme and Globex are the tenants Harness.New makes ready to publish.
	Acme   tenant.ID = "acme"
	Globex tenant.ID = "globex"
	// Durable is the consumer the suite creates, consumes and purges by: the
	// ingest worker's (ingest.BufferConsumerName), so a backend that only
	// finds durables an operator made has one to find.
	Durable = "buffer-consumer"
	// wait bounds every wait for something that should happen.
	wait = 5 * time.Second
	// quiet is how long a case watches for something that must not happen.
	quiet = 300 * time.Millisecond
)

// Harness is what a backend gives the suite.
type Harness struct {
	// New returns a fresh broker, isolated from every other New's, in which
	// Acme and Globex can publish, budgets already applied as the wiring
	// would. Its cleanup is registered on t and must tolerate the broker
	// having been closed already.
	New func(t *testing.T) mq.Broker
	// DeleteIngestDurable deletes the durable behind CreateConsumer while it
	// is consuming, as an operator could (the #587 failure path).
	DeleteIngestDurable func(t *testing.T, b mq.Broker, durable string)
	// Fill makes the next Publish for id refuse with mq.ErrQueueFull. nil
	// skips the cases that need it.
	Fill func(t *testing.T, b mq.Broker, id tenant.ID)
	Caps Caps
}

// Caps records where a backend's semantics legitimately differ.
type Caps struct {
	// PerTenantBudget: a full queue refuses its own tenant alone, so Fill on
	// one tenant leaves another publishing.
	PerTenantBudget bool
	// PurgesAcked: PurgeAcked removes acknowledged events past the cutoff,
	// rather than leaving retention to the broker's operator.
	PurgesAcked bool
	// UnbudgetedNotFound: DeadLetterCounts of a tenant never given a budget
	// is mq.ErrNoDeadLetterQueue rather than zero counts.
	UnbudgetedNotFound bool
	// ConfiguresDurables: CreateConsumer applies cfg.AckWait to the durable,
	// rather than checking it against one the operator configured.
	ConfiguresDurables bool
}

type testCase struct {
	name string
	// need, when false, skips the case: the backend lacks what it checks.
	need bool
	run  func(t *testing.T, h Harness)
}

// Run runs every case against h, each as a parallel subtest on a broker of
// its own. It sets the global W3C trace-context propagator for its duration
// (the trace case needs one), so it must not be called from a parallel test.
func Run(t *testing.T, h Harness) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	cases := []testCase{
		{"RoundTrip", true, roundTrip},
		{"RefusesATopicWithoutATenant", true, refusesATopicWithoutATenant},
		{"SubscribeCarriesTheTraceContext", true, subscribeCarriesTheTraceContext},
		{"SubscribeSeesEveryTenant", true, subscribeSeesEveryTenant},
		{"EachTenantInOrder", true, eachTenantInOrder},
		{"NakRedelivers", true, nakRedelivers},
		{"AckWaitRedelivers", h.Caps.ConfiguresDurables, ackWaitRedelivers},
		{"DeadLetterKeepsTheTopicAndDoesNotAck", true, deadLetterKeepsTheTopicAndDoesNotAck},
		{"DeadLetterCounts", true, deadLetterCounts},
		{"ReplaySince", true, replaySince},
		{"ReplaySinceStopsWhenContextIsDone", true, replaySinceStopsWhenContextIsDone},
		{"ReplaySincePullFailureIsAnError", true, replaySincePullFailureIsAnError},
		{"FailedOnceWhenTheDurableIsDeleted", true, failedOnceWhenTheDurableIsDeleted},
		{"FailedNeverAfterStop", true, failedNeverAfterStop},
		{"MaxBytesReportsTheBudget", true, maxBytesReportsTheBudget},
		{"Stats", true, stats},
		{"QueueFull", h.Fill != nil, queueFull},
		{"PurgeAcked", true, purgeAcked},
		{"PurgeAckedUnknownConsumer", true, purgeAckedUnknownConsumer},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !c.need {
				t.Skip("the backend's capabilities exclude this case")
			}
			t.Parallel()
			c.run(t, h)
		})
	}
}

// ctx is a test's context with the suite's overall bound.
func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(t.Context(), 4*wait)
	t.Cleanup(cancel)
	return c
}
