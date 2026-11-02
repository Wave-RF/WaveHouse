//go:build integration

package mq

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// An external publish is one span across its retries, counting them.
func TestExternalNATS_PublishSpan(t *testing.T) {
	rec := recordSpans(t)
	f := shippedFixture(t)
	e := f.broker(t, nil)
	flaky := &flakyPublish{JetStream: e.js}
	flaky.lose.Store(publishRetries)
	e.js = flaky

	pctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	require.NoError(t, e.Publish(pctx, Topic{Tenant: "acme", Table: "t"}, []byte("x")))
	parent.End()

	spans := publishSpans(rec)
	require.Len(t, spans, 1)
	assertPublishSpan(t, spans[0], parent.SpanContext(), publishKindIngest, publishRetries+1)
}
