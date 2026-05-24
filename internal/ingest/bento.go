package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	// Bento component imports: only pure (processors) and io (http_client output).
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/query"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	// Pre-existing bento-specific counters. Naming retained for dashboard
	// continuity; the new cross-cutting metrics (ingest duration, CH
	// duration/errors) live in observability/instruments.go.
	bentoMeter              = otel.Meter("wavehouse-bento")
	bentoEventsProcessed, _ = bentoMeter.Int64Counter(
		"wavehouse_bento_events_processed",
		metric.WithDescription("Total number of events successfully processed by Bento"),
	)
	bentoDLQDropped, _ = bentoMeter.Int64Counter(
		"wavehouse_bento_dlq_dropped",
		metric.WithDescription("Total number of messages permanently dropped from DLQ due to NATS failure"),
	)

	registerOnce sync.Once
	registerErr  error
)

// clickhouseErrCode parses ClickHouse's "Code: 60. DB::Exception: ..." prefix
// out of a non-2xx response body. Returns 0 when the body has no recognizable
// code (e.g. a network-level failure surfaced from the HTTP client, or an
// empty body). The numeric code goes onto the wavehouse_clickhouse_errors_total
// counter's `clickhouse_code` label so dashboards can split out "table doesn't
// exist" (60) from "too many parts" (252) etc. without parsing message text.
var clickhouseCodeRe = regexp.MustCompile(`^Code: (\d+)`)

func clickhouseErrCode(body []byte) string {
	if m := clickhouseCodeRe.FindSubmatch(body); m != nil {
		return string(m[1])
	}
	return "0"
}

// parseReceivedTimestamp extracts the API-side ingest timestamp from message
// metadata. Returns the zero time when the metadata is missing or unparseable
// — callers must check IsZero() before computing a duration.
func parseReceivedTimestamp(m *service.Message) time.Time {
	rt, ok := m.MetaGet("received_timestamp")
	if !ok || rt == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, rt)
	if err != nil {
		return time.Time{}
	}
	return t
}

type jsInput struct {
	consumer jetstream.Consumer
	iter     jetstream.MessagesContext
}

func (j *jsInput) Connect(ctx context.Context) error {
	iter, err := j.consumer.Messages()
	if err != nil {
		return fmt.Errorf("create jetstream iterator: %w", err)
	}
	j.iter = iter
	return nil
}

func (j *jsInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for {
		m, err := j.iter.Next()
		if err != nil {
			return nil, nil, service.ErrNotConnected
		}

		msgCtx := observability.ExtractNATS(ctx, m)
		// Stamp the bento component on the per-message context so every
		// slog.XContext call below carries `component=ingest/bento`.
		// ExtractNATS only adds W3C trace propagation; without this stamp
		// the high-volume operational logs (one per inbound event) emit
		// without the component field that the rest of the binary advertises.
		msgCtx = observability.WithComponent(msgCtx, "ingest/bento")
		// Per-message receipt is DEBUG: at ingest scale this fires for every
		// inbound event. Keeping it at INFO floods stdout and any log shipper.
		slog.DebugContext(msgCtx, "received message from JetStream", "subject", m.Subject())

		var raw struct {
			TableName         string          `json:"table_name"`
			Scope             string          `json:"scope,omitempty"`
			Payload           json.RawMessage `json:"data"`
			ReceivedTimestamp string          `json:"received_timestamp"`
		}
		if err := json.Unmarshal(m.Data(), &raw); err != nil {
			slog.ErrorContext(msgCtx, "rejecting message: invalid JSON", "error", err)
			if doubleAckErr := m.DoubleAck(msgCtx); doubleAckErr != nil {
				slog.WarnContext(msgCtx, "double ack failed for dropped message", "error", doubleAckErr)
			}
			continue
		}

		// Reject messages with no table name.
		if raw.TableName == "" {
			slog.ErrorContext(msgCtx, "rejecting message: empty table_name")
			if doubleAckErr := m.DoubleAck(msgCtx); doubleAckErr != nil {
				slog.WarnContext(msgCtx, "double ack failed for dropped message", "error", doubleAckErr)
			}
			continue
		}

		payload := raw.Payload
		if len(payload) == 0 || string(payload) == "null" {
			// TODO: if they have a table with all defaults, would this not be a valid insert?
			slog.ErrorContext(msgCtx, "rejecting insert: empty payload/data")
			if doubleAckErr := m.DoubleAck(msgCtx); doubleAckErr != nil {
				slog.WarnContext(msgCtx, "double ack failed for malformed message", "error", doubleAckErr)
			}
			continue
		}

		msg := service.NewMessage(payload)

		msg = msg.WithContext(msgCtx)

		msg.MetaSet("table_name", raw.TableName)
		msg.MetaSet("scope", raw.Scope)
		msg.MetaSet("received_timestamp", raw.ReceivedTimestamp)

		// Instead of time.Now(), ask NATS exactly when this message arrived in the queue
		publishedTime := time.Now()
		if meta, err := m.Metadata(); err == nil {
			publishedTime = meta.Timestamp
		}
		msg.MetaSet("bento_start_time", fmt.Sprintf("%d", publishedTime.UnixMilli()))

		ackFn := func(ackCtx context.Context, err error) error {
			if err != nil {
				slog.ErrorContext(msgCtx, "batch processing failed", "error", err)
				// Log the Nak failure but return nil to Bento so it doesn't treat the Nak-error as a crash
				if nakErr := m.Nak(); nakErr != nil {
					slog.WarnContext(msgCtx, "nak failed for unprocessed batch", "error", nakErr)
				}
				return nil
			}

			// Per-message ack is DEBUG for the same reason as the receive log —
			// at production load this fires once per row.
			slog.DebugContext(msgCtx, "message batch acknowledged by ClickHouse")
			// Counter increment uses msgCtx (component-stamped) rather than
			// Bento's raw ackCtx so trace_id/span_id and component propagate
			// onto the exemplar for the data point.
			bentoEventsProcessed.Add(msgCtx, 1, metric.WithAttributes(
				attribute.String("table", raw.TableName),
			))

			return m.DoubleAck(ackCtx)
		}
		return msg, ackFn, nil
	}
}

func (j *jsInput) Close(ctx context.Context) error {
	if j.iter != nil {
		j.iter.Stop()
	}
	return nil
}

type dlqOutput struct {
	js jetstream.JetStream
}

func (d *dlqOutput) Connect(ctx context.Context) error { return nil }
func (d *dlqOutput) Wait(ctx context.Context) error    { return nil }
func (d *dlqOutput) Close(ctx context.Context) error   { return nil }
func (d *dlqOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	for _, m := range batch {
		// Stamp component on the per-message context — see jsInput.Read for
		// the same pattern. m.Context() preserves the trace context from the
		// inbound batch but does not carry the bento component label.
		msgCtx := observability.WithComponent(m.Context(), "ingest/bento")
		data, _ := m.AsBytes()

		tableName, tableNameSet := m.MetaGet("table_name")
		scope, scopeSet := m.MetaGet("scope")

		subject := "dlq.unknown"
		if tableNameSet && tableName != "" {
			subject = "dlq." + query.SafeEncodeNATS(tableName)
			if scopeSet && scope != "" {
				subject += "." + query.SafeEncodeNATS(scope)
			}
		}

		if _, err := d.js.Publish(ctx, subject, data); err != nil {
			slog.ErrorContext(msgCtx, "NATS DLQ publish failed — message dropped", "subject", subject, "error", err)
			bentoDLQDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("table", tableName)))
			recordIngestDuration(ctx, m, tableName, "dropped")
		} else {
			slog.WarnContext(msgCtx, "sent failed message to DLQ", "subject", subject)
			recordIngestDuration(ctx, m, tableName, "dlq")
		}
	}
	return nil
}

// recordIngestDuration records the end-to-end ingest latency for one message,
// from the API handler's receive timestamp to the present moment. Skipped
// silently when the receive timestamp is missing/unparseable — happens for
// drop paths inside the worker that never see a well-formed envelope.
func recordIngestDuration(ctx context.Context, m *service.Message, table, outcome string) {
	t := parseReceivedTimestamp(m)
	if t.IsZero() {
		return
	}
	observability.IngestDuration.Record(ctx, time.Since(t).Seconds(),
		metric.WithAttributes(
			attribute.String("table", table),
			attribute.String("outcome", outcome),
		))
}

type clickhouseOutput struct {
	httpClient *http.Client
	cache      cache.Cache
	scheme     string
	host       string
	port       string
	user       string
	password   string
	db         string
}

func (c *clickhouseOutput) Connect(ctx context.Context) error { return nil }
func (c *clickhouseOutput) Close(ctx context.Context) error   { return nil }

func (c *clickhouseOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// Stamp component on the batch-scoped context so the slog calls below
	// (and the descendants we derive via tracer.Start) carry
	// `component=ingest/bento`.
	ctx = observability.WithComponent(ctx, "ingest/bento")

	firstMsg := batch[0]

	tableName, tableNameSet := firstMsg.MetaGet("table_name")
	if !tableNameSet || tableName == "" {
		return fmt.Errorf("missing table_name in message metadata")
	}

	bentoStartTime := time.Now()
	var oldestMilli int64 = -1

	for _, m := range batch {
		if startStr, ok := m.MetaGet("bento_start_time"); ok {
			if milli, err := strconv.ParseInt(startStr, 10, 64); err == nil {
				if oldestMilli == -1 || milli < oldestMilli {
					oldestMilli = milli
				}
			}
		}
	}

	if oldestMilli != -1 {
		bentoStartTime = time.UnixMilli(oldestMilli)
	} else {
		slog.WarnContext(ctx, "failed to parse any bento_start_time in batch, falling back to time.Now()")
	}

	// Extract original API trace context and setup Tracer
	parentCtx := trace.ContextWithSpanContext(ctx, trace.SpanContextFromContext(firstMsg.Context()))
	tracer := observability.Tracer()

	var links []trace.Link
	for i := 1; i < len(batch); i++ {
		spanCtx := trace.SpanContextFromContext(batch[i].Context())
		if spanCtx.IsValid() {
			links = append(links, trace.Link{SpanContext: spanCtx})
		}
	}

	// RETROACTIVELY DRAW BENTO SPAN (Starts in past, ends exactly NOW)
	_, bentoSpan := tracer.Start(parentCtx, "bento_queue_wait",
		trace.WithTimestamp(bentoStartTime),
		trace.WithLinks(links...),
		trace.WithAttributes(attribute.Int("batch_size", len(batch))),
	)
	bentoSpan.End(trace.WithTimestamp(time.Now()))

	// START THE CLICKHOUSE SPAN (Sibling to Bento span)
	reqCtx, chSpan := tracer.Start(parentCtx, "clickhouse_insert",
		trace.WithAttributes(
			attribute.String("clickhouse.operation", "insert"),
			attribute.String("clickhouse.table", tableName),
			attribute.Int("batch.size", len(batch)),
		),
	)
	defer chSpan.End()

	insertStart := time.Now()
	chAttrs := metric.WithAttributes(
		attribute.String("operation", "insert"),
		attribute.String("table", tableName),
	)

	// set of all scope values in the batch for invalidation
	allScopes := make(map[string]struct{})

	var buf bytes.Buffer
	for _, msg := range batch {
		scope, scopeSet := msg.MetaGet("scope")
		if !scopeSet {
			scope = ""
		}
		allScopes[scope] = struct{}{}

		data, err := msg.AsBytes()
		if err != nil {
			chSpan.RecordError(err)
			slog.ErrorContext(reqCtx, "failed to read message bytes",
				"table", tableName,
				"scope", scope,
				"error", err,
			)
			// TODO: is this path just dropping messages right now?
			return fmt.Errorf("read message bytes: %w", err)
		}

		buf.Write(data)
		buf.WriteString("\n")
	}

	q := url.Values{}
	q.Set("database", c.db)

	q.Set("param_target_table", tableName)

	q.Set("query", "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow")
	q.Set("input_format_skip_unknown_fields", "1")
	q.Set("date_time_input_format", "best_effort")

	u := &url.URL{
		Scheme:   c.scheme,
		Host:     net.JoinHostPort(c.host, c.port), // Safely joins host and port, adding IPv6 brackets if needed
		RawQuery: q.Encode(),                       // Safely URL-encodes all parameters
	}

	req, err := http.NewRequestWithContext(reqCtx, "POST", u.String(), &buf)
	if err != nil {
		chSpan.RecordError(err)
		observability.ClickHouseErrors.Add(reqCtx, 1, metric.WithAttributes(
			attribute.String("operation", "insert"),
			attribute.String("clickhouse_code", "0"),
		))
		return fmt.Errorf("create clickhouse request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.password)

	// TODO: have ClickHouse insert all the rows it can, only failing rows that are invalid instead of the entire batch.
	// TODO: how do we know which of the scope set to invalidate?
	resp, err := c.httpClient.Do(req)
	if err != nil {
		chSpan.RecordError(err)
		observability.ClickHouseDuration.Record(reqCtx, time.Since(insertStart).Seconds(), chAttrs)
		observability.ClickHouseErrors.Add(reqCtx, 1, metric.WithAttributes(
			attribute.String("operation", "insert"),
			attribute.String("clickhouse_code", "0"),
		))
		return fmt.Errorf("execute clickhouse request: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		chCode := clickhouseErrCode(body)
		err := fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, string(body))
		chSpan.RecordError(err)
		chSpan.SetAttributes(attribute.String("clickhouse.error_code", chCode))
		observability.ClickHouseDuration.Record(reqCtx, time.Since(insertStart).Seconds(), chAttrs)
		observability.ClickHouseErrors.Add(reqCtx, 1, metric.WithAttributes(
			attribute.String("operation", "insert"),
			attribute.String("clickhouse_code", chCode),
		))
		return err
	}

	observability.ClickHouseDuration.Record(reqCtx, time.Since(insertStart).Seconds(), chAttrs)
	// Record end-to-end ingest duration for every successfully committed row
	// in the batch. We emit per-row rather than per-batch so the histogram
	// reflects per-event SLO, not per-batch (which would understate the
	// batch's actual fan-out to N rows).
	for _, m := range batch {
		recordIngestDuration(reqCtx, m, tableName, "committed")
	}

	// TODO: is this the only place we need to invalidate the cache? How do partial failures work?
	// TODO: what context do we use here? reqCtx? parentCtx? or a new one?
	if c.cache != nil {
		// Detached context so cancellation doesn't abort invalidation
		invCtx := trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(reqCtx))
		if _, err := c.cache.InvalidateCache(invCtx, tableName, allScopes); err != nil {
			slog.ErrorContext(reqCtx, "failed to invalidate cache after insert - your cache is holding stale data now!", "table", tableName, "error", err)
			// TODO: we need a safer mechanism to ensure the cache invalidation can be retried or something here – for now the assumption is ClickHouse insertion errors will happen far more frequently than ristretto increments will, so it should be an incredibly small/unlikely edge case that will be loudly logged, but this MUST be addressed before cache implementations like Redis are added
		}
	}

	return nil
}

// StartIngestWorker sets up the Bento-based ingest pipeline and returns the
// running stream for lifecycle management. Callers should call stream.Stop(ctx)
// during graceful shutdown to drain in-flight batches. The provided ctx controls
// the stream's lifetime — cancelling it initiates shutdown of the Bento pipeline.
func StartIngestWorker(ctx context.Context, nc *nats.Conn, cache cache.Cache, chHost, chHTTPPort, chHTTPScheme, chUser, chPassword, chDB string, onFatal func(error)) (*service.Stream, error) {
	host, _, err := net.SplitHostPort(chHost)
	if err != nil {
		host = chHost
	}

	logger := slog.Default().With("component", "ingest/bento")

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}

	setupCtx, setupCancel := context.WithTimeout(ctx, 10*time.Second)
	defer setupCancel()

	cons, err := js.CreateOrUpdateConsumer(setupCtx, mq.StreamName(), jetstream.ConsumerConfig{
		Durable:       BufferConsumerName,
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create durable pull consumer: %w", err)
	}

	registerOnce.Do(func() {
		if err := service.RegisterInput("nats_bridge", service.NewConfigSpec(),
			func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
				return &jsInput{
					consumer: cons,
				}, nil
			},
		); err != nil {
			registerErr = fmt.Errorf("register Bento input: %w", err)
			return
		}

		if err := service.RegisterBatchOutput("nats_dlq_bridge", service.NewConfigSpec(),
			func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
				return &dlqOutput{js: js}, service.BatchPolicy{}, 1, nil
			},
		); err != nil {
			registerErr = fmt.Errorf("register Bento DLQ output: %w", err)
		}

		if err := service.RegisterBatchOutput("clickhouse_json_bridge", service.NewConfigSpec(),
			func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
				scheme := chHTTPScheme
				if scheme == "" {
					scheme = "http" // Default fallback
				}
				return &clickhouseOutput{
					httpClient: &http.Client{Timeout: 30 * time.Second},
					cache:      cache,
					scheme:     scheme,
					host:       host,
					port:       chHTTPPort,
					user:       chUser,
					password:   chPassword,
					db:         chDB,
				}, service.BatchPolicy{}, 1, nil
			},
		); err != nil {
			registerErr = fmt.Errorf("register ClickHouse output: %w", err)
		}
	})
	if registerErr != nil {
		return nil, registerErr
	}

	yamlConfig := `
input:
  nats_bridge: {}
output:
  fallback:
    - clickhouse_json_bridge:
        batching:
          count: 500
          period: 5s
          processors:
            - group_by_value:
                value: '${! meta("table_name") }'
    - nats_dlq_bridge: {}
`

	builder := service.NewStreamBuilder()
	builder.SetLogger(logger)

	if err := builder.SetYAML(yamlConfig); err != nil {
		return nil, fmt.Errorf("bento config: %w", err)
	}

	stream, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("bento build: %w", err)
	}

	go func() {
		logger.InfoContext(ctx, "ingest worker started")
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			// TODO: if bento fails unexpectedly, we probably want a retry loop here instead of just logging and exiting?
			logger.ErrorContext(ctx, "ingest worker stopped unexpectedly", "error", err)

			if onFatal != nil {
				onFatal(err)
			}
		}
	}()

	return stream, nil
}
