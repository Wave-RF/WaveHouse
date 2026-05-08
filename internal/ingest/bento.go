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
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	// Bento component imports: only pure (processors) and io (http_client output).
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// safeIdentifierRe matches safe SQL identifiers to prevent injection.
var safeIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var (
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

type jsInput struct {
	consumer jetstream.Consumer
	iter     jetstream.MessagesContext
	chConn   driver.Conn
	inFlight atomic.Int32
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
		slog.InfoContext(msgCtx, "received message from JetStream", "subject", m.Subject())

		var raw struct {
			Action            string          `json:"action"`
			TableName         string          `json:"table_name"`
			ID                string          `json:"id"`
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

		// Validate table name to prevent SQL injection.
		if !safeIdentifierRe.MatchString(raw.TableName) {
			slog.WarnContext(msgCtx, "rejecting message with unsafe table name", "table", raw.TableName)
			// TODO: manually push to a DLQ subject with metadata for later analysis instead of silently dropping?
			if doubleAckErr := m.DoubleAck(msgCtx); doubleAckErr != nil {
				slog.WarnContext(msgCtx, "double ack failed for dropped message", "error", doubleAckErr)
			}
			continue
		}

		// Delete case
		if raw.Action == "delete" {
			tracer := otel.Tracer("wavehouse-worker")
			spanCtx, span := tracer.Start(msgCtx, "clickhouse_delete") // Use the extracted msgCtx

			slog.InfoContext(spanCtx, "delete received, waiting for in-flight inserts to drain", "table", raw.TableName)

			// Wait for in-flight insert messages to be finalized by Bento's pipeline.
			// The inFlight counter is decremented in the ackFn callback once the
			// message batch is fully processed (either successfully committed to
			// ClickHouse or routed to the DLQ). This drain ensures that pending
			// writes are finished before executing a delete operation.
			if j.inFlight.Load() > 0 {
				// NOTE: The effective drain window is min(60s, global_shutdown_timeout).
				// If the main context (ctx) is cancelled, the drain terminates immediately.
				flushCtx, flushCancel := context.WithTimeout(ctx, 60*time.Second)
				ticker := time.NewTicker(10 * time.Millisecond)

				for j.inFlight.Load() > 0 {
					select {
					case <-ctx.Done(): // Main context cancelled (shutdown)
						ticker.Stop()
						span.End()
						flushCancel() // Explicit cancel
						return nil, nil, ctx.Err()

					case <-flushCtx.Done(): // 60-second drain timeout hit or context cancelled
						ticker.Stop()
						span.End()
						flushCancel()

						if ctx.Err() != nil {
							return nil, nil, ctx.Err() // Return canonical cancellation
						}

						// Real 60-second timeout
						slog.ErrorContext(spanCtx, "timeout waiting for in-flight inserts to drain",
							"table", raw.TableName,
							"id", raw.ID,
						)
						// Only Nak if we aren't shutting down; otherwise, let NATS handle redelivery
						if nakErr := m.Nak(); nakErr != nil {
							slog.WarnContext(spanCtx, "nak failed during drain timeout", "error", nakErr)
						}

						// Use ErrNotConnected to trigger a Bento reconnect/retry rather than a fatal exit.
						return nil, nil, service.ErrNotConnected

					case <-ticker.C:
						// Keep waiting
					}
				}
				ticker.Stop()
				flushCancel() // Explicit cancel on successful drain
			}

			delQuery := fmt.Sprintf("DELETE FROM `%s` WHERE id = ?", raw.TableName)

			if err := j.chConn.Exec(spanCtx, delQuery, raw.ID); err != nil {
				span.RecordError(err)
				slog.ErrorContext(spanCtx, "clickhouse delete failed",
					"table", raw.TableName,
					"id", raw.ID,
					"error", err,
				)

				if nakErr := m.Nak(); nakErr != nil {
					slog.WarnContext(spanCtx, "nak failed after delete error", "error", nakErr)
				}

				span.End()
				return nil, nil, fmt.Errorf("execute delete: %w", err)
			}
			// Log the success with all context
			slog.InfoContext(spanCtx, "successfully deleted record",
				"table", raw.TableName,
				"id", raw.ID,
			)
			if doubleAckErr := m.DoubleAck(spanCtx); doubleAckErr != nil {
				slog.WarnContext(spanCtx, "double ack failed for processed delete message", "error", doubleAckErr)
			}

			span.End()
			continue
		} else if raw.Action != "insert" && raw.Action != "" {
			// ACTION SAFETY: Reject unrecognized actions (e.g., "update", "upsert")
			// to prevent them from accidentally falling through to the insert path.
			slog.WarnContext(msgCtx, "rejecting message: unknown action type", "action", raw.Action)
			if doubleAckErr := m.DoubleAck(msgCtx); doubleAckErr != nil {
				slog.WarnContext(msgCtx, "double ack failed for rejected message", "error", doubleAckErr)
			}
			continue
		}

		// Insert case
		payload := raw.Payload
		if len(payload) == 0 || string(payload) == "null" {
			slog.ErrorContext(msgCtx, "rejecting insert: empty payload/data")
			if doubleAckErr := m.DoubleAck(msgCtx); doubleAckErr != nil {
				slog.WarnContext(msgCtx, "double ack failed for malformed message", "error", doubleAckErr)
			}
			continue
		}

		msg := service.NewMessage(payload)

		msg = msg.WithContext(msgCtx)

		msg.MetaSet("table_name", raw.TableName)

		msg.MetaSet("received_timestamp", raw.ReceivedTimestamp)

		// Instead of time.Now(), ask NATS exactly when this message arrived in the queue
		publishedTime := time.Now()
		if meta, err := m.Metadata(); err == nil {
			publishedTime = meta.Timestamp
		}

		msg.MetaSet("bento_start_time", fmt.Sprintf("%d", publishedTime.UnixMilli()))

		j.inFlight.Add(1)

		ackFn := func(ackCtx context.Context, err error) error {
			defer j.inFlight.Add(-1)

			if err != nil {
				slog.ErrorContext(msgCtx, "batch processing failed", "error", err)
				// Log the Nak failure but return nil to Bento so it doesn't treat the Nak-error as a crash
				if nakErr := m.Nak(); nakErr != nil {
					slog.WarnContext(msgCtx, "nak failed for unprocessed batch", "error", nakErr)
				}
				return nil
			}

			slog.InfoContext(msgCtx, "message batch successfully acknowledged by ClickHouse")
			bentoEventsProcessed.Add(ackCtx, 1, metric.WithAttributes(
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
	js     jetstream.JetStream
	logger *slog.Logger
}

func (d *dlqOutput) Connect(ctx context.Context) error { return nil }
func (d *dlqOutput) Wait(ctx context.Context) error    { return nil }
func (d *dlqOutput) Close(ctx context.Context) error   { return nil }
func (d *dlqOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	for _, m := range batch {
		msgCtx := m.Context()
		data, _ := m.AsBytes()

		tableName, exists := m.MetaGet("table_name")

		subject := "dlq.unknown"
		if exists && tableName != "" {
			subject = "dlq." + tableName
		}

		if _, err := d.js.Publish(ctx, subject, data); err != nil {
			slog.ErrorContext(msgCtx, "NATS DLQ publish failed — message dropped", "subject", subject, "error", err)
			bentoDLQDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("table", tableName)))
		} else {
			slog.WarnContext(msgCtx, "sent failed message to DLQ", "subject", subject)
		}
	}
	return nil
}

type clickhouseOutput struct {
	httpClient *http.Client
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

	firstMsg := batch[0]

	tableName, ok := firstMsg.MetaGet("table_name")
	if !ok || tableName == "" {
		return fmt.Errorf("missing table_name in message metadata")
	}

	if !safeIdentifierRe.MatchString(tableName) {
		return fmt.Errorf("invalid table name: %q", tableName)
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
	tracer := otel.Tracer("wavehouse-worker")

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
	reqCtx, chSpan := tracer.Start(parentCtx, "clickhouse_insert")
	defer chSpan.End()

	var buf bytes.Buffer
	for _, msg := range batch {
		data, err := msg.AsBytes()
		if err != nil {
			chSpan.RecordError(err)
			slog.ErrorContext(reqCtx, "failed to read message bytes",
				"table", tableName,
				"error", err,
			)
			return fmt.Errorf("read message bytes: %w", err)
		}

		// Inject timestamp via byte-splicing to prevent float64 precision loss on large Int64/UInt64 values.
		if rt, ok := msg.MetaGet("received_timestamp"); ok && rt != "" {
			data = bytes.TrimSpace(data)
			// Ensure it's a valid JSON object structure {...}
			if len(data) >= 2 && data[0] == '{' && data[len(data)-1] == '}' {
				rtJSON, _ := json.Marshal(rt)

				// Check if the object has existing fields to see if we need a comma
				inside := data[1 : len(data)-1]
				needsComma := len(bytes.TrimSpace(inside)) > 0

				var extra string
				if needsComma {
					extra = fmt.Sprintf(`,"received_timestamp":%s}`, rtJSON)
				} else {
					extra = fmt.Sprintf(`"received_timestamp":%s}`, rtJSON)
				}

				// SAFE ALLOCATION: Create a completely new slice to avoid mutating the original message's backing array
				newData := make([]byte, 0, len(data)+len(extra))
				newData = append(newData, data[:len(data)-1]...) // copy everything except the trailing '}'
				newData = append(newData, []byte(extra)...)      // append the new field + closing '}'

				data = newData // reassign the local variable
			}
		}
		buf.Write(data)
		buf.WriteString("\n")
	}

	q := url.Values{}
	q.Set("database", c.db)

	q.Set("query", fmt.Sprintf("INSERT INTO `%s` FORMAT JSONEachRow", tableName))
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
		return fmt.Errorf("create clickhouse request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		chSpan.RecordError(err)
		return fmt.Errorf("execute clickhouse request: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, string(body))
		chSpan.RecordError(err)
		return err
	}

	return nil
}

// StartIngestWorker sets up the Bento-based ingest pipeline and returns the
// running stream for lifecycle management. Callers should call stream.Stop(ctx)
// during graceful shutdown to drain in-flight batches. The provided ctx controls
// the stream's lifetime — cancelling it initiates shutdown of the Bento pipeline.
//
// NOTE: Bento components are registered globally. The ClickHouse and NATS
// configurations passed to this function are frozen during the first call per
// process. Subsequent calls in the same process will silently use the original
// configuration for the output plugins.
func StartIngestWorker(ctx context.Context, nc *nats.Conn, streamName string, chConn driver.Conn, chHost, chHTTPPort, chHTTPScheme, chUser, chPassword, chDB string, onFatal func(error)) (*service.Stream, error) {
	host, _, err := net.SplitHostPort(chHost)
	if err != nil {
		host = chHost
	}

	logger := slog.Default().With("component", "bento")

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}

	setupCtx, setupCancel := context.WithTimeout(ctx, 10*time.Second)
	defer setupCancel()

	cons, err := js.CreateOrUpdateConsumer(setupCtx, streamName, jetstream.ConsumerConfig{
		Durable:       "buffer-consumer",
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
					chConn:   chConn,
				}, nil
			},
		); err != nil {
			registerErr = fmt.Errorf("register Bento input: %w", err)
			return
		}

		if err := service.RegisterBatchOutput("nats_dlq_bridge", service.NewConfigSpec(),
			func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
				return &dlqOutput{
					js:     js,
					logger: logger,
				}, service.BatchPolicy{}, 1, nil
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
			logger.ErrorContext(ctx, "ingest worker stopped unexpectedly", "error", err)

			if onFatal != nil {
				onFatal(err)
			}
		}
	}()

	return stream, nil
}
