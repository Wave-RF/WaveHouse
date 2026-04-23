package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	// Bento component imports: only pure (processors) and io (http_client output).
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/attribute" // ADD THIS
	"go.opentelemetry.io/otel/metric"
)

// safeIdentifierRe matches safe SQL identifiers to prevent injection.
var safeIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var (
	bentoMeter              = otel.Meter("wavehouse-bento")
	bentoEventsProcessed, _ = bentoMeter.Int64Counter(
		"wavehouse_bento_events_processed",
		metric.WithDescription("Total number of events successfully processed by Bento"),
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

		msgCtx := observability.ExtractNATS(context.Background(), m)
		slog.InfoContext(msgCtx, "received message from JetStream", "subject", m.Subject())

		var raw struct {
			Action    string `json:"action"`
			TableName string `json:"table_name"`
			ID        string `json:"id"`
		}
		if err := json.Unmarshal(m.Data(), &raw); err != nil {
			slog.Error("rejecting message: invalid JSON", "error", err)
			if ackErr := m.Ack(); ackErr != nil { // Drop — not retryable.
				slog.Warn("ack failed for invalid JSON message", "error", ackErr)
			}
			continue
		}

		// Reject messages with no table name.
		if raw.TableName == "" {
			slog.Error("rejecting message: empty table_name")
			if ackErr := m.Ack(); ackErr != nil {
				slog.Warn("ack failed for empty-table message", "error", ackErr)
			}
			continue
		}

		// Validate table name to prevent SQL injection.
		if raw.TableName != "" && !safeIdentifierRe.MatchString(raw.TableName) {
			slog.WarnContext(msgCtx, "rejecting message with unsafe table name", "table", raw.TableName)
			// TODO: manually push to a DLQ subject with metadata for later analysis instead of silently dropping?
			if ackErr := m.Ack(); ackErr != nil { // Drop malformed messages.
				slog.Warn("ack failed for unsafe-table message", "error", ackErr)
			}
			continue
		}

		// Delete case
		if raw.Action == "delete" {
			tracer := otel.Tracer("wavehouse-worker")
			spanCtx, span := tracer.Start(msgCtx, "clickhouse_delete") // Use the extracted msgCtx

			slog.InfoContext(spanCtx, "DELETE DETECTED: Flushing buffer...", "table", raw.TableName)

			// Wait for in-flight insert messages to be acked by Bento's pipeline.
			// The inFlight counter is decremented when the http_client output
			// receives a 200 from ClickHouse, so inFlight==0 means all prior
			// inserts have been committed to ClickHouse. This requires Bento's
			// default max_in_flight > 1 so acks can flow back while Read blocks.
			ticker := time.NewTicker(10 * time.Millisecond)
			for j.inFlight.Load() > 0 {
				select {
				case <-ctx.Done():
					ticker.Stop()
					if nakErr := m.Nak(); nakErr != nil {
						slog.WarnContext(spanCtx, "nak failed during ctx cancellation", "error", nakErr)
					}
					return nil, nil, ctx.Err()
				case <-ticker.C:
				}
			}
			ticker.Stop()

			delQuery := fmt.Sprintf("DELETE FROM %s WHERE id = ?", raw.TableName)

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
			} else {
				// Log the success with all context
				slog.InfoContext(spanCtx, "successfully deleted record",
					"table", raw.TableName,
					"id", raw.ID,
				)
				if ackErr := m.Ack(); ackErr != nil {
					slog.WarnContext(spanCtx, "ack failed after delete", "error", ackErr)
				}
			}
			span.End()
			continue
		}

		// Insert case
		msg := service.NewMessage(m.Data())
		msg = msg.WithContext(msgCtx)

		tracer := otel.Tracer("wavehouse-worker")
		bufferCtx, bufferSpan := tracer.Start(msgCtx, "bento_batch_buffer")

		msg.MetaSet("table_name", raw.TableName)

		j.inFlight.Add(1)

		ackFn := func(ctx context.Context, err error) error {
			j.inFlight.Add(-1)

			if err != nil {
				bufferSpan.RecordError(err)
				slog.ErrorContext(bufferCtx, "batch processing failed", "error", err)
			} else {
				slog.InfoContext(bufferCtx, "message batch successfully acknowledged by ClickHouse")

				bentoEventsProcessed.Add(ctx, 1, metric.WithAttributes(
					attribute.String("table", raw.TableName),
				))
			}

			bufferSpan.End()

			if err != nil {
				return m.Nak()
			}
			return m.Ack()
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
		} else {
			slog.WarnContext(msgCtx, "sent failed message to DLQ", "subject", subject)
		}
	}
	return nil
}

// StartIngestWorker sets up the Bento-based ingest pipeline and returns the
// running stream for lifecycle management. Callers should call stream.Stop(ctx)
// during graceful shutdown to drain in-flight batches. The provided ctx controls
// the stream's lifetime — cancelling it initiates shutdown of the Bento pipeline.
func StartIngestWorker(ctx context.Context, nc *nats.Conn, chConn driver.Conn, chHost, chHTTPPort, chUser, chPassword, chDB string) (*service.Stream, error) {
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

	cons, err := js.CreateOrUpdateConsumer(setupCtx, "WAVEHOUSE", jetstream.ConsumerConfig{
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
	})
	if registerErr != nil {
		return nil, registerErr
	}

	yamlConfig := fmt.Sprintf(`
input:
  nats_bridge: {}
output:
  fallback:
    - http_client:
        url: 'http://%s:%s/?database=%s&query=INSERT+INTO+${! meta("table_name") }+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1&date_time_input_format=best_effort'
        verb: POST
        headers:
          Content-Type: application/json
          X-ClickHouse-User: "%s"
          X-ClickHouse-Key: "%s"
        batching:
          count: 500
          period: 5s
          processors:
            - group_by_value:
                value: '${! json("table_name") }'
            - mapping: |
                meta table_name = this.table_name
                root = this.data | {}
                root.received_timestamp = this.received_timestamp | deleted()
            - archive:
                format: lines
    - nats_dlq_bridge: {}
`, host, chHTTPPort, chDB, chUser, chPassword)

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
		logger.Info("ingest worker started")
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("ingest worker stopped", "error", err)
		}
	}()

	return stream, nil
}
