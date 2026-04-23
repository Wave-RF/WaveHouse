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
	"regexp"
	"strconv"
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
			_ = m.DoubleAck(ctx) // Drop — not retryable.
			continue
		}

		// Reject messages with no table name.
		if raw.TableName == "" {
			slog.Error("rejecting message: empty table_name")
			_ = m.DoubleAck(ctx)
			continue
		}

		// Validate table name to prevent SQL injection.
		if raw.TableName != "" && !safeIdentifierRe.MatchString(raw.TableName) {
			slog.WarnContext(msgCtx, "rejecting message with unsafe table name", "table", raw.TableName)
			// TODO: manually push to a DLQ subject with metadata for later analysis instead of silently dropping?
			_ = m.DoubleAck(ctx)
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
				_ = m.DoubleAck(ctx)
			}
			span.End()
			continue
		}

		// Insert case
		msg := service.NewMessage(m.Data())
		
		msg = msg.WithContext(msgCtx) 

		msg.MetaSet("table_name", raw.TableName)

		// Instead of time.Now(), ask NATS exactly when this message arrived in the queue
		publishedTime := time.Now()
		if meta, err := m.Metadata(); err == nil {
			publishedTime = meta.Timestamp
		}
		
		msg.MetaSet("bento_start_time", fmt.Sprintf("%d", publishedTime.UnixMilli()))

		j.inFlight.Add(1)

		ackFn := func(ctx context.Context, err error) error {
			j.inFlight.Add(-1)

			if err != nil {
				// Replaced bufferCtx with msgCtx and removed RecordError
				slog.ErrorContext(msgCtx, "batch processing failed", "error", err)
			} else {
				// Replaced bufferCtx with msgCtx
				slog.InfoContext(msgCtx, "message batch successfully acknowledged by ClickHouse")

				bentoEventsProcessed.Add(ctx, 1, metric.WithAttributes(
					attribute.String("table", raw.TableName),
				))
			}

			if err != nil {
				return m.Nak()
			}
			return m.DoubleAck(ctx)
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

type clickhouseOutput struct {
	httpClient *http.Client
	host       string
	port       string
	user       string
	password   string
	db         string
}

func (c *clickhouseOutput) Connect(ctx context.Context) error { return nil }
func (c *clickhouseOutput) Close(ctx context.Context) error  { return nil }

func (c *clickhouseOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	tableName, _ := batch[0].MetaGet("table_name")

	// 1. Fetch the timestamp stamped during jsInput.Read
	startStr, _ := batch[0].MetaGet("bento_start_time")
	
	bentoStartTime := time.Now() // Default fallback
	
	// Parse the string into an int64 integer
	if startMilli, err := strconv.ParseInt(startStr, 10, 64); err == nil {
		// Convert the integer back into a real Go time.Time object
		bentoStartTime = time.UnixMilli(startMilli)
	} else {
		slog.Warn("Failed to parse bento_start_time int", "startStr", startStr, "error", err)
	}
	// 2. Extract original API trace context and setup Tracer
	parentCtx := batch[0].Context()
	tracer := otel.Tracer("wavehouse-worker")

	// 3. RETROACTIVELY DRAW BENTO SPAN (Starts in past, ends exactly NOW)
	_, bentoSpan := tracer.Start(parentCtx, "bento_queue_wait", trace.WithTimestamp(bentoStartTime))
	bentoSpan.End(trace.WithTimestamp(time.Now()))

	// 4. START THE CLICKHOUSE SPAN (Sibling to Bento span)
	reqCtx, chSpan := tracer.Start(parentCtx, "clickhouse_insert")
	defer chSpan.End()

	var buf bytes.Buffer
	for _, msg := range batch {
		data, err := msg.AsBytes()
		if err != nil {
			chSpan.RecordError(err)
			continue
		}
		buf.Write(data)
		buf.WriteString("\n") // ClickHouse JSONEachRow requires newline separation
	}

	// Building the HTTP request
	url := fmt.Sprintf("http://%s:%s/?database=%s&query=INSERT+INTO+%s+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1&date_time_input_format=best_effort",
		c.host, c.port, c.db, tableName)

	req, err := http.NewRequestWithContext(reqCtx, "POST", url, &buf)
	if err != nil {
		chSpan.RecordError(err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		chSpan.RecordError(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
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
func StartIngestWorker(ctx context.Context, nc *nats.Conn, streamName string, chConn driver.Conn, chHost, chHTTPPort, chUser, chPassword, chDB string) (*service.Stream, error) {
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
				return &clickhouseOutput{
					httpClient: &http.Client{Timeout: 15 * time.Second},
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
                value: '${! json("table_name") }'
            - mapping: |
                meta bento_start_time = meta("bento_start_time")
                meta table_name = meta("table_name")
                root = this.data
                root.received_timestamp = deleted()
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
		logger.Info("ingest worker started")
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("ingest worker stopped", "error", err)
		}
	}()

	return stream, nil
}
