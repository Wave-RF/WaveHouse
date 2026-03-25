package ingest

import (
	"context"
	"fmt"
	"log"
    "net"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/warpstreamlabs/bento/public/service"
	_ "github.com/warpstreamlabs/bento/public/components/all"
)

type wrappedMessage struct {
	BentoMsg *service.Message
	NatsMsg  jetstream.Msg 
}

var dataChan = make(chan wrappedMessage, 1000)

func init() {
	err := service.RegisterInput(
		"nats_bridge",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return &chanInput{}, nil
		},
	)
	if err != nil {
		panic(err)
	}
}

type chanInput struct{}

func (c *chanInput) Connect(ctx context.Context) error { return nil }

func (c *chanInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	wrapped, ok := <-dataChan
	if !ok {
		return nil, nil, service.ErrNotConnected
	}

	// This is the function Bento will call AFTER the HTTP request finishes
	ackFn := func(ctx context.Context, err error) error {
		if err != nil {
			// Bento failed completely. NAK tells NATS to try again later.
			return wrapped.NatsMsg.Nak()
		}
		return wrapped.NatsMsg.Ack()
	}

	return wrapped.BentoMsg, ackFn, nil
}

func (c *chanInput) Close(ctx context.Context) error { return nil }

func StartIngestWorker(nc *nats.Conn, chHost, chHTTPPort, chUser, chPassword, chDB string) {
    host, _, err := net.SplitHostPort(chHost)
    if err != nil {
        host = chHost // If there was no port to begin with, just use the original
    }

	yamlConfig := fmt.Sprintf(`
input:
  nats_bridge: {}

output:
  fallback:
    - http_client:
        url: 'http://%s:%s/?database=%s&query=INSERT+INTO+${! json("table_name") }+FORMAT+JSONEachRow'
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
            - archive:
                format: lines
    - file:
        path: /tmp/bento_failed_ingest.jsonl
        codec: lines
`, host, chHTTPPort, chDB, chUser, chPassword)

	builder := service.NewStreamBuilder()

	if err := builder.SetYAML(yamlConfig); err != nil {
		log.Fatalf("Bento config error: %v", err)
	}

	stream, err := builder.Build()
	if err != nil {
		log.Fatalf("Bento build error: %v", err)
	}


	// Initialize modern JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to initialize JetStream: %v", err)
	}

    // _ = js.DeleteConsumer(context.Background(), "BEACHHOUSE", "buffer-consumer")

	// Create or bind to the Durable Pull Consumer
	// This makes sure the Sweeper can find "buffer-consumer" and read its AckFloor!
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "BEACHHOUSE", jetstream.ConsumerConfig{
		Durable:       "buffer-consumer",
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("Failed to create durable pull consumer: %v", err)
	}

	// This safely pulls data in the background and hands it to our function.
	_, err = cons.Consume(func(m jetstream.Msg) {
		wrapped := wrappedMessage{
			BentoMsg: service.NewMessage(m.Data()),
			NatsMsg:  m,
		}

		dataChan <- wrapped

		// We trust Bento's AckFunc to handle m.Ack() and m.Nak()
	})
	if err != nil {
		log.Fatalf("Failed to consume from NATS: %v", err)
	}
	
	go func() {
		fmt.Println("***** Bento Zero-Port Worker Running *****")
		if err := stream.Run(context.Background()); err != nil {
			log.Printf("Bento worker stopped: %v", err)
		}
	}()
}