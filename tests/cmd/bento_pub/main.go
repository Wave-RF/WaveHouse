package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	os.Exit(run())
}

// run orchestrates the smoke test and returns a non-zero exit code on any
// setup or verification failure so CI scripts and `make smoke-test` wrappers
// can distinguish success from failure.
func run() int {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Printf("NATS Connect Error: %v", err)
		return 1
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Printf("JetStream Init Error: %v", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "WAVEHOUSE",
		Subjects: []string{"ingest.>"},
		Storage:  jetstream.FileStorage,
	}); err != nil {
		log.Printf("Failed to setup NATS Stream: %v", err)
		return 1
	}
	log.Println("NATS Stream 'WAVEHOUSE' is configured and ready.")

	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		log.Printf("ClickHouse Connect Error: %v", err)
		return 1
	}
	defer func() { _ = chConn.Close() }()

	createTable := `
    CREATE TABLE IF NOT EXISTS users (
        id String,
        name String,
        received_timestamp Int64,
        table_name String
    ) ENGINE = MergeTree()
    ORDER BY id`
	if err := chConn.Exec(context.Background(), createTable); err != nil {
		log.Printf("Failed to create table: %v", err)
		return 1
	}
	log.Println("ClickHouse table 'users' is ready.")

	insertData, _ := json.Marshal(map[string]interface{}{
		"action":     "insert",
		"table_name": "users",
		"data":       map[string]string{"id": "99", "name": "Bento User"},
	})
	if _, err := js.Publish(ctx, "ingest.users", insertData); err != nil {
		log.Printf("Insert Publish Error: %v", err)
		return 1
	}
	log.Println("Published Insert to ingest.users")

	// We wait 100ms just to ensure NATS sequence order is preserved
	time.Sleep(100 * time.Millisecond)

	deleteData, _ := json.Marshal(map[string]interface{}{
		"action":     "delete",
		"table_name": "users",
		"id":         "99",
	})
	if _, err := js.Publish(ctx, "ingest.users", deleteData); err != nil {
		log.Printf("Delete Publish Error: %v", err)
		return 1
	}
	log.Println("Published Delete to ingest.users")

	// Since Bento has a 5s batch window, we wait at least 6s to be safe
	log.Println("Waiting 7 seconds for Bento to flush batch and Worker to execute delete...")
	time.Sleep(7 * time.Second)

	var count uint64
	if err := chConn.QueryRow(context.Background(), "SELECT count() FROM users WHERE id = '99'").Scan(&count); err != nil {
		log.Printf("Verification Query Failed: %v", err)
		return 1
	}
	if count == 0 {
		fmt.Println("SUCCESS: The record was inserted and then deleted correctly!")
		return 0
	}
	fmt.Printf("FAILED: Record still exists (%d rows). Check worker logs for 'DELETE DETECTED' message.\n", count)
	return 1
}
