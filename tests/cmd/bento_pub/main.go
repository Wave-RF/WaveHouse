package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal("NATS Connect Error: ", err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "WAVEHOUSE",
		Subjects: []string{"ingest.>"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		log.Fatal("Failed to setup NATS Stream: ", err)
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
		log.Fatal("ClickHouse Connect Error: ", err)
	}

	createTable := `
    CREATE TABLE IF NOT EXISTS users (
        id String,
        name String,
        received_timestamp Int64,
        table_name String
    ) ENGINE = MergeTree()
    ORDER BY id`
	err = chConn.Exec(context.Background(), createTable)
	if err != nil {
		log.Fatal("Failed to create table: ", err)
	}
	log.Println("ClickHouse table 'users' is ready.")

	insertData, _ := json.Marshal(map[string]interface{}{
		"action":     "insert",
		"table_name": "users",
		"data":       map[string]string{"id": "99", "name": "Bento User"},
	})
	_, err = js.Publish(ctx, "ingest.users", insertData)
	if err != nil {
		log.Fatal("Insert Publish Error: ", err)
	}
	log.Println("Published Insert to ingest.users")

	// We wait 100ms just to ensure NATS sequence order is preserved
	time.Sleep(100 * time.Millisecond)

	deleteData, _ := json.Marshal(map[string]interface{}{
		"action":     "delete",
		"table_name": "users",
		"id":         "99",
	})
	_, err = js.Publish(ctx, "ingest.users", deleteData)
	if err != nil {
		log.Fatal("Delete Publish Error: ", err)
	}
	log.Println("Published Delete to ingest.users")

	// Since Bento has a 5s batch window, we wait at least 6s to be safe
	log.Println("Waiting 7 seconds for Bento to flush batch and Worker to execute delete...")
	time.Sleep(7 * time.Second)

	var count uint64
	err = chConn.QueryRow(context.Background(), "SELECT count() FROM users WHERE id = '99'").Scan(&count)
	if err != nil {
		log.Printf("Verification Query Failed: %v", err)
		return
	}
	if count == 0 {
		fmt.Println("SUCCESS: The record was inserted and then deleted correctly!")
	} else {
		fmt.Printf("FAILED: Record still exists (%d rows). Check worker logs for 'DELETE DETECTED' message.\n", count)
	}
}
