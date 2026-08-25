package wavehouse_test

import (
	"context"
	"fmt"
	"log"

	wavehouse "github.com/Wave-RF/WaveHouse/clients/go"
)

// ExampleNewClient demonstrates creating an unauthenticated client and
// performing a health check. The Output assertion is omitted because the
// example needs a running server.
func ExampleNewClient() {
	client := wavehouse.NewClient(wavehouse.Config{
		BaseURL: "http://localhost:8080",
	})

	// Health check — returns nil when the server is reachable.
	if err := client.Sys.Health(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func ExampleNewClient_withAuth() {
	_ = wavehouse.NewClient(wavehouse.Config{
		BaseURL: "http://localhost:8080",
		Auth:    wavehouse.StaticToken("my-jwt-token"),
	})
}

func ExampleClient_From() {
	client := wavehouse.NewClient(wavehouse.Config{
		BaseURL: "http://localhost:8080",
	})

	// Insert a row.
	_, _ = client.From("clicks").Insert(context.Background(), map[string]any{
		"page":   "/home",
		"button": "cta",
	})

	// Query with the builder.
	page, err := client.From("clicks").
		Select("page", "button").
		Where("page", wavehouse.OpEq, "/home").
		OrderBy("page", "asc").
		Limit(10).
		FetchUntyped(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range page.Data {
		fmt.Println(row["page"])
	}
}

func ExampleSQL() {
	client := wavehouse.NewClient(wavehouse.Config{
		BaseURL: "http://localhost:8080",
		Auth:    wavehouse.StaticToken("admin-token"),
	})

	rows, err := wavehouse.SQL[map[string]any](
		context.Background(), client,
		"SELECT page, count() as views FROM clicks GROUP BY page LIMIT 5",
	)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Println(row["page"], row["views"])
	}
}
