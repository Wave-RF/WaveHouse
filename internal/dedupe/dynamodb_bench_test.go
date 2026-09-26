//go:build dynamobench

// Manual latency benchmark for the DynamoDB backend, never run by CI. Point it
// at an existing table (the credentials and region come from the SDK chain):
//
//	DEDUPE_BENCH_TABLE=wavehouse-dedupe-dev go test -tags dynamobench \
//	  -run '^$' -bench Dynamo -benchtime 2000x ./internal/dedupe/
//
// DEDUPE_BENCH_ENDPOINT=http://localhost:8000 runs it against dynamodb-local
// instead, creating the table there.
package dedupe

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var benchSeq atomic.Uint64

func benchDynamo(b *testing.B) *Managed {
	b.Helper()
	cfg := DynamoConfig{Table: os.Getenv("DEDUPE_BENCH_TABLE"), Endpoint: os.Getenv("DEDUPE_BENCH_ENDPOINT")}
	if cfg.Table == "" {
		b.Skip("DEDUPE_BENCH_TABLE is not set")
	}
	if cfg.Endpoint != "" {
		cfg.Timeout = 5 * time.Second // dynamodb-local is far slower than the service
	}
	d, err := NewDynamo(b.Context(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	if cfg.Endpoint != "" {
		if err := d.CreateTable(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	if err := d.Check(b.Context()); err != nil {
		b.Fatal(err)
	}
	m := d.Tenant("bench")
	if err := m.Apply(true); err != nil {
		b.Fatal(err)
	}
	return m
}

// benchKeys are n ids no run has used, with a short retention so the table
// forgets them.
func benchKeys(n int) []Key {
	run := time.Now().UnixNano()
	out := make([]Key, n)
	for i := range out {
		out[i] = Key{Table: "bench", ID: fmt.Sprintf("%d-%d", run, benchSeq.Add(1))}
	}
	return out
}

func benchReserveCommit(b *testing.B, window int) {
	m := benchDynamo(b)
	b.ResetTimer()
	for b.Loop() {
		claims, err := m.Reserve(b.Context(), benchKeys(window), DefaultLease)
		if err != nil {
			b.Fatal(err)
		}
		if err := m.Commit(b.Context(), claims, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDynamo_ReserveCommit1(b *testing.B)   { benchReserveCommit(b, 1) }
func BenchmarkDynamo_ReserveCommit256(b *testing.B) { benchReserveCommit(b, 256) }
