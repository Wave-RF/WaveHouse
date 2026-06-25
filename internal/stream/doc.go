// Package stream holds the SSE fan-out primitives shared by the streaming API:
// the keepalive wheel that nudges idle GET /v1/stream connections, and the
// reusable Bucket fan-out it's built on. Each abstraction lives in its own file —
// the Subscriber connection handle (subscriber.go), the Bucket fan-out primitive
// (bucket.go), and the Heartbeater keepalive wheel (heartbeat.go).
//
// The delivery-path throughput work (#294) will move the broadcast hub in here
// too and reuse Bucket so projection + serialization run once per (role, table)
// column-set instead of once per subscriber, routing the projected event frames
// through the same Subscriber outbound queue the keepalive wheel uses today.
package stream
