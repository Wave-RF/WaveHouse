// Package stream holds the SSE fan-out primitives shared by the streaming API:
// the Subscriber connection handle (subscriber.go), the Bucket fan-out primitive
// (bucket.go), and the Heartbeater keepalive wheel (heartbeat.go) that nudges
// idle GET /v1/stream connections.
//
// Issue #294 will move the delivery hot path in here too, reusing Bucket so a
// projected event is serialized once per (role, table) column-set and pushed
// through the same Subscriber queue the keepalive wheel uses today.
package stream
