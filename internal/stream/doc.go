// Package stream holds the SSE fan-out primitives shared by the streaming API:
// the Subscriber connection handle (subscriber.go), the Bucket fan-out primitive
// (bucket.go), the Heartbeater keepalive wheel (heartbeat.go) that nudges idle
// GET /v1/stream connections, and the event Hub (hub.go).
//
// The Hub is the #294 delivery hot path: subscribers register under (topic, role),
// and each event is projected — column-filtered per the role's policy — and
// serialized ONCE per role, then pushed through the same Subscriber queue the
// keepalive wheel uses, so the handler drains both from a single byte-pump. That
// collapses the prior per-subscriber unmarshal/project/re-serialize into one pass
// per distinct (role, table) output shape. The one per-subscriber decision left is
// row-level security (#319): a role whose policy carries a row-filter shares the
// column projection but delivers each event only to the subscribers whose JWT
// claims admit the row.
package stream
