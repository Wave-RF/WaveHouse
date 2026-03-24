package dedupe

import "context"

// Deduplicator checks whether an event has been seen before and marks it.
// Keys are scoped by tenant: {tenant_id}:{event_id} to enable future
// per-tenant deletion and TTL policies.
type Deduplicator interface {
	// CheckAndMark returns true if the event was already seen (duplicate).
	// If not seen, it atomically marks the event as seen.
	CheckAndMark(ctx context.Context, tenantID, eventID string) (isDuplicate bool, err error)

	// Close releases resources held by the deduplicator.
	Close() error
}
