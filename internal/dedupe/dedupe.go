package dedupe

import "context"

// Deduplicator checks whether an event has been seen before and marks it.
type Deduplicator interface {
	// CheckAndMark returns true if the event was already seen (duplicate).
	// If not seen, it atomically marks the event as seen. The key is namespaced
	// by table so equal id values in different tables never collide (#222).
	CheckAndMark(ctx context.Context, table, eventID string) (isDuplicate bool, err error)

	Stats() map[string]int64

	// Close releases resources held by the deduplicator.
	Close() error
}
