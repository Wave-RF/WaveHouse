package mq

import "context"

// DeleteDurable deletes durable from every tenant's ingest stream, as an
// operator could underneath a running consumer.
func DeleteDurable(ctx context.Context, e *EmbeddedNATS, durable string) error {
	e.mu.Lock()
	ids := e.ingestTenants()
	e.mu.Unlock()
	for _, id := range ids {
		if err := e.js.DeleteConsumer(ctx, ingestStreamName(id), durable); err != nil {
			return err
		}
	}
	return nil
}
