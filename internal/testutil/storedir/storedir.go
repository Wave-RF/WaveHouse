// Package storedir gives a test a directory for the embedded message broker's
// store. It imports nothing from the repository, so internal/mq's own tests can
// use it.
package storedir

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// maxRemovals bounds the store's removal: a consumer-state write still under way
// when the broker closes adds at most two entries after it — its temporary file,
// then the rename into place — so a removal is refilled at most twice per
// durable. Past this many, something is writing that Close did not stop.
const maxRemovals = 32

// New returns an empty directory under t.TempDir for a broker's store, removed
// once the broker is closed: New's cleanup runs after the test's own, Close
// among them (cleanups run last-in, first-out).
//
// A closed broker's store is not yet quiescent: the embedded NATS server writes
// each durable consumer's state from a goroutine its Shutdown does not join, so
// a write under way can land after Close has returned — failing t.TempDir's
// one-shot RemoveAll with "directory not empty" (#442). There is nothing to
// wait on, so the removal is tried again whenever a directory was refilled
// between reading and removing it. That ends without a clock: those writes add a
// bounded number of entries, and none once their directory is gone.
func New(t testing.TB) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("store directory: %v", err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		for i := 1; i < maxRemovals && errors.Is(err, syscall.ENOTEMPTY); i++ {
			err = os.RemoveAll(dir)
		}
		if err != nil {
			t.Errorf("remove the store: %v", err)
		}
	})
	return dir
}
