package coord

import "fmt"

// Revoke ends name's live term under this coordinator as a lost lease,
// which a local lease never is: it lets the shared suite and RunElected's
// tests drive the loss path through the real implementation.
func (l *Local) Revoke(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for t := range l.terms {
		if t.name == name {
			l.endLocked(t, fmt.Errorf("%w: revoked", ErrLost))
		}
	}
}
