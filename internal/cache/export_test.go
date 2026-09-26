package cache

// Len counts the unexpired entries l holds, for the conformance suite's
// Options.Entries: no Lookup reads the key a zero snapshot would land under.
func (l *LocalCache) Len() int {
	n := 0
	l.cache.IterValues(func([]byte) bool {
		n++
		return false
	})
	return n
}
