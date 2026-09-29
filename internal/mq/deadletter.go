package mq

// deadLetterTables counts a dead-letter stream's parked messages per table,
// from its per-subject counts under prefix. Every scope of a table counts
// under the table itself, so no table + scope pair can share a count with a
// dotted table name. A non-empty table keeps that table alone, all of its
// scopes included.
func deadLetterTables(subjects map[string]uint64, prefix, table string) map[string]uint64 {
	tables := make(map[string]uint64, len(subjects))
	for subj, n := range subjects {
		t := parseTopicKey(topicKey(prefix, subj))
		if table != "" && t.Table != table {
			continue
		}
		tables[t.Table] += n
	}
	return tables
}
