package query

import "regexp"

// SafeIdentifierRe is the single source of truth for validating ClickHouse
// table names and other identifiers to prevent SQL injection.
var SafeIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
