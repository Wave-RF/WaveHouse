// Package tenant defines the tenant identifier: the token that selects a
// settings folder, and with it the tables, policies, and pipes a request or
// an event belongs to (#583). It imports nothing from the rest of the
// repository, so every package can name a tenant without a cycle.
package tenant

import (
	"errors"
	"fmt"
	"strings"
)

// ID is a validated tenant identifier. It is a string, never a number: ids
// issued upstream can be 19 digits long, which already round as a float64.
type ID string

const (
	// Default is the reserved tenant every request without a tenant header
	// resolves to, and the only tenant of a flat settings directory.
	Default ID = "0"

	// Header carries the tenant id on a request. Absent means Default.
	Header = "X-Tenant-ID"

	// MaxLen caps an id's length in bytes.
	MaxLen = 64
)

// reserved are the names that fit the grammar and are no tenant's, in any
// letter case: the entries data_dir keeps for itself beside the tenants' own
// directories (data_dir/<tenant>, #583 story 7) — the embedded queue's nats
// and the earlier layout's pebble dedupe store — so no tenant's directory is
// ever one of those. Any letter case, because the path a name becomes is
// only as case-sensitive as the filesystem under data_dir (macOS and Windows
// are not, by default) and the grammar is ASCII, so lowercasing is exact.
// Data-directory conventions, but named here: the grammar is the one check
// every path a tenant is named on goes through.
var reserved = map[string]string{
	"nats":   "the embedded queue's directory",
	"pebble": "the earlier layout's dedupe store",
}

// Parse validates s against the one grammar an id must satisfy to be safe
// both as a folder name and as a message-queue subject token: ASCII letters,
// digits, '_' and '-', at most MaxLen bytes, and not a reserved name in any
// letter case. Dots, slashes, spaces, and wildcards are rejected because
// each means something to one of the two.
func Parse(s string) (ID, error) {
	if s == "" {
		return "", errors.New("tenant id is empty")
	}
	if len(s) > MaxLen {
		return "", fmt.Errorf("tenant id is %d bytes, the limit is %d", len(s), MaxLen)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return "", fmt.Errorf("tenant id has %q at byte %d: only letters, digits, '_' and '-' are allowed", c, i)
		}
	}
	if what, ok := reserved[strings.ToLower(s)]; ok {
		return "", fmt.Errorf("tenant id %q is reserved: data_dir/%s is %s", s, strings.ToLower(s), what)
	}
	return ID(s), nil
}

func (id ID) String() string { return string(id) }
