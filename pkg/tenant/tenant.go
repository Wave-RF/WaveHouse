// Package tenant defines the tenant identifier: the token that selects a
// settings folder, and with it the tables, policies, and pipes a request or
// an event belongs to (#583). It imports nothing from the rest of the
// repository, so every package can name a tenant without a cycle.
package tenant

import (
	"errors"
	"fmt"
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

// Parse validates s against the one grammar an id must satisfy to be safe
// both as a folder name and as a message-queue subject token: ASCII letters,
// digits, '_' and '-', at most MaxLen bytes. Dots, slashes, spaces, and
// wildcards are rejected because each means something to one of the two.
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
	return ID(s), nil
}

func (id ID) String() string { return string(id) }
