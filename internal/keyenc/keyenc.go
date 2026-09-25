// Package keyenc is the one escaping composite WaveHouse keys are built
// from: NATS subject tokens, cache namespace tokens and dedupe keys. A field
// keeps ASCII letters, digits, '_' and '-' as they are and writes every other
// byte as %XX (uppercase hex), so no separator, wildcard, whitespace, brace
// or non-ASCII byte ever appears in it unescaped, and any table name
// ClickHouse accepts encodes. The bytes it keeps are exactly a tenant id's
// (tenant.Parse), so a tenant id is its own escaped form.
//
// Keys built from it are stored — queued under NATS subjects, held in caches
// — so a change to what it keeps orphans them. Earlier builds escaped '-' as
// %2D; Unescape still reads that form.
package keyenc

import (
	"fmt"
	"net/url"
	"strings"
)

const upperHex = "0123456789ABCDEF"

// kept reports whether b is written as itself.
func kept(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '-'
}

// Escape encodes s as one field.
func Escape(s string) string {
	for i := 0; i < len(s); i++ {
		if !kept(s[i]) {
			return string(AppendEscape(make([]byte, 0, len(s)+2*(len(s)-i)), s))
		}
	}
	return s
}

// AppendEscape appends Escape(s) to dst.
func AppendEscape(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if kept(b) {
			dst = append(dst, b)
		} else {
			dst = append(dst, '%', upperHex[b>>4], upperHex[b&0x0F])
		}
	}
	return dst
}

// Unescape reverses Escape. It is url.PathUnescape: %XX in either hex case
// decodes, and any other byte reads as itself, so a field another writer
// left partly unescaped — an earlier build's %2D included — still reads.
func Unescape(s string) (string, error) {
	return url.PathUnescape(s)
}

// checkSep panics unless sep can separate escaped fields: a byte Escape never
// writes, and ASCII, so the key stays valid UTF-8.
func checkSep(sep byte) {
	if kept(sep) || sep == '%' || sep >= 0x80 {
		panic(fmt.Sprintf("keyenc: %q cannot separate fields", sep))
	}
}

// Join escapes each field and joins them with sep. It panics on no fields,
// whose key would be one empty field's, and on a separator Escape could
// write.
func Join(sep byte, fields ...string) string {
	return string(AppendJoin(nil, sep, fields...))
}

// AppendJoin appends Join(sep, fields...) to dst.
func AppendJoin(dst []byte, sep byte, fields ...string) []byte {
	checkSep(sep)
	if len(fields) == 0 {
		panic("keyenc: Join needs at least one field")
	}
	for i, f := range fields {
		if i > 0 {
			dst = append(dst, sep)
		}
		dst = AppendEscape(dst, f)
	}
	return dst
}

// Split reverses Join: the fields of key, each unescaped. It panics on a
// separator Join would refuse.
func Split(key string, sep byte) ([]string, error) {
	checkSep(sep)
	parts := strings.Split(key, string([]byte{sep}))
	for i, p := range parts {
		f, err := Unescape(p)
		if err != nil {
			return nil, err
		}
		parts[i] = f
	}
	return parts, nil
}
