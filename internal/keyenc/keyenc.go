// Package keyenc is the one escaping composite WaveHouse keys are built
// from: NATS subject tokens and cache namespace tokens. A field keeps ASCII
// letters, digits and '_' as they are and writes every other byte as %XX
// (uppercase hex), so no separator, wildcard, whitespace, brace or non-ASCII
// byte ever appears in it unescaped, and any table name ClickHouse accepts
// encodes.
//
// The output is pinned byte for byte: NATS subjects have carried it since
// v0.1.0, and queued messages outlive the binary that wrote them.
package keyenc

import (
	"errors"
	"fmt"
	"strings"
)

const upperHex = "0123456789ABCDEF"

// kept reports whether b is written as itself.
func kept(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
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

// ErrBadEscape is a '%' not followed by two hex digits.
var ErrBadEscape = errors.New("keyenc: malformed escape")

// Unescape reverses Escape. It decodes %XX in either hex case and takes any
// other byte as itself — what url.PathUnescape accepts — so a field some
// other writer left partly unescaped still reads.
func Unescape(s string) (string, error) {
	i := strings.IndexByte(s, '%')
	if i < 0 {
		return s, nil
	}
	out := make([]byte, 0, len(s))
	out = append(out, s[:i]...)
	for ; i < len(s); i++ {
		if s[i] != '%' {
			out = append(out, s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", fmt.Errorf("%w in %q", ErrBadEscape, s)
		}
		hi, ok1 := unhex(s[i+1])
		lo, ok2 := unhex(s[i+2])
		if !ok1 || !ok2 {
			return "", fmt.Errorf("%w in %q", ErrBadEscape, s)
		}
		out = append(out, hi<<4|lo)
		i += 2
	}
	return string(out), nil
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// Join escapes each field and joins them with sep. It panics if sep is a
// byte Escape keeps, since a key split on it could then not be told apart.
func Join(sep byte, fields ...string) string {
	return string(AppendJoin(nil, sep, fields...))
}

// AppendJoin appends Join(sep, fields...) to dst.
func AppendJoin(dst []byte, sep byte, fields ...string) []byte {
	if kept(sep) || sep == '%' {
		panic(fmt.Sprintf("keyenc: %q cannot separate fields", sep))
	}
	for i, f := range fields {
		if i > 0 {
			dst = append(dst, sep)
		}
		dst = AppendEscape(dst, f)
	}
	return dst
}

// Split reverses Join: the fields of key, each unescaped.
func Split(key string, sep byte) ([]string, error) {
	parts := strings.Split(key, string(sep))
	for i, p := range parts {
		f, err := Unescape(p)
		if err != nil {
			return nil, err
		}
		parts[i] = f
	}
	return parts, nil
}
