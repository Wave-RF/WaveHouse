package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"gopkg.in/yaml.v3"
)

// Millis and ByteSize are the human-friendly-in / number-out value types for the
// policy's resource caps. On the way IN (a config file or a hand-crafted API
// body), they accept either a readable string ("10s", "4GiB") or a bare number
// in the canonical unit. On the way OUT (GET /v1/admin/policy and any read-back)
// they marshal as that bare number — they implement no Marshaler, so the default
// integer encoding applies — so SDKs consume a plain int and never reimplement
// the humanization. The canonical units are milliseconds (time) and bytes
// (memory); ClickHouse receives those numbers directly, so its own size/duration
// syntax never leaks to policy authors.

// Millis is a duration stored as whole milliseconds. Input accepts a Go duration
// string ("10s", "500ms", "1m30s") or a bare integer count of milliseconds.
type Millis int64

// Duration returns the value as a time.Duration.
func (m Millis) Duration() time.Duration { return time.Duration(m) * time.Millisecond }

func (m *Millis) parse(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		*m = 0
		return nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		// A unitless string is taken as milliseconds, mirroring the bare-number form.
		if n, convErr := strconv.ParseInt(s, 10, 64); convErr == nil {
			*m = Millis(n)
			return nil
		}
		return fmt.Errorf("invalid duration %q (use %q, %q, or a number of milliseconds): %w", s, "5s", "500ms", err)
	}
	ms := d.Milliseconds()
	if d > 0 && ms == 0 {
		// Fail closed: a positive-but-sub-millisecond cap must not round to 0
		// (which would read as "no limit").
		return fmt.Errorf("duration %q is below the 1ms resolution of a resource cap", s)
	}
	*m = Millis(ms)
	return nil
}

// UnmarshalJSON accepts a duration string or a bare millisecond count.
func (m *Millis) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		return m.parse(s)
	}
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("max_execution_time must be a duration string (%q) or a number of milliseconds: %w", "5s", err)
	}
	*m = Millis(n)
	return nil
}

// UnmarshalYAML accepts either a string or a bare numeric scalar (yaml.v3 hands
// the scalar's text to us either way).
func (m *Millis) UnmarshalYAML(value *yaml.Node) error { return m.parse(value.Value) }

// ByteSize is a byte count. Input accepts a size string ("4GiB", "512MiB", with
// SI vs IEC distinguished — "4GB" is 4×10⁹, "4GiB" is 4×2³⁰) or a bare integer
// count of bytes.
type ByteSize int64

// Bytes returns the value as a raw byte count.
func (b ByteSize) Bytes() int64 { return int64(b) }

func (b *ByteSize) parse(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		*b = 0
		return nil
	}
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return fmt.Errorf("invalid byte size %q (use %q, %q, or a number of bytes): %w", s, "512MiB", "4GiB", err)
	}
	if v > math.MaxInt64 {
		return fmt.Errorf("byte size %q is too large", s)
	}
	*b = ByteSize(v)
	return nil
}

// UnmarshalJSON accepts a size string or a bare byte count.
func (b *ByteSize) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		return b.parse(s)
	}
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("max_memory_usage must be a size string (%q) or a number of bytes: %w", "4GiB", err)
	}
	*b = ByteSize(n)
	return nil
}

// UnmarshalYAML accepts either a string or a bare numeric scalar.
func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error { return b.parse(value.Value) }
