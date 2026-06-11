// Package units provides config/policy scalar types that (un)marshal in
// human-readable form across JSON, YAML, and environment variables, so the same
// value reads the same everywhere it is configured.
//
// Both types implement only encoding.TextMarshaler / TextUnmarshaler. That one
// pair is enough for every surface WaveHouse parses config from: encoding/json
// and gopkg.in/yaml.v3 both route a string scalar through the Text methods, and
// ilyakaznacheev/cleanenv uses TextUnmarshaler for env values and env-default
// tags. In JSON the value must therefore be a quoted string ("5s", "4GiB") — a
// bare JSON number is rejected, which is fine since the readable string form is
// exactly what these types exist to encourage. In YAML either a string or a
// bare scalar works (yaml.v3 hands the scalar's text to UnmarshalText either
// way), so "5s" and 5000000000 are both accepted there.
package units

import (
	"fmt"
	"strings"
	"time"

	gounits "github.com/docker/go-units"
)

// Duration is a time.Duration rendered as a Go duration string ("5s", "500ms",
// "1m30s", "1.5s") rather than the raw nanosecond integer time.Duration would
// otherwise produce in JSON. It mirrors the format already used by
// clickhouse.query_timeout in the server config.
type Duration time.Duration

// Std returns the value as a standard time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalText parses a Go duration string. An empty value is treated as zero
// (no limit) so an explicitly-blank field doesn't fail closed.
func (d *Duration) UnmarshalText(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q (use a unit, e.g. %q or %q): %w", s, "5s", "500ms", err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// ByteSize is a byte count rendered in human-readable binary form ("4GiB",
// "512MiB"). Parsing uses go-units' RAM semantics, so "2G", "2GB", "2GiB", and
// "2 GiB" all mean 2*1024³ bytes — the way operators think about a memory cap —
// and a plain integer is taken as a raw byte count.
type ByteSize int64

// Bytes returns the value as a raw byte count.
func (b ByteSize) Bytes() int64 { return int64(b) }

func (b ByteSize) String() string { return gounits.BytesSize(float64(b)) }

// UnmarshalText parses a human-readable byte size. An empty value is treated as
// zero (no limit).
func (b *ByteSize) UnmarshalText(t []byte) error {
	s := strings.TrimSpace(string(t))
	if s == "" {
		*b = 0
		return nil
	}
	v, err := gounits.RAMInBytes(s)
	if err != nil {
		return fmt.Errorf("invalid byte size %q (e.g. %q or %q): %w", s, "512MiB", "4GiB", err)
	}
	*b = ByteSize(v)
	return nil
}

func (b ByteSize) MarshalText() ([]byte, error) { return []byte(b.String()), nil }
