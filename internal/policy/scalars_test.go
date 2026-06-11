package policy

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// TestMillis_FlexibleInput pins the parse-friendly-in / number-out contract for
// the duration cap: a string ("10s") or a bare number (milliseconds) on input,
// the canonical millisecond integer on output.
func TestMillis_FlexibleInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		json    string
		want    Millis
		wantErr bool
	}{
		{name: "duration string seconds", json: `"10s"`, want: 10_000},
		{name: "duration string ms", json: `"500ms"`, want: 500},
		{name: "duration string compound", json: `"1m30s"`, want: 90_000},
		{name: "bare number is milliseconds", json: `5000`, want: 5000},
		{name: "unitless string is milliseconds", json: `"5000"`, want: 5000},
		{name: "empty string is zero", json: `""`, want: 0},
		{name: "sub-millisecond is rejected", json: `"500us"`, wantErr: true},
		{name: "garbage string is rejected", json: `"nonsense"`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var m Millis
			err := json.Unmarshal([]byte(tt.json), &m)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got %v", tt.json, m)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", tt.json, err)
			}
			if m != tt.want {
				t.Errorf("Millis(%s) = %d, want %d", tt.json, m, tt.want)
			}
			// Output is always the canonical number, never a string, so SDKs
			// consume a plain int.
			out, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(out); got[0] == '"' {
				t.Errorf("Millis marshalled as a string %s, want a number", got)
			}
		})
	}
}

func TestByteSize_FlexibleInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		json    string
		want    ByteSize
		wantErr bool
	}{
		{name: "IEC binary", json: `"4GiB"`, want: 4 << 30},
		{name: "SI decimal differs from IEC", json: `"4GB"`, want: 4_000_000_000},
		{name: "mebibytes", json: `"512MiB"`, want: 512 << 20},
		{name: "bare number is bytes", json: `4294967296`, want: 4 << 30},
		{name: "unitless string is bytes", json: `"4294967296"`, want: 4 << 30},
		{name: "empty string is zero", json: `""`, want: 0},
		{name: "garbage is rejected", json: `"nonsense"`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var b ByteSize
			err := json.Unmarshal([]byte(tt.json), &b)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got %d", tt.json, b)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", tt.json, err)
			}
			if b != tt.want {
				t.Errorf("ByteSize(%s) = %d, want %d", tt.json, b, tt.want)
			}
			out, err := json.Marshal(b)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(out); got[0] == '"' {
				t.Errorf("ByteSize marshalled as a string %s, want a number", got)
			}
		})
	}
}

// TestScalars_YAML confirms the YAML bootstrap path accepts the same string or
// bare-number forms (yaml.v3 hands the scalar text to UnmarshalYAML either way).
func TestScalars_YAML(t *testing.T) {
	t.Parallel()
	type doc struct {
		T Millis   `yaml:"t"`
		M ByteSize `yaml:"m"`
	}
	var d doc
	if err := yaml.Unmarshal([]byte("t: 10s\nm: 4GiB\n"), &d); err != nil {
		t.Fatal(err)
	}
	if d.T.Duration() != 10*time.Second || d.M.Bytes() != 4<<30 {
		t.Fatalf("string form: T=%v M=%d", d.T.Duration(), d.M.Bytes())
	}
	var n doc
	if err := yaml.Unmarshal([]byte("t: 5000\nm: 4294967296\n"), &n); err != nil {
		t.Fatal(err)
	}
	if n.T != 5000 || n.M.Bytes() != 4<<30 {
		t.Fatalf("bare-number form: T=%d M=%d", n.T, n.M.Bytes())
	}
}
