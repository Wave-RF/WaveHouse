package units

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDuration_TextRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "5s", want: 5 * time.Second},
		{in: "500ms", want: 500 * time.Millisecond},
		{in: "1m30s", want: 90 * time.Second},
		{in: "1.5s", want: 1500 * time.Millisecond},
		{in: "  2s  ", want: 2 * time.Second}, // trimmed
		{in: "", want: 0},                     // blank = zero, no error
		{in: "5", wantErr: true},              // missing unit
		{in: "nonsense", wantErr: true},
	}
	for _, tt := range tests {
		var d Duration
		err := d.UnmarshalText([]byte(tt.in))
		if tt.wantErr {
			if err == nil {
				t.Errorf("UnmarshalText(%q): expected error, got %v", tt.in, d.Std())
			}
			continue
		}
		if err != nil {
			t.Errorf("UnmarshalText(%q): unexpected error %v", tt.in, err)
			continue
		}
		if d.Std() != tt.want {
			t.Errorf("UnmarshalText(%q) = %v, want %v", tt.in, d.Std(), tt.want)
		}
	}
}

func TestByteSize_TextRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "4GiB", want: 4 << 30},
		{in: "4GB", want: 4 << 30},   // RAM semantics: GB == GiB
		{in: "2 GiB", want: 2 << 30}, // space tolerated
		{in: "512MiB", want: 512 << 20},
		{in: "4294967296", want: 4 << 30}, // plain integer = bytes
		{in: "", want: 0},                 // blank = zero, no error
		{in: "GiB", wantErr: true},
		{in: "nonsense", wantErr: true},
	}
	for _, tt := range tests {
		var b ByteSize
		err := b.UnmarshalText([]byte(tt.in))
		if tt.wantErr {
			if err == nil {
				t.Errorf("UnmarshalText(%q): expected error, got %d", tt.in, b.Bytes())
			}
			continue
		}
		if err != nil {
			t.Errorf("UnmarshalText(%q): unexpected error %v", tt.in, err)
			continue
		}
		if b.Bytes() != tt.want {
			t.Errorf("UnmarshalText(%q) = %d, want %d", tt.in, b.Bytes(), tt.want)
		}
	}
}

// TestScalars_AcrossEncodings proves the single Text-method pair serves JSON,
// YAML, and (by extension) cleanenv — and that a round trip preserves the
// human-readable form. A bare number decodes via the underlying int64 kind.
func TestScalars_AcrossEncodings(t *testing.T) {
	t.Parallel()

	type doc struct {
		T Duration `json:"t" yaml:"t"`
		M ByteSize `json:"m" yaml:"m"`
	}

	t.Run("json string form", func(t *testing.T) {
		t.Parallel()
		var d doc
		if err := json.Unmarshal([]byte(`{"t":"5s","m":"4GiB"}`), &d); err != nil {
			t.Fatal(err)
		}
		if d.T.Std() != 5*time.Second || d.M.Bytes() != 4<<30 {
			t.Fatalf("got T=%v M=%d", d.T.Std(), d.M.Bytes())
		}
		out, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != `{"t":"5s","m":"4GiB"}` {
			t.Errorf("json round trip = %s", out)
		}
	})

	t.Run("json rejects a bare number", func(t *testing.T) {
		t.Parallel()
		// encoding/json only routes string scalars through TextUnmarshaler, so a
		// bare number errors — the policy API must use the quoted, readable form.
		var d doc
		if err := json.Unmarshal([]byte(`{"t":5000000000,"m":4294967296}`), &d); err == nil {
			t.Fatal("expected error unmarshalling bare JSON numbers, got nil")
		}
	})

	t.Run("yaml string or bare number", func(t *testing.T) {
		t.Parallel()
		var d doc
		if err := yaml.Unmarshal([]byte("t: 1m30s\nm: 512MiB\n"), &d); err != nil {
			t.Fatal(err)
		}
		if d.T.Std() != 90*time.Second || d.M.Bytes() != 512<<20 {
			t.Fatalf("string form: got T=%v M=%d", d.T.Std(), d.M.Bytes())
		}
		// yaml.v3 hands a bare scalar's text to UnmarshalText, so a number works too.
		var n doc
		if err := yaml.Unmarshal([]byte("t: 2s\nm: 4294967296\n"), &n); err != nil {
			t.Fatal(err)
		}
		if n.M.Bytes() != 4<<30 {
			t.Fatalf("bare-number yaml: got M=%d", n.M.Bytes())
		}
	})
}
