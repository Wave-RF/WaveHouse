// External test package so the cross-parser check can import observability
// without dragging the OTel SDK into the production import graph of
// internal/config (see validateOTelHeaders).
package config_test

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// TestHeaderParsers_StayInSync pins config.validateOTelHeaders and
// observability.ParseOTelHeaders to identical accept/reject decisions. The
// runtime double-parse in main.go only catches one direction (config-accepts
// / SDK-rejects); this test catches the other direction at CI time.
func TestHeaderParsers_StayInSync(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "empty", in: "", wantErr: false},
		{name: "whitespace only", in: "   ", wantErr: false},
		{name: "one pair", in: "k=v", wantErr: false},
		{name: "two pairs", in: "k=v,k2=v2", wantErr: false},
		{name: "whitespace around segments", in: "  a = 1 ,  b = 2  ", wantErr: false},
		{name: "value contains equals (base64 padding)", in: "authorization=Basic dXNlcjpwYXNz==", wantErr: false},
		{name: "trailing comma tolerated", in: "k=v,", wantErr: false},
		{name: "missing equals", in: "not-a-pair", wantErr: true},
		{name: "empty key", in: "=value", wantErr: true},
		{name: "empty value", in: "k=", wantErr: true},
		{name: "whitespace-only value", in: "authorization=   ", wantErr: true},
		{name: "space in key", in: "my key=v", wantErr: true},
		{name: "colon in key", in: "x:y=v", wantErr: true},
		{name: "non-ascii key", in: "x-héader=v", wantErr: true},
		{name: "mixed-case key", in: "Authorization=Bearer x", wantErr: false},
		{name: "mixed valid and invalid", in: "a=1,broken", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.Config{
				Server:     config.Server{Port: 8080},
				ClickHouse: config.ClickHouse{HTTPScheme: "http"},
				Schema:     config.Schema{RefreshInterval: 60},
				OTel: config.OTel{
					Enabled: true,
					Addr:    "127.0.0.1:4317",
					Headers: tc.in,
				},
			}
			validateErr := cfg.Validate()
			_, parseErr := observability.ParseOTelHeaders(tc.in)

			// Error wording may differ; the accept/reject decision must not.
			if (validateErr != nil) != (parseErr != nil) {
				t.Fatalf("parser drift on %q: validateOTelHeaders=%v ParseOTelHeaders=%v",
					tc.in, validateErr, parseErr)
			}
			if tc.wantErr && validateErr == nil {
				t.Fatalf("expected both parsers to reject %q, both accepted", tc.in)
			}
			if !tc.wantErr && validateErr != nil {
				t.Fatalf("expected both parsers to accept %q, both rejected: %v", tc.in, validateErr)
			}
		})
	}
}
