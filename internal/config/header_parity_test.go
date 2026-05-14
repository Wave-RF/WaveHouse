package config_test

// External test package so the cross-parser check can import the
// observability package without dragging the OTel SDK into the
// production import graph of internal/config — see the doc on
// validateOTelHeaders for the dependency-graph rationale.

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// TestHeaderParsers_StayInSync pins config.validateOTelHeaders and
// observability.ParseOTelHeaders to the same accept/reject decisions on a
// shared corpus of inputs. The two parsers are hand-mirrored — config can't
// import observability in production without pulling the OTel SDK into every
// config consumer — and the runtime double-parse in cmd/wavehouse/main.go
// only flags drift that *rejects* a previously-valid input. Drift in the
// other direction (config silently accepts what the SDK parser rejects, or
// vice versa) reaches production as misconfigured auth. This test makes
// either kind of drift a CI failure.
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

			// Compare on the accept/reject boolean — error wording is allowed
			// to differ between the two parsers, but their decisions must not.
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
