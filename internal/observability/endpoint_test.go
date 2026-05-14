package observability

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		wantHost string
		wantTLS  bool
	}{
		{name: "bare host port", in: "otlp.example.com:4317", wantHost: "otlp.example.com:4317"},
		{name: "http scheme", in: "http://otlp.example.com:4317", wantHost: "otlp.example.com:4317"},
		{name: "https scheme", in: "https://otlp.example.com:443", wantHost: "otlp.example.com:443", wantTLS: true},
		{name: "https with path", in: "https://otlp-gateway.example.com/otlp", wantHost: "otlp-gateway.example.com", wantTLS: true},
		{name: "https with port and path", in: "https://otlp.example.com:443/v1/traces", wantHost: "otlp.example.com:443", wantTLS: true},
		{name: "empty", in: "", wantHost: ""},
		{name: "ipv4 plaintext", in: "127.0.0.1:4317", wantHost: "127.0.0.1:4317"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotHost, gotTLS := ParseEndpoint(tc.in)
			require.Equal(t, tc.wantHost, gotHost)
			require.Equal(t, tc.wantTLS, gotTLS)
		})
	}
}

func TestParseOTelHeaders(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    map[string]string
		wantErr bool
	}{
		{name: "empty", in: "", want: map[string]string{}},
		{name: "whitespace only", in: "   ", want: map[string]string{}},
		{name: "one pair", in: "x-honeycomb-team=abc123", want: map[string]string{"x-honeycomb-team": "abc123"}},
		{name: "two pairs", in: "a=1,b=2", want: map[string]string{"a": "1", "b": "2"}},
		{name: "whitespace around segments", in: "  a = 1 ,  b = 2  ", want: map[string]string{"a": "1", "b": "2"}},
		{name: "value contains equals", in: "authorization=Basic dXNlcjpwYXNz==", want: map[string]string{"authorization": "Basic dXNlcjpwYXNz=="}},
		{name: "trailing comma tolerated", in: "a=1,", want: map[string]string{"a": "1"}},
		{name: "missing equals", in: "not-a-pair", wantErr: true},
		{name: "empty key", in: "=value", wantErr: true},
		{name: "empty value allowed", in: "k=", want: map[string]string{"k": ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseOTelHeaders(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
