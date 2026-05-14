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
		// IPv6 cases pin the bracketed-literal behavior end-to-end. The
		// current implementation handles them correctly via the path-strip
		// loop, but a future refactor to net/url would need to preserve the
		// brackets — these cases would catch any regression there.
		{name: "ipv6 bare", in: "[::1]:4317", wantHost: "[::1]:4317"},
		{name: "ipv6 https", in: "https://[::1]:4317", wantHost: "[::1]:4317", wantTLS: true},
		{name: "ipv6 https with path", in: "https://[::1]:4317/otlp", wantHost: "[::1]:4317", wantTLS: true},
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
		{name: "empty value rejected", in: "k=", wantErr: true},
		{name: "whitespace-only value rejected", in: "authorization=   ", wantErr: true},
		{name: "space in key rejected", in: "my key=v", wantErr: true},
		{name: "colon in key rejected", in: "x:y=v", wantErr: true},
		{name: "non-ascii key rejected", in: "x-héader=v", wantErr: true},
		{name: "mixed-case key accepted", in: "Authorization=Bearer x", want: map[string]string{"Authorization": "Bearer x"}},
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
