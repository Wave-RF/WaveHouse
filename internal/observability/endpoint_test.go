package observability

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemeURL(t *testing.T) {
	// schemeURL only normalizes a bare host:port into an http:// URL so the
	// SDK's WithEndpointURL accepts it; scheme→TLS selection and path stripping
	// are the SDK's job (covered end-to-end by the integration tests).
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "bare host port gets http prefix", in: "otlp.example.com:4317", want: "http://otlp.example.com:4317"},
		{name: "ipv4 bare gets http prefix", in: "127.0.0.1:4317", want: "http://127.0.0.1:4317"},
		{name: "ipv6 bare gets http prefix", in: "[::1]:4317", want: "http://[::1]:4317"},
		{name: "http scheme passes through", in: "http://otlp.example.com:4317", want: "http://otlp.example.com:4317"},
		{name: "https scheme passes through", in: "https://otlp.example.com:443", want: "https://otlp.example.com:443"},
		{name: "https with path passes through", in: "https://otlp-gateway.example.com/otlp", want: "https://otlp-gateway.example.com/otlp"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, schemeURL(tc.in))
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
		{name: "duplicate key rejected", in: "authorization=t1,authorization=t2", wantErr: true},
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
