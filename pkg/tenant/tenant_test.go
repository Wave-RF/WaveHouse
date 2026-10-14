package tenant

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "default", in: "0"},
		{name: "letters digits underscore dash", in: "Acme_co-42"},
		{name: "19 digit id", in: "9223372036854775807"},
		{name: "at the length cap", in: strings.Repeat("a", MaxLen)},
		{name: "empty", in: "", wantErr: true},
		{name: "over the length cap", in: strings.Repeat("a", MaxLen+1), wantErr: true},
		{name: "dot", in: "a.b", wantErr: true},
		{name: "parent directory", in: "..", wantErr: true},
		{name: "slash", in: "a/b", wantErr: true},
		{name: "backslash", in: `a\b`, wantErr: true},
		{name: "space", in: "a b", wantErr: true},
		{name: "subject wildcard star", in: "*", wantErr: true},
		{name: "subject wildcard tail", in: ">", wantErr: true},
		{name: "non-ascii letter", in: "ténant", wantErr: true},
		{name: "newline", in: "a\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q) = %q, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.in, err)
			}
			if got.String() != tt.in {
				t.Errorf("Parse(%q) = %q", tt.in, got)
			}
		})
	}
}

func TestDefaultSatisfiesTheGrammar(t *testing.T) {
	if _, err := Parse(Default.String()); err != nil {
		t.Errorf("Default %q fails its own grammar: %v", Default, err)
	}
}
