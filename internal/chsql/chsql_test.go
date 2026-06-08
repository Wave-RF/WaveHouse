package chsql

import "testing"

func TestQuoteIdent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "page", "`page`"},
		{"keyword", "select", "`select`"},
		{"reserved word that breaks bare", "all", "`all`"},
		{"dotted", "user.id", "`user.id`"},
		{"space", "gross $", "`gross $`"},
		{"unicode", "naïve", "`naïve`"},
		{"embedded backtick", "tick`col", "`tick\\`col`"},
		{"embedded backslash", `back\slash`, "`back\\\\slash`"},
		{"backslash then backtick", "x\\`y", "`x\\\\\\`y`"},
		{"star is just a name here", "*", "`*`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := QuoteIdent(tt.in); got != tt.want {
				t.Errorf("QuoteIdent(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestBindUnsafe(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"a?b", "?", "weird?col"} {
		if !BindUnsafe(s) {
			t.Errorf("BindUnsafe(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"page", "user.id", "naïve", "tick`col", ""} {
		if BindUnsafe(s) {
			t.Errorf("BindUnsafe(%q) = true, want false", s)
		}
	}
}
