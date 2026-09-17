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

// TestEscapeStringParam pins the encoding both {p:String} surfaces depend on.
// The single quote and '%'/'_' are deliberately NOT escaped: the value is read
// as an escaped FIELD, not as a quoted literal and not as a LIKE pattern, so
// encoding them would bind characters the caller never wrote.
func TestEscapeStringParam(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "acme", "acme"},
		{"empty", "", ""},
		{"backslash", `a\b`, `a\\b`},
		{"trailing backslash", `trail\`, `trail\\`},
		{"tab", "a\tb", `a\tb`},
		{"newline", "a\nb", `a\nb`},
		{"carriage return", "a\rb", `a\rb`},
		{"single quote is untouched", "O'Brien", "O'Brien"},
		{"like metacharacters are untouched", "50%_off", "50%_off"},
		{"nul is untouched", "a\x00b", "a\x00b"},
		{"backslash then t is not re-read as a tab", `a\tb`, `a\\tb`},
		{"every byte at once", "a\\\tb\nc\rd", `a\\\tb\nc\rd`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := EscapeStringParam(tt.in); got != tt.want {
				t.Errorf("EscapeStringParam(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
