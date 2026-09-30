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
		{"control characters as backQuote spells them", "a\x00\b\t\n\f\rb", "`a" + `\0\b\t\n\f\r` + "b`"},
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

func TestIntegerType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in     string
		want   IntType
		wantOK bool
	}{
		{"UInt8", "UInt8", true},
		{"UInt16", "UInt16", true},
		{"UInt32", "UInt32", true},
		{"UInt64", "UInt64", true},
		{"UInt128", "UInt128", true},
		{"UInt256", "UInt256", true},
		{"Int8", "Int8", true},
		{"Int16", "Int16", true},
		{"Int32", "Int32", true},
		{"Int64", "Int64", true},
		{"Int128", "Int128", true},
		{"Int256", "Int256", true},
		{"Nullable(UInt64)", "UInt64", true},
		{"LowCardinality(UInt32)", "UInt32", true},
		{"LowCardinality(Nullable(Int64))", "Int64", true},
		{"Nullable(Int256)", "Int256", true},

		{"String", "", false},
		{"LowCardinality(String)", "", false},
		{"Nullable(String)", "", false},
		{"Bool", "", false},
		{"Nullable(Bool)", "", false},
		{"Decimal(18, 4)", "", false},
		{"Decimal64(4)", "", false},
		{"Float64", "", false},
		{"Enum8('a' = 1, 'b' = 2)", "", false},
		{"Enum16('x' = 1)", "", false},
		{"UUID", "", false},
		{"Date", "", false},
		{"DateTime", "", false},
		{"DateTime64(3, 'UTC')", "", false},
		{"IPv4", "", false},
		{"Array(UInt64)", "", false},
		{"Map(String, UInt64)", "", false},
		{"Tuple(UInt64)", "", false},
		{"FixedString(8)", "", false},
		// Not ClickHouse's canonical spelling, so not recognised: the column
		// keeps the plain {p:String} form rather than a guessed one.
		{"uint64", "", false},
		{"BIGINT UNSIGNED", "", false},
		{"Nullable(UInt64", "", false},
		{"Nullable()", "", false},
		{"", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, ok := IntegerType(tt.in)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("IntegerType(%q) = (%q, %v), want (%q, %v)", tt.in, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestStrictInt(t *testing.T) {
	t.Parallel()
	got := StrictInt("p3", "UInt64")
	want := "if(toString(accurateCastOrNull({p3:String}, 'UInt64')) = {p3:String}, accurateCastOrNull({p3:String}, 'UInt64'), NULL)"
	if got != want {
		t.Errorf("StrictInt = %q\nwant        %q", got, want)
	}
}
