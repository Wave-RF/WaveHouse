package chsql

import "testing"

func TestQuoteString(t *testing.T) {
	tests := []struct{ in, want string }{
		{"acme", "'acme'"},
		{"a'b", "'a''b'"},
		{"a\\b", "'a\\\\b'"},
		{"org_id = '1'", "'org_id = ''1'''"},
	}
	for _, tt := range tests {
		if got := QuoteString(tt.in); got != tt.want {
			t.Errorf("QuoteString(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
