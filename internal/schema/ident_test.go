package schema

import "testing"

// Names within the 63-character limit are only sanitized, never truncated.
func TestIdentNameShort(t *testing.T) {
	got := IdentName("landing_order_packages")
	want := "landing_order_packages"
	if got != want {
		t.Errorf("IdentName(%q) = %q, want %q", "landing_order_packages", got, want)
	}
}

// Longer names are truncated to exactly 63 characters, keeping a readable
// prefix plus a deterministic hash suffix.
func TestIdentNameTruncatesTo63(t *testing.T) {
	long := "landing_order_" + repeat("extremely_long_property_name_", 4)
	got := IdentName(long)
	if len(got) != pgIdentMaxLen {
		t.Errorf("len = %d, want %d (%q)", len(got), pgIdentMaxLen, got)
	}
	// Deterministic: same input, same output across calls.
	if again := IdentName(long); again != got {
		t.Errorf("not deterministic: %q vs %q", got, again)
	}
	// Distinct long names sharing a prefix must not collide.
	other := "landing_order_" + repeat("extremely_long_property_name_", 4) + "_x"
	if IdentName(other) == got {
		t.Errorf("distinct long names collide: %q", got)
	}
	// Prefix remains readable.
	if want := "landing_order_extremely_long_property_name"; len(want) > 0 && got[:len(want)] != want {
		t.Errorf("readable prefix lost: got %q", got)
	}
}

func TestIdentNameSanitizesBeforeTruncating(t *testing.T) {
	got := IdentName("Schema-With WEIRD chars")
	// Non [a-z0-9_] chars become underscores, everything lowercase.
	if got != "schema_with_weird_chars" {
		t.Errorf("IdentName = %q", got)
	}
}

// A very long abbreviation of the previous case also stays within the limit.
func TestIdentNameSanitizesLong(t *testing.T) {
	got := IdentName("Schema-With WEIRD chars and à long name that exceeds the ninety six character limit greatly yes indeed")
	if len(got) > pgIdentMaxLen {
		t.Errorf("len = %d, want <= %d (%q)", len(got), pgIdentMaxLen, got)
	}
}

// QuoteIdent must apply the same truncation as identName, so DDL, DML and
// the registry all reference identical identifiers.
func TestQuoteIdentTruncates(t *testing.T) {
	long := "v_landing_order_" + repeat("a_very_long_array_name_", 4)
	q := QuoteIdent(long)
	if len(q) != pgIdentMaxLen+2 { // +2 for the surrounding quotes
		t.Errorf("quoted len = %d, want %d (%q)", len(q), pgIdentMaxLen+2, q)
	}
	if q[0] != '"' || q[len(q)-1] != '"' {
		t.Errorf("missing quotes: %q", q)
	}
	if q[1:len(q)-1] != IdentName(long) {
		t.Errorf("QuoteIdent does not match IdentName: %q vs %q", q, IdentName(long))
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
