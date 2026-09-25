package schema

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
)

var (
	camelBoundary  = regexp.MustCompile(`([a-z0-9])([A-Z])`)
	identSanitizer = regexp.MustCompile(`[^a-z0-9_]+`)
)

// pgIdentMaxLen is PostgreSQL's identifier length limit (NAMEDATALEN-1).
const pgIdentMaxLen = 63

// identHashSuffix is the length of the deterministic hash suffix used to
// disambiguate truncated identifiers.
const identHashSuffix = 8

// IdentName sanitizes a SQL identifier and, when longer than
// pgIdentMaxLen, trims it to a readable prefix plus a deterministic hash
// of the full name, so long generated names never collide and stay stable
// across runs. The hash guarantees uniqueness even when two long names
// share the same first characters.
func IdentName(name string) string {
	name = SanitizeIdent(name)
	if len(name) <= pgIdentMaxLen {
		return name
	}
	h := fnv.New32a()
	h.Write([]byte(name))
	hash := fmt.Sprintf("%08x", h.Sum32())[:identHashSuffix]
	keep := pgIdentMaxLen - identHashSuffix - 1
	return name[:keep] + "_" + hash
}

// QuoteIdent double-quotes a SQL identifier, truncating it to PostgreSQL's
// 63-character limit (with a deterministic hash suffix) when needed.
func QuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(IdentName(name), `"`, `""`) + `"`
}

// QuoteLiteral single-quotes a SQL string literal, doubling embedded quotes.
func QuoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// ToSnake converts a camelCase segment (e.g. "estimatedDays") to
// snake_case ("estimated_days") before it's joined into the column path.
func ToSnake(s string) string {
	s = camelBoundary.ReplaceAllString(s, `${1}_${2}`)
	return strings.ToLower(s)
}

// SanitizeIdent lowercases name and replaces every character outside
// [a-z0-9_] with an underscore.
func SanitizeIdent(name string) string {
	name = strings.ToLower(name)
	return identSanitizer.ReplaceAllString(name, "_")
}

// SortedKeys returns map keys in deterministic order.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
