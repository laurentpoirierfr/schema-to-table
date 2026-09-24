package service

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
)

// jsonSchema is a minimal, permissive representation of a JSON Schema
// node, sufficient to walk properties/allOf/oneOf/anyOf/$ref.
type jsonSchema struct {
	Type        json.RawMessage        `json:"type,omitempty"`
	Ref         string                 `json:"$ref,omitempty"`
	Format      string                 `json:"format,omitempty"`
	Const       json.RawMessage        `json:"const,omitempty"`
	Title       string                 `json:"title,omitempty"`
	Description string                 `json:"description,omitempty"`
	Properties  map[string]*jsonSchema `json:"properties,omitempty"`
	Items       *jsonSchema            `json:"items,omitempty"`
	AllOf       []*jsonSchema          `json:"allOf,omitempty"`
	OneOf       []*jsonSchema          `json:"oneOf,omitempty"`
	AnyOf       []*jsonSchema          `json:"anyOf,omitempty"`
	Defs        map[string]*jsonSchema `json:"$defs,omitempty"`
}

var (
	camelBoundary  = regexp.MustCompile(`([a-z0-9])([A-Z])`)
	identSanitizer = regexp.MustCompile(`[^a-z0-9_]+`)
)

// parseSchema unmarshals a JSON Schema document into the internal node
// representation used by every later stage.
func parseSchema(doc string) (*jsonSchema, error) {
	if strings.TrimSpace(doc) == "" {
		return nil, fmt.Errorf("parseSchema: schema document is required")
	}
	var root jsonSchema
	if err := json.Unmarshal([]byte(doc), &root); err != nil {
		return nil, fmt.Errorf("parseSchema: invalid schema: %w", err)
	}
	return &root, nil
}

// resolveRef follows a single-level local $ref (e.g. "#/$defs/name") into
// root's $defs. Non-local or unresolvable refs are reported as errors
// rather than silently ignored.
func resolveRef(s, root *jsonSchema) (*jsonSchema, error) {
	if s == nil {
		return nil, nil
	}
	if s.Ref == "" {
		return s, nil
	}
	const prefix = "#/$defs/"
	if !strings.HasPrefix(s.Ref, prefix) {
		return nil, fmt.Errorf("unsupported $ref %q (only local #/$defs/... refs are supported)", s.Ref)
	}
	name := strings.TrimPrefix(s.Ref, prefix)
	def, ok := root.Defs[name]
	if !ok {
		return nil, fmt.Errorf("$ref %q not found in $defs", s.Ref)
	}
	return def, nil
}

// primaryType returns the schema's declared type, taking the first
// non-null entry when "type" is an array (e.g. ["string","null"]).
func primaryType(s *jsonSchema) string {
	if s == nil || len(s.Type) == 0 {
		return ""
	}
	var single string
	if err := json.Unmarshal(s.Type, &single); err == nil {
		return single
	}
	var multi []string
	if err := json.Unmarshal(s.Type, &multi); err == nil {
		for _, t := range multi {
			if t != "null" {
				return t
			}
		}
	}
	return ""
}

// sqlType maps a leaf JSON Schema node to a PostgreSQL column type.
// complexType is used for a generic/untyped object (no declared properties).
func sqlType(s *jsonSchema, complexType string) string {
	switch primaryType(s) {
	case "integer":
		return "BIGINT"
	case "number":
		return "NUMERIC"
	case "boolean":
		return "BOOLEAN"
	case "object":
		return complexType // generic/untyped object
	case "string":
		switch s.Format {
		case "uuid":
			return "UUID"
		case "date-time":
			return "TIMESTAMPTZ"
		case "date":
			return "DATE"
		default:
			return "TEXT"
		}
	default:
		return "TEXT"
	}
}

// pgIdentMaxLen is PostgreSQL's identifier length limit (NAMEDATALEN-1).
const pgIdentMaxLen = 63

// identHashSuffix is the length of the deterministic hash suffix used to
// disambiguate truncated identifiers.
const identHashSuffix = 8

// identName sanitizes a SQL identifier and, when longer than
// pgIdentMaxLen, trims it to a readable prefix plus a deterministic hash
// of the full name, so long generated names never collide and stay stable
// across runs. The hash guarantees uniqueness even when two long names
// share the same first characters.
func identName(name string) string {
	name = sanitizeIdent(name)
	if len(name) <= pgIdentMaxLen {
		return name
	}
	h := fnv.New32a()
	h.Write([]byte(name))
	hash := fmt.Sprintf("%08x", h.Sum32())[:identHashSuffix]
	keep := pgIdentMaxLen - identHashSuffix - 1
	return name[:keep] + "_" + hash
}

// quoteIdent double-quotes a SQL identifier, truncating it to PostgreSQL's
// 63-character limit (with a deterministic hash suffix) when needed.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(identName(name), `"`, `""`) + `"`
}

// quoteLiteral single-quotes a SQL string literal, doubling embedded quotes.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// toSnake converts a camelCase segment (e.g. "estimatedDays") to
// snake_case ("estimated_days") before it's joined into the column path.
func toSnake(s string) string {
	s = camelBoundary.ReplaceAllString(s, `${1}_${2}`)
	return strings.ToLower(s)
}

func sanitizeIdent(name string) string {
	name = strings.ToLower(name)
	return identSanitizer.ReplaceAllString(name, "_")
}

// sortedKeys returns map keys in deterministic order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
