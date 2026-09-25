package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Node is a minimal, permissive representation of a JSON Schema
// node, sufficient to walk properties/allOf/oneOf/anyOf/$ref.
type Node struct {
	Type        json.RawMessage  `json:"type,omitempty"`
	Ref         string           `json:"$ref,omitempty"`
	Format      string           `json:"format,omitempty"`
	Const       json.RawMessage  `json:"const,omitempty"`
	Title       string           `json:"title,omitempty"`
	Description string           `json:"description,omitempty"`
	Properties  map[string]*Node `json:"properties,omitempty"`
	Items       *Node            `json:"items,omitempty"`
	AllOf       []*Node          `json:"allOf,omitempty"`
	OneOf       []*Node          `json:"oneOf,omitempty"`
	AnyOf       []*Node          `json:"anyOf,omitempty"`
	Defs        map[string]*Node `json:"$defs,omitempty"`
}

// Parse unmarshals a JSON Schema document into the internal node
// representation used by every later stage.
func Parse(doc string) (*Node, error) {
	if strings.TrimSpace(doc) == "" {
		return nil, fmt.Errorf("Parse: schema document is required")
	}
	var root Node
	if err := json.Unmarshal([]byte(doc), &root); err != nil {
		return nil, fmt.Errorf("Parse: invalid schema: %w", err)
	}
	return &root, nil
}

// Resolve follows a single-level local $ref (e.g. "#/$defs/name") into
// root's $defs. Non-local or unresolvable refs are reported as errors
// rather than silently ignored.
func Resolve(s, root *Node) (*Node, error) {
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
