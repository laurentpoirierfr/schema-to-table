package service

import (
	"fmt"
	"strings"
)

// Column is one planned SQL column: Name is the final (sanitized)
// identifier, Type the SQL type (or, once rendered, the literal is kept
// separately by the value stage).
type Column struct {
	Name string
	Type string
}

// Table is a parsed JSON Schema bound to a complex-type mapping,
// ready to run every later stage (plan columns, render DDL/DML).
type Table struct {
	root        *jsonSchema
	complexType string
}

// New parses schemaDoc (JSON Schema draft 2020-12) and returns the
// pipeline entry point. complexType is the SQL type used for anything
// that can't become a fixed column — arrays and untyped/generic objects
// (e.g. "JSONB", "TEXT", "VARCHAR(8000)")…; "" defaults to "TEXT".
func New(schemaDoc string, complexType string) (*Table, error) {
	root, err := parseSchema(schemaDoc)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(complexType) == "" {
		complexType = "TEXT"
	}
	return &Table{root: root, complexType: complexType}, nil
}

// Columns runs the column-planning stage: it flattens every leaf property
// into a column named after its full path (levels joined by "_",
// camelCase → snake_case), resolves $ref/allOf, and unions oneOf/anyOf
// branches (root polymorphism included) so every variant's fields land as
// columns — a given row simply leaves the other variants' columns NULL.
//
// headers describes extra ingestion columns that aren't part of the JSON
// payload itself (transport/metadata fields): each key becomes a column
// named "header_<key>", using the map value verbatim as its SQL type.
// Columns are returned in deterministic order (headers first, sorted).
func (t *Table) Columns(headers map[string]string) ([]Column, error) {
	var cols []Column
	seen := map[string]bool{}

	addColumn := func(name, typ string) {
		name = sanitizeIdent(name)
		if seen[name] {
			return // already added, e.g. a field shared across oneOf variants
		}
		seen[name] = true
		cols = append(cols, Column{Name: name, Type: typ})
	}

	for _, k := range sortedKeys(headers) {
		addColumn("header_"+k, headers[k])
	}

	if err := walkSchema("", t.root, t.root, t.complexType, addColumn, map[*jsonSchema]bool{}); err != nil {
		return nil, fmt.Errorf("plan columns: %w", err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("plan columns: schema produced no columns")
	}
	return cols, nil
}

// walkSchema (stage 2) recursively flattens s, located at the given
// underscore-joined prefix, into columns via addColumn.
// visited is a per-path cycle guard: entries are added on the way down
// and removed on the way back up, so the same $defs entry can
// legitimately be reused at different paths.
func walkSchema(prefix string, s, root *jsonSchema, complexType string, addColumn func(name, typ string), visited map[*jsonSchema]bool) error {
	if s == nil {
		return nil
	}

	resolved, err := resolveRef(s, root)
	if err != nil {
		return err
	}
	if visited[resolved] {
		return nil // cycle guard
	}
	visited[resolved] = true
	defer delete(visited, resolved)

	// allOf: merge every branch at the same prefix (common base schema).
	for _, sub := range resolved.AllOf {
		if err := walkSchema(prefix, sub, root, complexType, addColumn, visited); err != nil {
			return err
		}
	}

	// oneOf/anyOf: polymorphism, root-level included. A landing table has
	// one row shape, so we take the union of every variant's columns;
	// addColumn already dedupes fields variants share.
	for _, sub := range append(append([]*jsonSchema{}, resolved.OneOf...), resolved.AnyOf...) {
		if err := walkSchema(prefix, sub, root, complexType, addColumn, visited); err != nil {
			return err
		}
	}

	if len(resolved.Properties) == 0 {
		return nil
	}

	for _, key := range sortedKeys(resolved.Properties) {
		prop := resolved.Properties[key]
		segment := toSnake(key)
		childPrefix := segment
		if prefix != "" {
			childPrefix = prefix + "_" + segment
		}

		propResolved, err := resolveRef(prop, root)
		if err != nil {
			return err
		}

		switch {
		case propResolved.Items != nil || primaryType(propResolved) == "array":
			// A nested collection can't become fixed columns; it lands
			// as complexType for downstream reshaping (or a child table).
			addColumn(childPrefix, complexType)
		case len(propResolved.Properties) > 0 || len(propResolved.AllOf) > 0 ||
			len(propResolved.OneOf) > 0 || len(propResolved.AnyOf) > 0:
			if err := walkSchema(childPrefix, propResolved, root, complexType, addColumn, visited); err != nil {
				return err
			}
		default:
			addColumn(childPrefix, sqlType(propResolved, complexType))
		}
	}

	return nil
}
