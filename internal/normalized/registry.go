package normalized

import (
	"fmt"
	"strings"

	"github.com/laurentpoirierfr/schema-to-table/internal/schema"
)

// RegistryStatements renders the schema registry: a small table that maps
// every logical object (root/child tables and denormalized views) to its
// final SQL identifier. Because long generated names are truncated to
// PostgreSQL's 63-character limit (with a deterministic hash suffix), this
// registry preserves the mapping back to the original JSON path and carries
// a human description from the schema's title/description.
//
// The registry table is named "<root>_registry" (e.g. "landing_order_registry");
// identifiers are emitted through the same truncation as the objects they
// describe, so the documented names match the DDL exactly.
func (m *Model) RegistryStatements() ([]string, error) {
	registry := m.Root.Name + "_registry"
	cols := []schema.Column{
		{Name: "object_type", Type: "TEXT"},
		{Name: "logical_name", Type: "TEXT"},
		{Name: "sql_name", Type: "TEXT"},
		{Name: "json_path", Type: "TEXT"},
		{Name: "description", Type: "TEXT"},
	}

	var rows []registryRow
	// Root table.
	rows = append(rows, registryRow{
		Type:    "table",
		Logical: m.Root.Name,
		Path:    "$",
		Desc:    schemaDesc(m.root),
	})
	// Child tables, depth-first.
	for _, t := range m.Tables {
		if t == m.Root {
			continue
		}
		rows = append(rows, registryRow{
			Type:    "table",
			Logical: t.Name,
			Path:    "$" + jsonPath(t.Path),
			Desc:    t.Desc,
		})
	}
	// Denormalized views.
	for _, child := range m.Root.Arrays {
		rows = append(rows, registryRow{
			Type:    "view",
			Logical: m.viewName(child),
			Path:    "$" + jsonPath(child.Path),
			Desc:    "denormalized view for array " + strings.Join(child.Path, " → "),
		})
	}

	var lines []string
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("    (%s, %s, %s, %s, %s)",
			schema.QuoteLiteral(r.Type),
			schema.QuoteLiteral(r.Logical),
			schema.QuoteLiteral(schema.IdentName(r.Logical)),
			schema.QuoteLiteral(r.Path),
			schema.QuoteLiteral(r.Desc)))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", schema.QuoteIdent(registry))
	for i, c := range cols {
		sep := ","
		if i == len(cols)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "    %s %s%s\n", schema.QuoteIdent(c.Name), c.Type, sep)
	}
	b.WriteString(");\n")
	if len(lines) > 0 {
		b.WriteString("INSERT INTO " + schema.QuoteIdent(registry) + " (object_type, logical_name, sql_name, json_path, description) VALUES\n")
		b.WriteString(strings.Join(lines, ",\n"))
		b.WriteString(";\n")
	}
	return []string{b.String()}, nil
}

type registryRow struct {
	Type    string
	Logical string
	Path    string
	Desc    string
}

// jsonPath renders a JSON-path-like string for a column path, e.g.
// []string{"packages","items"} → "/packages/items".
func jsonPath(path []string) string {
	return "/" + strings.Join(path, "/")
}

// schemaDesc extracts the best human description from a schema node:
// title, then description, then "".
func schemaDesc(s *schema.Node) string {
	if s == nil {
		return ""
	}
	if s.Title != "" {
		return s.Title
	}
	if s.Description != "" {
		return s.Description
	}
	return ""
}
