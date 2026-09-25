package landing

import (
	"fmt"
	"strings"

	"github.com/laurentpoirierfr/schema-to-table/internal/schema"
)

// Insert renders an "INSERT INTO" statement for tableName, targeting
// exactly the columns Columns would plan for the same schema.
//
// headerValues holds the actual values for the extra ingestion columns
// (each becomes "header_<key>", values quoted as string literals).
// data is the JSON payload; a schema column absent from the payload (e.g.
// a field belonging to the non-matching oneOf variant) comes out NULL.
// Arrays and generic objects are serialized to JSON text for the
// complexType column, with an explicit "::type" cast when it looks like a
// JSON type.
func (t *Table) Insert(tableName string, headerValues map[string]string, data string) (string, error) {
	if strings.TrimSpace(tableName) == "" {
		return "", fmt.Errorf("Insert: tableName is required")
	}
	cols, lits, err := t.renderValues(headerValues, data)
	if err != nil {
		return "", err
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = schema.QuoteIdent(c)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (%s)\nVALUES (%s);\n",
		schema.QuoteIdent(tableName), strings.Join(quoted, ", "), strings.Join(lits, ", "))
	return b.String(), nil
}

// Upsert renders an "INSERT … ON CONFLICT (…) DO UPDATE" statement: same
// shape as Insert, plus a upsert on conflictColumn (a business key such as
// "id" or "header_source"). conflictColumn must be one of the generated
// columns; on conflict every other column is overwritten with the new
// value (EXCLUDED.<column>), conflictColumn itself is left untouched.
func (t *Table) Upsert(tableName string, headerValues map[string]string, data string, conflictColumn string) (string, error) {
	if strings.TrimSpace(tableName) == "" {
		return "", fmt.Errorf("Upsert: tableName is required")
	}
	if strings.TrimSpace(conflictColumn) == "" {
		return "", fmt.Errorf("Upsert: conflictColumn is required")
	}
	conflictColumn = schema.SanitizeIdent(conflictColumn)

	cols, lits, err := t.renderValues(headerValues, data)
	if err != nil {
		return "", err
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = schema.QuoteIdent(c)
	}

	found := false
	var updates []string
	for _, c := range cols {
		if c == conflictColumn {
			found = true
			continue
		}
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", schema.QuoteIdent(c), schema.QuoteIdent(c)))
	}
	if !found {
		return "", fmt.Errorf("Upsert: conflict column %q is not among the generated columns", conflictColumn)
	}
	if len(updates) == 0 {
		return "", fmt.Errorf("Upsert: no columns left to update besides conflict column %q", conflictColumn)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (%s)\nVALUES (%s)\nON CONFLICT (%s) DO UPDATE SET\n    %s;\n",
		schema.QuoteIdent(tableName), strings.Join(quoted, ", "), strings.Join(lits, ", "),
		schema.QuoteIdent(conflictColumn), strings.Join(updates, ",\n    "))
	return b.String(), nil
}

// renderValues runs the plan + value stages together: it plans the column
// order from the schema, collects the payload's literals for those
// columns, and returns aligned column-name and literal slices (missing
// values → NULL).
func (t *Table) renderValues(headerValues map[string]string, data string) (cols, lits []string, err error) {
	// Header columns always come first, sorted — same as the plan stage.
	headerKeys := schema.SortedKeys(headerValues)
	planned, err := t.Columns(headerTypeShim(headerKeys))
	if err != nil {
		return nil, nil, err
	}

	dataNode, err := schema.ParsePayload(data)
	if err != nil {
		return nil, nil, fmt.Errorf("render values: %w", err)
	}

	values := map[string]string{}
	for _, k := range headerKeys {
		values[schema.SanitizeIdent("header_"+k)] = schema.QuoteLiteral(headerValues[k])
	}
	if err := schema.CollectValues("", t.root, t.root, dataNode, t.complexType, values, map[*schema.Node]bool{}); err != nil {
		return nil, nil, fmt.Errorf("render values: %w", err)
	}

	cols = make([]string, 0, len(planned))
	lits = make([]string, 0, len(planned))
	for _, c := range planned {
		cols = append(cols, c.Name)
		lit, ok := values[c.Name]
		if !ok {
			lit = "NULL"
		}
		lits = append(lits, lit)
	}
	return cols, lits, nil
}

// headerTypeShim builds a headers map with placeholder SQL types: the
// plan stage only needs the header *keys* (values are the actual
// literals, provided by headerValues).
func headerTypeShim(keys []string) map[string]string {
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		m[k] = "TEXT"
	}
	return m
}
