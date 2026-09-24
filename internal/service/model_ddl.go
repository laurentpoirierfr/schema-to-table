package service

import (
	"fmt"
	"strings"
)

// CreateStatements renders the DDL for every planned table, in execution
// order: root first, then children (each parent before its children, so
// FKs resolve when executed sequentially).
func (m *Model) CreateStatements() ([]string, error) {
	var stmts []string
	for _, t := range m.Tables {
		s, err := createTableStmt(t, m.headers, m.Root)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	return stmts, nil
}

// ViewStatements renders one denormalized view per direct child table, on
// the model's root: one output row per child row, combining every root
// column (headers included) with every child column. Analysts query the
// view without writing joins.
func (m *Model) ViewStatements() ([]string, error) {
	var stmts []string
	for _, child := range m.Root.Arrays {
		s, err := rootChildView(m, child)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	return stmts, nil
}

// viewName returns the logical denormalized view name for a direct child
// of the root: "v_<root>_<json-path>". The JSON path (not the possibly
// truncated table name) drives the suffix, so the identifier stays
// meaningful and stable.
func (m *Model) viewName(child *TableSpec) string {
	return "v_" + m.Root.Name + "_" + strings.Join(child.Path, "_")
}

// rootChildView renders "CREATE VIEW v_<root>_<child> AS SELECT …" for one
// direct child of the root. Child column names colliding with root columns
// are suffixed with the child property segment (e.g. "packages_sku").
func rootChildView(m *Model, child *TableSpec) (string, error) {
	segment := "child"
	if len(child.Path) > 0 {
		segment = child.Path[len(child.Path)-1]
	}

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE VIEW %s AS\nSELECT\n", quoteIdent(m.viewName(child)))

	used := map[string]bool{}
	var rootCols []string
	for _, k := range sortedKeys(m.headers) {
		rootCols = append(rootCols, "header_"+k)
	}
	for _, c := range m.Root.Columns {
		rootCols = append(rootCols, c.Name)
	}

	var lines []string
	for _, name := range rootCols {
		used[name] = true
		lines = append(lines, fmt.Sprintf("    %s.%s", quoteIdent("o"), quoteIdent(name)))
	}
	for _, c := range child.Columns {
		out := c.Name
		if used[out] {
			out = segment + "_" + out
		}
		used[out] = true
		lines = append(lines, fmt.Sprintf("    %s.%s AS %s", quoteIdent("p"), quoteIdent(c.Name), quoteIdent(out)))
	}
	b.WriteString(strings.Join(lines, ",\n"))
	b.WriteString("\n")

	// Join child → root on the child FK == root key.
	var fkConds []string
	for i, fc := range child.FkCols {
		fkConds = append(fkConds, fmt.Sprintf("%s.%s = %s.%s",
			quoteIdent("p"), quoteIdent(fc), quoteIdent("o"), quoteIdent(m.Root.KeyCols[i])))
	}
	fmt.Fprintf(&b, "FROM %s AS %s\nJOIN %s AS %s ON %s;\n",
		quoteIdent(m.Root.Name), quoteIdent("o"),
		quoteIdent(child.Name), quoteIdent("p"), strings.Join(fkConds, " AND "))
	return b.String(), nil
}

func createTableStmt(t *TableSpec, headers map[string]string, root *TableSpec) (string, error) {
	var cols []Column
	if t == root {
		// Root: ingestion headers first (sorted), then planned columns.
		for _, k := range sortedKeys(headers) {
			cols = append(cols, Column{Name: "header_" + k, Type: headers[k]})
		}
	}
	cols = append(cols, t.Columns...)

	if len(cols) == 0 {
		return "", fmt.Errorf("create table %q: no columns", t.Name)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", quoteIdent(t.Name))
	for i, c := range cols {
		sep := ","
		if i == len(cols)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "    %s %s%s\n", quoteIdent(c.Name), c.Type, sep)
	}
	b.WriteString(");\n")

	// Root PK inline (single column); children get a composite key.
	if t.PK != "" {
		fmt.Fprintf(&b, "ALTER TABLE %s ADD PRIMARY KEY (%s);\n",
			quoteIdent(t.Name), quoteIdent(t.PK))
	} else {
		keyList := make([]string, len(t.KeyCols))
		for i, k := range t.KeyCols {
			keyList[i] = quoteIdent(k)
		}
		fmt.Fprintf(&b, "ALTER TABLE %s ADD PRIMARY KEY (%s);\n",
			quoteIdent(t.Name), strings.Join(keyList, ", "))
	}

	// FK to the parent's key.
	if t.Parent != nil {
		fkList := make([]string, len(t.FkCols))
		for i, fc := range t.FkCols {
			fkList[i] = quoteIdent(fc)
		}
		refList := make([]string, len(t.Parent.KeyCols))
		for i, k := range t.Parent.KeyCols {
			refList[i] = quoteIdent(k)
		}
		fmt.Fprintf(&b, "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s (%s);\n",
			quoteIdent(t.Name), strings.Join(fkList, ", "),
			quoteIdent(t.Parent.Name), strings.Join(refList, ", "))
	}
	return b.String(), nil
}

// Descendants returns the child tables of a table in plan order.
func (t *TableSpec) Descendants() []*TableSpec { return t.Arrays }
