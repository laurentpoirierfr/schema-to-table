package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

// InsertStatements renders the INSERT statements loading one JSON document
// into the model's tables: root first, then children depth-first in plan
// order (parents before children, so FKs resolve).
func (m *Model) InsertStatements(headerValues map[string]string, data string) ([]string, error) {
	stmts, err := m.insert(false, headerValues, data, "")
	return stmts, err
}

// UpsertStatements renders an atomic upsert batch for one document:
//
//  1. DELETE every descendant row bound to this document (deepest first)
//  2. INSERT ... ON CONFLICT (root PK) DO UPDATE   (root row)
//  3. INSERT every child row (parent first)
//
// Arrays have no natural key, so children use replace semantics
// scoped to this document's root key. conflictColumn must equal the
// root PK.
func (m *Model) UpsertStatements(headerValues map[string]string, data string, conflictColumn string) ([]string, error) {
	return m.insert(true, headerValues, data, conflictColumn)
}

func (m *Model) insert(upsert bool, headerValues map[string]string, data string, conflictColumn string) ([]string, error) {
	if upsert && sanitizeIdent(conflictColumn) != m.Root.PK {
		return nil, fmt.Errorf("UpsertStatements: conflict column %q must be the root primary key %q", conflictColumn, m.Root.PK)
	}
	var dataNode interface{}
	if err := json.Unmarshal([]byte(data), &dataNode); err != nil {
		return nil, fmt.Errorf("insert/model: invalid data: %w", err)
	}

	// Root row values (payload + headers).
	rootVals, err := m.gatherRowValues(m.Root, dataNode, headerValues)
	if err != nil {
		return nil, err
	}
	rootLit := rootVals[m.Root.PK]
	if rootLit == "" || rootLit == "NULL" {
		// Absent from the payload, collectValues leaves a NULL literal.
		return nil, fmt.Errorf("insert/model: root primary key %q not found in payload", m.Root.PK)
	}
	rootKey := map[string]string{m.Root.PK: rootLit}

	var stmts []string

	if upsert {
		// 1. Replaces: deletes deepest-first, scoped to this document.
		var deletes []string
		if err := m.collectDeletes(m.Root, dataNode, rootKey, &deletes); err != nil {
			return nil, err
		}
		stmts = append(stmts, deletes...)

		// 2. Root upsert.
		cols, lits, err := m.rowColumnsLits(m.Root, rootVals, nil, 0)
		if err != nil {
			return nil, err
		}
		var updates []string
		for _, c := range m.Root.Columns {
			if c.Name == m.Root.PK {
				continue
			}
			updates = append(updates, fmt.Sprintf("    %s = EXCLUDED.%s", quoteIdent(c.Name), quoteIdent(c.Name)))
		}
		if len(updates) == 0 {
			return nil, fmt.Errorf("insert/model: no columns to update besides root key")
		}
		stmts = append(stmts, fmt.Sprintf("INSERT INTO %s (%s)\nVALUES (%s)\nON CONFLICT (%s) DO UPDATE SET\n%s;\n",
			quoteIdent(m.Root.Name), strings.Join(cols, ", "), strings.Join(lits, ", "),
			quoteIdent(m.Root.PK), strings.Join(updates, ",\n")))
	} else {
		cols, lits, err := m.rowColumnsLits(m.Root, rootVals, nil, 0)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, renderInsert(m.Root.Name, cols, lits))
	}

	// 3. Child inserts (parents before children).
	if err := m.renderChildren(m.Root, dataNode, rootKey, &stmts); err != nil {
		return nil, err
	}
	return stmts, nil
}

// gatherRowValues collects root/child row payload values keyed by planned
// column name, plus ingestion headers for the root.
func (m *Model) gatherRowValues(t *TableSpec, dataNode interface{}, headers map[string]string) (map[string]string, error) {
	values := map[string]string{}
	if t == m.Root {
		for k, v := range headers {
			values["header_"+k] = quoteLiteral(v)
		}
	}
	if err := collectValues("", t.Schema, m.root, dataNode, "TEXT", values, map[*jsonSchema]bool{}); err != nil {
		return nil, fmt.Errorf("gather row %q: %w", t.Name, err)
	}
	return values, nil
}

// rowColumnsLits renders a row's column list and aligned literal list.
// parentKeyLits carries the parent row's key literal values (children);
// rowNo is this row's row_no (children). Root rows also emit the
// ingestion header columns (sorted, matching createTableStmt).
func (m *Model) rowColumnsLits(t *TableSpec, values map[string]string, parentKeyLits []string, rowNo int) ([]string, []string, error) {
	var cols, lits []string
	add := func(name, lit string) {
		cols = append(cols, quoteIdent(name))
		lits = append(lits, lit)
	}

	if t == m.Root {
		for _, k := range sortedKeys(m.headers) {
			name := "header_" + k
			add(name, values[name])
		}
	}
	if t != m.Root {
		for i, fc := range t.FkCols {
			lit := "NULL"
			if parentKeyLits != nil && i < len(parentKeyLits) {
				lit = parentKeyLits[i]
			}
			add(fc, lit)
		}
		add(t.RowNoCol, fmt.Sprintf("%d", rowNo))
	}

	for _, c := range t.Columns {
		// FK/row_no already emitted (child key columns are part of
		// Columns after reorderChildColumns).
		dup := false
		if t != m.Root {
			if c.Name == t.RowNoCol {
				dup = true
			}
			for _, fc := range t.FkCols {
				if c.Name == fc {
					dup = true
					break
				}
			}
		}
		if dup {
			continue
		}
		lit, ok := values[c.Name]
		if !ok {
			lit = "NULL"
		}
		add(c.Name, lit)
	}
	return cols, lits, nil
}

// collectDeletes walks the payload and records a DELETE per child table,
// rooted on the document's key so only this document's rows are replaced.
// Deletes are emitted deepest-first (grandchildren before parents) so the
// FK hierarchy is satisfied without cascades.
func (m *Model) collectDeletes(t *TableSpec, subject interface{}, rootKey map[string]string, out *[]string) error {
	var walk func(t *TableSpec, subject interface{}, parentKeyLits []string) error
	walk = func(t *TableSpec, subject interface{}, parentKeyLits []string) error {
		for _, child := range t.Arrays {
			arr := navigatePath(subject, child.Path)
			if arr == nil {
				continue
			}
			list, ok := arr.([]interface{})
			if !ok {
				return fmt.Errorf("collectDeletes: expected array at %q", strings.Join(child.Path, "."))
			}
			for i, item := range list {
				itemKeyLits := append(append([]string{}, parentKeyLits...), fmt.Sprintf("%d", i+1))
				if err := walk(child, item, itemKeyLits); err != nil {
					return err
				}
			}
			// This parent row's DELETE (after descendants).
			if stmt, err := childDeleteStmt(child, parentKeyLits); err == nil {
				*out = append(*out, stmt)
			}
		}
		return nil
	}
	return walk(t, subject, []string{rootKey[m.Root.PK]})
}

// childDeleteStmt builds "DELETE FROM child WHERE fkcol_i = keylit_i …",
// scoped to the parent row's key literals.
func childDeleteStmt(child *TableSpec, parentKeyLits []string) (string, error) {
	if len(child.FkCols) == 0 {
		return "", fmt.Errorf("child %q has no FK columns", child.Name)
	}
	var conds []string
	for i, fc := range child.FkCols {
		if i == len(parentKeyLits) {
			return "", fmt.Errorf("child %q: not enough FK literals", child.Name)
		}
		conds = append(conds, fmt.Sprintf("%s = %s", quoteIdent(fc), parentKeyLits[i]))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;\n", quoteIdent(child.Name), strings.Join(conds, " AND ")), nil
}

// renderChildren walks every array of the row and emits child INSERTs
// (recursing into grandchildren), parents before children.
func (m *Model) renderChildren(t *TableSpec, subject interface{}, rootKey map[string]string, out *[]string) error {
	parentKeyLits := []string{rootKey[m.Root.PK]}
	if t != m.Root {
		parentKeyLits = nil // rebuilt by caller recursion for grandchildren
	}
	var walk func(t *TableSpec, subject interface{}, pkl []string) error
	walk = func(t *TableSpec, subject interface{}, pkl []string) error {
		for _, child := range t.Arrays {
			arr := navigatePath(subject, child.Path)
			if arr == nil {
				continue
			}
			list, ok := arr.([]interface{})
			if !ok {
				return fmt.Errorf("renderChildren: expected array at %q", strings.Join(child.Path, "."))
			}

			for i, item := range list {
				var (
					itemCols, itemLits []string
					err                error
				)
				if child.valueCol != "" {
					// Array of scalars: one row per scalar in the value column.
					itemCols, itemLits, err = m.rowColumnsLits(child, map[string]string{
						child.valueCol: formatLiteral(item, child.Schema),
					}, pkl, i+1)
				} else {
					itemVals, gerr := m.gatherRowValues(child, item, nil)
					if gerr != nil {
						return gerr
					}
					itemCols, itemLits, err = m.rowColumnsLits(child, itemVals, pkl, i+1)
				}
				if err != nil {
					return err
				}
				*out = append(*out, renderInsert(child.Name, itemCols, itemLits))

				// The child's own key literal = FK values + row_no.
				childKeyLits := append(append([]string{}, pkl...), fmt.Sprintf("%d", i+1))
				if err := walk(child, item, childKeyLits); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(t, subject, parentKeyLits)
}

func navigatePath(v interface{}, path []string) interface{} {
	for _, seg := range path {
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil
		}
		// Path segments are stored snake_case; the payload keys keep
		// their original JSON casing. Match the exact key first, then
		// the key whose snake_case form equals the segment.
		key := ""
		if _, exists := m[seg]; exists {
			key = seg
		} else {
			for k := range m {
				if toSnake(k) == seg {
					key = k
					break
				}
			}
		}
		if key == "" {
			return nil
		}
		v = m[key]
	}
	return v
}

func renderInsert(table string, cols, lits []string) string {
	return fmt.Sprintf("INSERT INTO %s (%s)\nVALUES (%s);\n",
		quoteIdent(table), strings.Join(cols, ", "), strings.Join(lits, ", "))
}
