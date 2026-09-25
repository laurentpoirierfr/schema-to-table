package normalized

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/laurentpoirierfr/schema-to-table/internal/schema"
)

// ---------------------------------------------------------------------------
// Normalized (fully-tabular) model.
//
// The flat landing pipeline (internal/landing) collapses arrays and generic
// objects into a single complexType column. This planner instead turns the
// whole JSON Schema into a set of *typed* tables:
//
//   - the root table keeps every flattened scalar leaf (one-to-one nested
//     objects are flattened, so analysts don't need joins for them);
//   - every array of typed objects becomes a child table linked by foreign
//     key to its parent, plus a row_no column preserving order/multiplicity;
//   - arrays of scalars become child tables (fk, row_no, value);
//   - oneOf/anyOf polymorphism is modeled as single-table inheritance
//     (STI): one table, an auto-detected discriminator column, and nullable
//     columns per variant (shared fields deduped);
//   - anything that cannot be typed (objects without declared properties,
//     arrays without typed items) is a hard plan error listing the JSON
//     path — no silent JSONB fallback.
// ---------------------------------------------------------------------------

// TableSpec is one typed relational table planned from the schema.
type TableSpec struct {
	Name        string          // SQL table name
	Columns     []schema.Column // materialized typed columns, in DDL order
	PK          string          // root: primary key column name ("" on children)
	FkCols      []string        // child: FK column names referencing parent key
	FkColTypes  []string        // child: SQL type of each FkCol
	RowNoCol    string          // child: row number column name ("row_no")
	KeyCols     []string        // full key: root [PK]; child FkCols + RowNoCol
	KeyColTypes []string        // SQL type of each KeyCol
	Path        []string        // JSON property keys from parent row to this array
	Schema      *schema.Node    // schema describing one row of this table
	Parent      *TableSpec      // owning table (nil on root)
	valueCol    string          // "value" when the child stores an array of scalars
	Desc        string          // human description of the array property (title/description)
	Arrays      []*TableSpec
}

// Model is the full set of typed tables planned from a schema, plus the
// root schema needed to render DML values.
type Model struct {
	Root          *TableSpec
	Tables        []*TableSpec
	Discriminator string
	root          *schema.Node
	headers       map[string]string
}

// Plan parses the schema document and plans the fully-tabular model.
// tableName is the root table name, pk the root business key (e.g. "id"),
// headers map header keys → SQL types (root-only ingestion columns).
func Plan(schemaDoc, tableName, pk string, headers map[string]string) (*Model, error) {
	root, err := schema.Parse(schemaDoc)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(tableName) == "" {
		return nil, fmt.Errorf("Plan: tableName is required")
	}
	if strings.TrimSpace(pk) == "" {
		return nil, fmt.Errorf("Plan: pk is required")
	}

	m := &Model{root: root, headers: headers}
	rootSpec := &TableSpec{Name: tableName, Schema: root, PK: pk, RowNoCol: "row_no"}

	// Phase 1: plan column structure across the whole schema tree
	// (arrays become child tables; FK/key metadata stays unresolved).
	if err := planNode(rootSpec, "", root, root, map[*schema.Node]bool{}); err != nil {
		return nil, err
	}

	// Flatten the tree into depth-first order (parents before children).
	m.Tables = tableOrder(rootSpec)
	m.Root = rootSpec

	// Resolve the root key type from the planned pk column.
	pkType := ""
	for _, c := range rootSpec.Columns {
		if c.Name == pk {
			pkType = c.Type
		}
	}
	if pkType == "" {
		return nil, fmt.Errorf("Plan: primary key column %q not found among planned columns", pk)
	}
	rootSpec.KeyCols = []string{pk}
	rootSpec.KeyColTypes = []string{pkType}

	// Phase 2: finalize every child — its FK references the parent's key
	// (now known), and FK/row_no become the child's leading columns.
	for _, t := range m.Tables {
		if t.Parent == nil {
			continue
		}
		t.FkCols = childFkCols(t.Parent)
		t.FkColTypes = append([]string{}, t.Parent.KeyColTypes...)
		t.KeyCols = append(append([]string{}, t.FkCols...), t.RowNoCol)
		t.KeyColTypes = append(append([]string{}, t.FkColTypes...), "INT")
		reorderChildColumns(t)
	}

	if len(root.OneOf)+len(root.AnyOf) > 0 {
		variants := append(append([]*schema.Node{}, root.OneOf...), root.AnyOf...)
		if d, err := detectDiscriminator(variants, root); err != nil {
			return nil, err
		} else if d != "" {
			m.Discriminator = d
		}
	}
	return m, nil
}

// tableOrder flattens the Arrays tree into depth-first order, each parent
// before its children (so DDL can be emitted sequentially).
func tableOrder(root *TableSpec) []*TableSpec {
	order := []*TableSpec{}
	var walk func(t *TableSpec)
	walk = func(t *TableSpec) {
		order = append(order, t)
		for _, c := range t.Arrays {
			walk(c)
		}
	}
	walk(root)
	return order
}

// reorderChildColumns moves the FK + row_no columns of a child table to the
// front, so the key is visible at the start of the row. Payload columns
// that collide by name are dropped.
func reorderChildColumns(t *TableSpec) {
	prepend := []schema.Column{}
	for i, fc := range t.FkCols {
		prepend = append(prepend, schema.Column{Name: fc, Type: t.FkColTypes[i]})
	}
	if t.RowNoCol != "" {
		prepend = append(prepend, schema.Column{Name: t.RowNoCol, Type: "INT"})
	}

	rest := []schema.Column{}
	for _, c := range t.Columns {
		dup := false
		for _, p := range prepend {
			if c.Name == p.Name {
				dup = true
				break
			}
		}
		if !dup {
			rest = append(rest, c)
		}
	}
	t.Columns = append(prepend, rest...)
}

// detectDiscriminator looks for a single property that every variant
// declares with a const value — that shared key is the STI type tag.
func detectDiscriminator(variants []*schema.Node, root *schema.Node) (string, error) {
	variantConsts := make([]map[string]string, 0, len(variants))
	for _, v := range variants {
		consts := map[string]string{}
		if err := collectConsts(v, root, consts); err != nil {
			return "", err
		}
		variantConsts = append(variantConsts, consts)
	}
	if len(variantConsts) == 0 {
		return "", nil
	}

	keys := make([]string, 0, len(variantConsts[0]))
	for k := range variantConsts[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		all := true
		for _, consts := range variantConsts {
			if _, ok := consts[key]; !ok {
				all = false
				break
			}
		}
		if all {
			return key, nil
		}
	}
	return "", nil
}

// collectConsts flattens allOf branches and records the first const value
// found for each property key.
func collectConsts(s, root *schema.Node, out map[string]string) error {
	if s == nil {
		return nil
	}
	resolved, err := schema.Resolve(s, root)
	if err != nil {
		return err
	}
	for _, sub := range resolved.AllOf {
		if err := collectConsts(sub, root, out); err != nil {
			return err
		}
	}
	for key, prop := range resolved.Properties {
		if len(prop.Const) > 0 {
			if _, exists := out[key]; !exists {
				var cst string
				if err := json.Unmarshal(prop.Const, &cst); err == nil {
					out[key] = cst
				}
			}
		}
	}
	return nil
}

// planNode recursively adds a node's scalar leaves as columns of tbl
// (flattening one-to-one nested objects) and spawns child tables for
// arrays. prefix is the underscore-joined flatten path. visited is a
// per-path cycle guard.
func planNode(tbl *TableSpec, prefix string, s, root *schema.Node, visited map[*schema.Node]bool) error {
	if s == nil {
		return nil
	}
	resolved, err := schema.Resolve(s, root)
	if err != nil {
		return err
	}
	if visited[resolved] {
		return nil
	}
	visited[resolved] = true
	defer delete(visited, resolved)

	for _, sub := range resolved.AllOf {
		if err := planNode(tbl, prefix, sub, root, visited); err != nil {
			return err
		}
	}
	for _, sub := range append(append([]*schema.Node{}, resolved.OneOf...), resolved.AnyOf...) {
		if err := planNode(tbl, prefix, sub, root, visited); err != nil {
			return err
		}
	}

	if len(resolved.Properties) == 0 {
		return nil
	}

	for _, key := range schema.SortedKeys(resolved.Properties) {
		prop := resolved.Properties[key]
		segment := schema.ToSnake(key)
		childPrefix := segment
		if prefix != "" {
			childPrefix = prefix + "_" + segment
		}

		propResolved, err := schema.Resolve(prop, root)
		if err != nil {
			return err
		}

		isArray := propResolved.Items != nil || schema.PrimaryType(propResolved) == "array"
		isObject := schema.PrimaryType(propResolved) == "object" && propResolved.Items == nil

		switch {
		case isArray:
			if err := planChildTable(tbl, childPrefix, segment, propResolved, root); err != nil {
				return err
			}
		case isObject && len(propResolved.Properties) == 0 &&
			len(propResolved.AllOf) == 0 && len(propResolved.OneOf) == 0 && len(propResolved.AnyOf) == 0:
			return fmt.Errorf("plan model: untypable object at %q: no declared properties", prefixKey(childPrefix))
		case len(propResolved.Properties) > 0 || len(propResolved.AllOf) > 0 ||
			len(propResolved.OneOf) > 0 || len(propResolved.AnyOf) > 0:
			if err := planNode(tbl, childPrefix, propResolved, root, visited); err != nil {
				return err
			}
		default:
			addModelColumn(tbl, childPrefix, schema.SQLType(propResolved, "TEXT"))
		}
	}
	return nil
}

// planChildTable builds and registers the child TableSpec for an array
// property. Arrays of objects → typed columns from items (recursively);
// arrays of scalars → (fk, row_no, value); anything untyped → hard error.
func planChildTable(parent *TableSpec, prefix, segment string, prop *schema.Node, root *schema.Node) error {
	if prop.Items == nil {
		return fmt.Errorf("plan model: untypable array at %q: missing items definition", prefixKey(prefix, segment))
	}
	itemResolved, err := schema.Resolve(prop.Items, root)
	if err != nil {
		return err
	}

	name := parent.Name + "_" + schema.SanitizeIdent(prefix)
	// The same array path can be reached through several oneOf variants
	// (shared base fields) — register the child only once.
	for _, existing := range parent.Arrays {
		if existing.Name == name {
			return nil
		}
	}

	child := &TableSpec{
		Name:     name,
		RowNoCol: "row_no",
		Path:     append(append([]string{}, parent.Path...), schema.ToSnake(segment)),
		Schema:   itemResolved,
		Parent:   parent,
		Desc:     schemaDesc(prop),
	}

	primary := schema.PrimaryType(itemResolved)
	if primary != "" && primary != "object" && primary != "array" {
		// Array of scalars → single typed value column.
		child.valueCol = "value"
		addModelColumn(child, "value", schema.SQLType(itemResolved, "TEXT"))
		parent.Arrays = append(parent.Arrays, child)
		return nil
	}

	if err := planNode(child, "", itemResolved, root, map[*schema.Node]bool{}); err != nil {
		return err
	}
	parent.Arrays = append(parent.Arrays, child)
	return nil
}

// childFkCols names the FK columns that a child uses to reference the
// parent's key columns: <parent>_<keycol>.
func childFkCols(parent *TableSpec) []string {
	cols := make([]string, len(parent.KeyCols))
	for i, k := range parent.KeyCols {
		cols[i] = parent.Name + "_" + k
	}
	return cols
}

func addModelColumn(tbl *TableSpec, name, typ string) {
	name = schema.SanitizeIdent(name)
	for _, c := range tbl.Columns {
		if c.Name == name {
			return
		}
	}
	tbl.Columns = append(tbl.Columns, schema.Column{Name: name, Type: typ})
}

func prefixKey(prefix ...string) string {
	return strings.Join(prefix, "_")
}
