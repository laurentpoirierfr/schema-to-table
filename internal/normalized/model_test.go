package normalized

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The fixture schema mirrors schemas/schema.json: a oneOf root
// (standard/express) with a shared "packages" array.
const modelSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": {
    "baseOrder": {
      "type": "object",
      "properties": {
        "id": {"type": "string", "format": "uuid"},
        "customer": {
          "type": "object",
          "properties": {
            "name": {"type": "string"},
            "email": {"type": "string"}
          }
        },
        "packages": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "sku": {"type": "string"},
              "quantity": {"type": "integer"},
              "weightKg": {"type": "number"}
            }
          }
        },
        "type": {"type": "string"}
      }
    },
    "standardOrder": {
      "allOf": [
        {"$ref": "#/$defs/baseOrder"},
        {"type": "object", "properties": {"estimatedDays": {"type": "integer"},
          "type": {"const": "standard"}}}
      ]
    },
    "expressOrder": {
      "allOf": [
        {"$ref": "#/$defs/baseOrder"},
        {"type": "object", "properties": {"courierPhone": {"type": "string"},
          "type": {"const": "express"}}}
      ]
    }
  },
  "oneOf": [
    {"$ref": "#/$defs/standardOrder"},
    {"$ref": "#/$defs/expressOrder"}
  ]
}`

func planFixture(t *testing.T) *Model {
	t.Helper()
	m, err := Plan(modelSchema, "landing_order", "id", map[string]string{
		"source":      "TEXT",
		"ingested_at": "TIMESTAMPTZ",
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	return m
}

func TestModelPlanStructure(t *testing.T) {
	m := planFixture(t)

	if m.Root.Name != "landing_order" {
		t.Errorf("root name = %q", m.Root.Name)
	}
	if m.Discriminator != "type" {
		t.Errorf("discriminator = %q, want type", m.Discriminator)
	}
	if m.Root.PK != "id" {
		t.Errorf("root pk = %q", m.Root.PK)
	}
	if len(m.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(m.Tables))
	}

	// Root columns: flattened one-to-one customer, plus type union,
	// plus discriminator.
	wantRoot := []string{"customer_email", "customer_name", "id", "type", "estimated_days", "courier_phone"}
	names := make([]string, len(m.Root.Columns))
	for i, c := range m.Root.Columns {
		names[i] = c.Name
	}
	if len(wantRoot) != len(names) {
		t.Fatalf("root columns = %v, want %v", names, wantRoot)
	}
	for i, w := range wantRoot {
		if names[i] != w {
			t.Fatalf("root column[%d] = %q, want %q", i, names[i], w)
		}
	}

	// Child: one table per array, FK + row_no + typed item columns.
	child := m.Tables[1]
	if child.Name != "landing_order_packages" {
		t.Errorf("child name = %q", child.Name)
	}
	if child.Parent != m.Root {
		t.Error("child parent is not root")
	}
	if len(child.FkCols) != 1 || child.FkCols[0] != "landing_order_id" {
		t.Errorf("child fk = %v", child.FkCols)
	}
	if len(child.FkColTypes) != 1 || child.FkColTypes[0] != "UUID" {
		t.Errorf("child fk types = %v", child.FkColTypes)
	}
	if child.RowNoCol != "row_no" {
		t.Errorf("row_no col = %q", child.RowNoCol)
	}
	wantKeys := []string{"landing_order_id", "row_no"}
	if strings.Join(child.KeyCols, ",") != strings.Join(wantKeys, ",") {
		t.Errorf("child key cols = %v, want %v", child.KeyCols, wantKeys)
	}
	if child.KeyColTypes[0] != "UUID" || child.KeyColTypes[1] != "INT" {
		t.Errorf("child key types = %v", child.KeyColTypes)
	}

	// FK + row_no are leading columns.
	if child.Columns[0].Name != "landing_order_id" || child.Columns[1].Name != "row_no" {
		t.Errorf("child leading columns = %v", child.Columns)
	}
}

func TestModelViewStatementsDenormalized(t *testing.T) {
	m := planFixture(t)
	vws, err := m.ViewStatements()
	if err != nil {
		t.Fatalf("ViewStatements: %v", err)
	}
	if len(vws) != 1 {
		t.Fatalf("expected 1 view, got %d", len(vws))
	}
	v := vws[0]
	for _, want := range []string{
		`CREATE VIEW "v_landing_order_packages"`,
		`"o"."id"`,
		`"o"."customer_name"`,
		`"o"."type"`,
		`"o"."estimated_days"`,
		`"p"."row_no" AS "row_no"`,
		`"p"."sku" AS "sku"`,
		`"p"."weight_kg" AS "weight_kg"`,
		`FROM "landing_order" AS "o"`,
		`JOIN "landing_order_packages" AS "p" ON "p"."landing_order_id" = "o"."id"`,
	} {
		if !strings.Contains(v, want) {
			t.Errorf("view missing %q\n%s", want, v)
		}
	}
}

// The registry documents every table and view with its logical name, final
// SQL name, JSON path and description (from the schema title).
func TestModelRegistryStatements(t *testing.T) {
	m, err := Plan(`{
		"title": "Commande",
		"type": "object",
		"required": ["id"],
		"properties": {
			"id": { "type": "string" },
			"packages": {
				"type": "array",
				"description": "Les colis",
				"items": { "type": "object", "properties": { "sku": {"type":"string"} } }
			}
		}
	}`, "landing_order", "id", nil)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	regs, err := m.RegistryStatements()
	if err != nil {
		t.Fatalf("RegistryStatements: %v", err)
	}
	if len(regs) != 1 {
		t.Fatalf("expected 1 registry statement, got %d", len(regs))
	}
	sql := regs[0]
	for _, want := range []string{
		`CREATE TABLE "landing_order_registry"`,
		`"object_type" TEXT`,
		`"logical_name" TEXT`,
		`"sql_name" TEXT`,
		`"json_path" TEXT`,
		`"description" TEXT`,
		`('table', 'landing_order', 'landing_order', '$', 'Commande')`,
		`('table', 'landing_order_packages', 'landing_order_packages', '$/packages', 'Les colis')`,
		`('view', 'v_landing_order_packages', 'v_landing_order_packages', '$/packages', 'denormalized view for array packages')`,
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("registry missing %q\n%s", want, sql)
		}
	}
}

func TestModelCreateStatementsDDL(t *testing.T) {
	m := planFixture(t)
	stmts, err := m.CreateStatements()
	if err != nil {
		t.Fatalf("CreateStatements: %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("expected 2 DDL statements, got %d", len(stmts))
	}
	rootDDL, childDDL := stmts[0], stmts[1]

	for _, want := range []string{
		`CREATE TABLE "landing_order" (`,
		`"header_ingested_at" TIMESTAMPTZ`,
		`"header_source" TEXT`,
		`"customer_email" TEXT`,
		`ALTER TABLE "landing_order" ADD PRIMARY KEY ("id");`,
	} {
		if !strings.Contains(rootDDL, want) {
			t.Errorf("root DDL missing %q\n%s", want, rootDDL)
		}
	}

	for _, want := range []string{
		`CREATE TABLE "landing_order_packages" (`,
		`"landing_order_id" UUID`,
		`"row_no" INT`,
		`"quantity" BIGINT`,
		`"weight_kg" NUMERIC`,
		`ALTER TABLE "landing_order_packages" ADD PRIMARY KEY ("landing_order_id", "row_no");`,
		`ALTER TABLE "landing_order_packages" ADD FOREIGN KEY ("landing_order_id") REFERENCES "landing_order" ("id");`,
	} {
		if !strings.Contains(childDDL, want) {
			t.Errorf("child DDL missing %q\n%s", want, childDDL)
		}
	}
}

func TestModelInsertStatements(t *testing.T) {
	m := planFixture(t)
	data := `{
	  "id": "550e8400-e29b-41d4-a716-446655440001",
	  "type": "standard",
	  "customer": {"name": "Alice Martin", "email": "alice@example.com"},
	  "estimatedDays": 5,
	  "packages": [
	    {"sku": "SKU-001", "quantity": 2, "weightKg": 1.25},
	    {"sku": "SKU-002", "quantity": 1}
	  ]
	}`
	stmts, err := m.InsertStatements(map[string]string{
		"source":      "file://x.json",
		"ingested_at": "2026-09-24T10:00:00Z",
	}, data)
	if err != nil {
		t.Fatalf("InsertStatements: %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements (root + 2 children), got %d", len(stmts))
	}

	root := stmts[0]
	for _, want := range []string{
		`INSERT INTO "landing_order"`,
		`"customer_email"`,
		`'alice@example.com'`,
		`'Alice Martin'`,
		`'standard'`,
		`'550e8400-e29b-41d4-a716-446655440001'`,
		`'file://x.json'`,
	} {
		if !strings.Contains(root, want) {
			t.Errorf("root insert missing %q\n%s", want, root)
		}
	}

	child1, child2 := stmts[1], stmts[2]
	for _, want := range []string{
		`INSERT INTO "landing_order_packages"`,
		`"landing_order_id", "row_no", "quantity", "sku", "weight_kg"`,
		`'550e8400-e29b-41d4-a716-446655440001'`, // FK back-ref value
	} {
		if !strings.Contains(child1, want) && !strings.Contains(child2, want) {
			t.Errorf("child inserts missing %q\n%s\n%s", want, child1, child2)
		}
	}
	if !strings.Contains(child1, `'SKU-001', 1.25`) && !strings.Contains(child1, `1.25`) {
		t.Errorf("child1 weight missing:\n%s", child1)
	}
	if !strings.Contains(child2, `NULL`) || !strings.Contains(child2, `'SKU-002'`) {
		t.Errorf("child2 missing SKU/NULL weight:\n%s", child2)
	}
}

func TestModelUpsertStatementsReplaceSemantics(t *testing.T) {
	m := planFixture(t)
	data := `{
	  "id": "550e8400-e29b-41d4-a716-446655440001",
	  "type": "express",
	  "customer": {"name": "Bob Dupont"},
	  "courierPhone": "+33612345678",
	  "packages": [{"sku": "SKU-X", "quantity": 3}]
	}`
	stmts, err := m.UpsertStatements(map[string]string{}, data, "id")
	if err != nil {
		t.Fatalf("UpsertStatements: %v", err)
	}

	// 1) child DELETE before root UPSERT, 2) root ON CONFLICT, 3) child INSERT.
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(stmts))
	}
	if !strings.Contains(stmts[0], `DELETE FROM "landing_order_packages"`) ||
		!strings.Contains(stmts[0], `"landing_order_id" = '550e8400-e29b-41d4-a716-446655440001'`) {
		t.Errorf("delete stmt wrong:\n%s", stmts[0])
	}
	if !strings.Contains(stmts[1], `ON CONFLICT ("id") DO UPDATE`) {
		t.Errorf("root upsert stmt wrong:\n%s", stmts[1])
	}
	if !strings.Contains(stmts[2], `INSERT INTO "landing_order_packages"`) {
		t.Errorf("child insert stmt wrong:\n%s", stmts[2])
	}
	// Repeated rows for the same id must replace (DELETE not INSERT-violate).
	if strings.Count(stmts[1], "ON CONFLICT") != 1 {
		t.Errorf("root must upsert once:\n%s", stmts[1])
	}
}

func TestModelUpsertRejectsBadConflictColumn(t *testing.T) {
	m := planFixture(t)
	if _, err := m.UpsertStatements(map[string]string{}, `{"id":"x"}`, "other_col"); err == nil {
		t.Fatal("expected error for non-PK conflict column")
	}
}

func TestModelPlanScalarArrayChild(t *testing.T) {
	schema := `{
	  "$defs": {"ob":{"type":"object","properties":{"a":{"type":"string"},
	     "tags":{"type":"array","items":{"type":"string"}}}}},
	  "$ref": "#/$defs/ob"
	}`
	m, err := Plan(schema, "tbl", "a", nil)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(m.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(m.Tables))
	}
	child := m.Tables[1]
	if child.Name != "tbl_tags" {
		t.Errorf("scalar child name = %q", child.Name)
	}
	if len(child.Columns) != 3 || child.Columns[2].Name != "value" {
		t.Errorf("scalar child columns = %v", child.Columns)
	}
}

func TestModelPlanScalarArrayInsert(t *testing.T) {
	schema := `{"type":"object","properties":{
	   "id":{"type":"string"},
	   "tags":{"type":"array","items":{"type":"string"}}}}`
	m, err := Plan(schema, "tbl", "id", nil)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	stmts, err := m.InsertStatements(nil, `{"id":"A1","tags":["x","y"]}`)
	if err != nil {
		t.Fatalf("InsertStatements: %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(stmts))
	}
	if !strings.Contains(stmts[1], `INSERT INTO "tbl_tags"`) ||
		!strings.Contains(stmts[1], `'x'`) {
		t.Errorf("scalar insert 1 wrong:\n%s", stmts[1])
	}
	if !strings.Contains(stmts[2], `'y'`) {
		t.Errorf("scalar insert 2 wrong:\n%s", stmts[2])
	}
}

func TestModelPlanUntypableObjectErrors(t *testing.T) {
	schema := `{"type":"object","properties":{"id":{"type":"string"},
	   "meta":{"type":"object"}}}`
	_, err := Plan(schema, "tbl", "id", nil)
	if err == nil {
		t.Fatal("expected plan error for untyped object")
	}
	if !strings.Contains(err.Error(), "meta") {
		t.Errorf("error should mention the JSON path, got: %v", err)
	}
}

func TestModelPlanUntypableArrayErrors(t *testing.T) {
	schema := `{"type":"object","properties":{"id":{"type":"string"},
	   "items":{"type":"array"}}}`
	_, err := Plan(schema, "tbl", "id", nil)
	if err == nil {
		t.Fatal("expected plan error for array without items")
	}
}

func TestModelPlanMissingPkErrors(t *testing.T) {
	m := planFixture(t)
	if _, err := m.InsertStatements(nil, `{"type":"standard","customer":{"name":"No Id"}}`); err == nil {
		t.Fatal("expected error when payload lacks the root key")
	}
}

// Runs the CLI model mode end to end against the order scenario datas.
func TestModelEndToEndDataDir(t *testing.T) {
	schemaDoc, err := os.ReadFile(filepath.Join("..", "..", "schemas", "order", "schema.json"))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	m, err := Plan(string(schemaDoc), "landing_order", "id", map[string]string{
		"source":      "TEXT",
		"ingested_at": "TIMESTAMPTZ",
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	files, err := os.ReadDir(filepath.Join("..", "..", "schemas", "order", "datas"))
	if err != nil {
		t.Fatalf("read datas: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no data files")
	}
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join("..", "..", "schemas", "order", "datas", f.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", f.Name(), err)
		}
		stmts, err := m.InsertStatements(nil, string(data))
		if err != nil {
			t.Fatalf("Insert %s: %v", f.Name(), err)
		}
		if len(stmts) < 2 {
			t.Errorf("%s: expected root + child inserts, got %d", f.Name(), len(stmts))
		}
	}
}
