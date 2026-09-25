package landing

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testTable(t *testing.T) *Table {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join("..", "..", "schemas", "order", "schema.json"))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	tbl, err := Parse(string(doc), "JSONB")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return tbl
}

func testData(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "schemas", "order", "datas", name))
	if err != nil {
		t.Fatalf("read data %s: %v", name, err)
	}
	return string(b)
}

func datasFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join("..", "..", "schemas", "order", "datas"))
	if err != nil {
		t.Fatalf("read datas dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	return names
}

func TestCreateTableGolden(t *testing.T) {
	tbl := testTable(t)
	headers := map[string]string{"source": "TEXT", "ingested_at": "TIMESTAMPTZ"}
	got, err := tbl.CreateTable("landing_order", headers)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	want := `CREATE TABLE "landing_order" (
    "header_ingested_at" TIMESTAMPTZ,
    "header_source" TEXT,
    "customer_email" TEXT,
    "customer_name" TEXT,
    "id" UUID,
    "packages" JSONB,
    "type" TEXT,
    "estimated_days" BIGINT,
    "courier_phone" TEXT
);
`
	if got != want {
		t.Errorf("CreateTable:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestCreateTableNoColumns(t *testing.T) {
	// A schema without any walked property → error at plan stage.
	tbl, err := Parse(`{"type":"object"}`, "TEXT")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := tbl.Columns(nil); err == nil {
		t.Error("expected error for schema with no columns")
	}
}

func TestInsertDisplay(t *testing.T) {
	tbl := testTable(t)
	got, err := tbl.Insert("landing_order", map[string]string{"source": "api"}, testData(t, "order-express-full.json"))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Express variant: estimated_days must be NULL, courier_phone filled.
	if !strings.Contains(got, `"estimated_days"`) || !strings.Contains(got, ", NULL") {
		t.Errorf("express insert should carry NULL for estimated_days:\n%s", got)
	}
	if !strings.Contains(got, `'[{"quantity":10,"sku":"SKU-100","weightKg":12.75}]'::JSONB`) {
		t.Errorf("packages should be JSONB-serialized:\n%s", got)
	}
	if !strings.Contains(got, `"header_source"`) {
		t.Errorf("header columns missing:\n%s", got)
	}
}

func TestInsertColumnsMatchCreate(t *testing.T) {
	tbl := testTable(t)
	headers := map[string]string{"source": "TEXT"}
	createSQL, err := tbl.CreateTable("t", headers)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	createCols := columnNames(t, createSQL)
	for _, name := range datasFiles(t) {
		insertSQL, err := tbl.Insert("t", map[string]string{"source": name}, testData(t, name))
		if err != nil {
			t.Fatalf("Insert %s: %v", name, err)
		}
		insCols := columnNames(t, insertSQL)
		if len(insCols) != len(createCols) {
			t.Fatalf("%s: insert has %d cols, create has %d", name, len(insCols), len(createCols))
		}
		for i := range createCols {
			if insCols[i] != createCols[i] {
				t.Errorf("%s: col mismatch idx %d: insert %q create %q", name, i, insCols[i], createCols[i])
			}
		}
	}
}

func TestInsertEscaping(t *testing.T) {
	tbl := testTable(t)
	got, err := tbl.Insert("t", nil, testData(t, "order-standard-sql-escaping.json"))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if !strings.Contains(got, `O''Malley`) {
		t.Errorf("single quote not escaped:\n%s", got)
	}
	if !strings.Contains(got, `SKU-'';--`) {
		t.Errorf("quote in sku not escaped:\n%s", got)
	}
}

func TestUpsertGolden(t *testing.T) {
	tbl := testTable(t)
	got, err := tbl.Upsert("landing_order", nil, testData(t, "order-standard-full.json"), "id")
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	want := `INSERT INTO "landing_order" ("customer_email", "customer_name", "id", "packages", "type", "estimated_days", "courier_phone")
VALUES ('alice.martin@example.com', 'Alice Martin', '550e8400-e29b-41d4-a716-446655440001', '[{"quantity":2,"sku":"SKU-001","weightKg":1.25},{"quantity":1,"sku":"SKU-002","weightKg":0.5}]'::JSONB, 'standard', 5, NULL)
ON CONFLICT ("id") DO UPDATE SET
    "customer_email" = EXCLUDED."customer_email",
    "customer_name" = EXCLUDED."customer_name",
    "packages" = EXCLUDED."packages",
    "type" = EXCLUDED."type",
    "estimated_days" = EXCLUDED."estimated_days",
    "courier_phone" = EXCLUDED."courier_phone";
`
	if got != want {
		t.Errorf("Upsert:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestUpsertErrors(t *testing.T) {
	tbl := testTable(t)
	data := testData(t, "order-standard-full.json")
	if _, err := tbl.Upsert("t", nil, data, "does_not_exist"); err == nil {
		t.Error("expected error for unknown conflict column")
	}
	if _, err := tbl.Upsert("t", nil, data, ""); err == nil {
		t.Error("expected error for empty conflict column")
	}
	if _, err := tbl.Upsert("", nil, data, "id"); err == nil {
		t.Error("expected error for empty table name")
	}
}

func TestValueOrderMatchesColumnPlan(t *testing.T) {
	tbl := testTable(t)
	scalar := `{"id":"550e8400-e29b-41d4-a716-446655440010","type":"standard","customer":{"name":"Zoe"},"packages":[{"sku":"S","quantity":1}],"estimatedDays":2}`
	cols, err := tbl.Columns(nil)
	if err != nil {
		t.Fatalf("Columns: %v", err)
	}
	if len(cols) != 7 {
		t.Fatalf("expected 7 columns, got %d: %+v", len(cols), cols)
	}
	got, err := tbl.Insert("t", nil, scalar)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	insCols := columnNames(t, got)
	if len(insCols) != len(cols) {
		t.Fatalf("insert/plan column count mismatch: %d vs %d", len(insCols), len(cols))
	}
	for i := range cols {
		if insCols[i] != cols[i].Name {
			t.Errorf("col %d: insert %q plan %q", i, insCols[i], cols[i].Name)
		}
	}
}

// columnNames extracts column identifiers from a CREATE TABLE or INSERT
// statement.
func columnNames(t *testing.T, sql string) []string {
	t.Helper()
	var names []string
	if strings.Contains(sql, "CREATE TABLE") {
		for _, line := range strings.Split(sql, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, `"`) {
				if j := strings.Index(line[1:], `"`); j >= 0 {
					names = append(names, line[1:1+j])
				}
			}
		}
		return names
	}
	const marker = "INSERT INTO "
	i := strings.Index(sql, marker)
	if i < 0 {
		t.Fatalf("not a CREATE/INSERT statement:\n%s", sql)
	}
	rest := sql[i+len(marker):]
	open := strings.Index(rest, "(")
	if open < 0 {
		t.Fatalf("no column list:\n%s", sql)
	}
	close := strings.Index(rest[open:], ")")
	if close < 0 {
		t.Fatalf("unterminated column list:\n%s", sql)
	}
	list := rest[open+1 : open+close]
	for _, tok := range strings.Split(list, ",") {
		tok = strings.TrimSpace(tok)
		if len(tok) >= 2 && tok[0] == '"' && tok[len(tok)-1] == '"' {
			names = append(names, tok[1:len(tok)-1])
		}
	}
	return names
}
