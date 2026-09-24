//go:build integration

// Package tests runs end-to-end integration tests against a live
// PostgreSQL. Every scenario under ../schemas (a directory containing
// schema.json, scenario.json and datas/*.json) is exercised through both
// pipelines (flat + normalized model) with generic invariants.
package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/laurentpoirierfr/schema-to-table/internal/service"
)

// integrationDB opens the test database, skipping when unreachable.
func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable"
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("PostgreSQL unreachable (%v), start with: docker compose up -d", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nSQL:\n%s", err, query)
	}
}

func mustCount(t *testing.T, db *sql.DB, query string, want int, what string) {
	t.Helper()
	var n int
	if err := db.QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("count %s: %v\nSQL: %s", what, err, query)
	}
	if n != want {
		t.Fatalf("%s: expected %d, got %d\nSQL: %s", what, want, n, query)
	}
}

// scenario describes one schema fixture directory.
type scenario struct {
	Dir     string            `json:"-"`
	Table   string            `json:"table"`
	PK      string            `json:"pk"`
	Headers map[string]string `json:"headers"`
}

// findScenarios lists every directory under schemas containing a
// schema.json + scenario.json, sorted for determinism.
func findScenarios(t *testing.T) []scenario {
	t.Helper()
	root := filepath.Join("..", "schemas")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read schemas dir: %v", err)
	}
	var out []scenario
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		if _, err := os.Stat(filepath.Join(dir, "schema.json")); err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, "scenario.json"))
		if err != nil {
			t.Fatalf("read scenario.json in %s: %v", dir, err)
		}
		var s scenario
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("parse scenario.json in %s: %v", dir, err)
		}
		s.Dir = dir
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Dir < out[j].Dir })
	if len(out) == 0 {
		t.Fatal("no scenario found under schemas/")
	}
	return out
}

func (s scenario) schemaDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(s.Dir, "schema.json"))
	if err != nil {
		t.Fatalf("read schema.json: %v", err)
	}
	return string(b)
}

// dataFiles lists the sorted datas/*.json payloads of a scenario.
func (s scenario) dataFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.Dir, "datas"))
	if err != nil {
		t.Fatalf("read datas dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			files = append(files, filepath.Join(s.Dir, "datas", e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatalf("no datas/*.json in %s", s.Dir)
	}
	return files
}

// TestIntegrationAllScenarios loops every scenario and runs the whole
// pipeline (flat create/insert/upsert) against the live database.
func TestIntegrationAllScenarios(t *testing.T) {
	db := integrationDB(t)

	for _, sc := range findScenarios(t) {
		sc := sc
		t.Run(filepath.Base(sc.Dir), func(t *testing.T) {
			dropTables(t, db, sc)

			tbl, err := service.New(sc.schemaDoc(t), "JSONB")
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			mustExec(t, db, mustCreate(t, tbl, sc))
			mustExec(t, db, fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)", quoteIdent(sc.Table), quoteIdent(sc.PK)))

			files := sc.dataFiles(t)
			for _, f := range files {
				data := readFile(t, f)
				mustExec(t, db, mustInsert(t, tbl, sc.Table, filepath.Base(f), data))
			}
			mustCount(t, db, fmt.Sprintf(`SELECT count(*) FROM %q`, sc.Table), len(files), "root rows after insert")

			// Upsert the last document again: same key, changed payload.
			last := files[len(files)-1]
			mustExec(t, db, mustUpsert(t, tbl, sc, filepath.Base(last), readFile(t, last)))
			mustCount(t, db, fmt.Sprintf(`SELECT count(*) FROM %q`, sc.Table), len(files), "root rows after upsert (no duplicate)")
		})
	}
}

// TestIntegrationAllScenariosModelNormalized runs the fully-tabular model
// (create tables + views, insert, upsert) on every scenario.
func TestIntegrationAllScenariosModelNormalized(t *testing.T) {
	db := integrationDB(t)

	for _, sc := range findScenarios(t) {
		sc := sc
		t.Run(filepath.Base(sc.Dir), func(t *testing.T) {
			model, err := service.PlanModel(sc.schemaDoc(t), sc.Table, sc.PK, sc.Headers)
			if err != nil {
				t.Fatalf("PlanModel: %v", err)
			}
			if model.Root.Name != sc.Table {
				t.Fatalf("root table = %q, want %q", model.Root.Name, sc.Table)
			}
			dropTables(t, db, sc)

			for _, s := range mustCreateModelStatements(t, model) {
				mustExec(t, db, s)
			}
			for _, s := range mustViewModelStatements(t, model) {
				mustExec(t, db, s)
			}
			for _, s := range mustRegistryStatements(t, model) {
				mustExec(t, db, s)
			}

			// The registry documents every table and view with its logical
			// name, final SQL name, JSON path and description.
			mustCount(t, db, fmt.Sprintf(
				`SELECT count(*) FROM %q`, sc.Table+"_registry"),
				len(model.Tables)+len(model.Root.Arrays), "registry rows")
			mustCount(t, db, fmt.Sprintf(
				`SELECT count(*) FROM %q WHERE object_type = 'table'`, sc.Table+"_registry"),
				len(model.Tables), "registry table rows")

			files := sc.dataFiles(t)
			for _, f := range files {
				data := readFile(t, f)
				for _, s := range mustInsertModelStatements(t, model, filepath.Base(f), data, sc.Headers) {
					mustExec(t, db, s)
				}
			}

			mustCount(t, db, fmt.Sprintf(`SELECT count(*) FROM %q`, sc.Table), len(files), "root rows")

			// Every child table holds exactly the flattened array items of
			// all documents (one row per array element per parent row).
			for _, child := range model.Tables {
				if child == model.Root {
					continue
				}
				total := 0
				for _, f := range files {
					total += arrayLength(t, readFile(t, f), child.Path)
				}
				want := total
				mustCount(t, db, fmt.Sprintf(`SELECT count(*) FROM %q`, child.Name), want,
					fmt.Sprintf("child table %s", child.Name))
			}

			// Each denormalized view exposes exactly as many rows as its
			// child table (one row per child row).
			for _, child := range model.Root.Arrays {
				var viewCount, childCount int
				viewName := "v_" + sc.Table + "_" + strings.Join(child.Path, "_")
				if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %q`, viewName)).Scan(&viewCount); err != nil {
					t.Fatalf("count view %s: %v", viewName, err)
				}
				if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %q`, child.Name)).Scan(&childCount); err != nil {
					t.Fatalf("count child %s: %v", child.Name, err)
				}
				if viewCount != childCount {
					t.Errorf("view %s has %d rows, child %s has %d", viewName, viewCount, child.Name, childCount)
				}
			}

			// Upsert last document (replace semantics): children deleted
			// then re-inserted for the same parent key.
			last := files[len(files)-1]
			for _, s := range mustUpsertModelStatements(t, model, filepath.Base(last), readFile(t, last), sc.Headers, sc.PK) {
				mustExec(t, db, s)
			}
			mustCount(t, db, fmt.Sprintf(`SELECT count(*) FROM %q`, sc.Table), len(files), "root rows after upsert")
		})
	}
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func mustCreate(t *testing.T, tbl *service.Table, sc scenario) string {
	t.Helper()
	s, err := tbl.CreateTable(sc.Table, sc.Headers)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return s
}

func mustInsert(t *testing.T, tbl *service.Table, table, name, data string) string {
	t.Helper()
	s, err := tbl.Insert(table, headerVals(name), data)
	if err != nil {
		t.Fatalf("Insert %s: %v", name, err)
	}
	return s
}

func mustUpsert(t *testing.T, tbl *service.Table, sc scenario, name, data string) string {
	t.Helper()
	s, err := tbl.Upsert(sc.Table, headerVals(name), data, sc.PK)
	if err != nil {
		t.Fatalf("Upsert %s: %v", name, err)
	}
	return s
}

func mustCreateModelStatements(t *testing.T, m *service.Model) []string {
	t.Helper()
	ss, err := m.CreateStatements()
	if err != nil {
		t.Fatalf("CreateStatements: %v", err)
	}
	return ss
}

func mustViewModelStatements(t *testing.T, m *service.Model) []string {
	t.Helper()
	ss, err := m.ViewStatements()
	if err != nil {
		t.Fatalf("ViewStatements: %v", err)
	}
	return ss
}

func mustRegistryStatements(t *testing.T, m *service.Model) []string {
	t.Helper()
	ss, err := m.RegistryStatements()
	if err != nil {
		t.Fatalf("RegistryStatements: %v", err)
	}
	return ss
}

func mustInsertModelStatements(t *testing.T, m *service.Model, name, data string, headers map[string]string) []string {
	t.Helper()
	// headers type map isn't needed for DML, only root ingest values.
	ss, err := m.InsertStatements(headerVals(name), data)
	if err != nil {
		t.Fatalf("Insert %s: %v", name, err)
	}
	return ss
}

func mustUpsertModelStatements(t *testing.T, m *service.Model, name, data string, headers map[string]string, pk string) []string {
	t.Helper()
	ss, err := m.UpsertStatements(headerVals(name), data, pk)
	if err != nil {
		t.Fatalf("Upsert %s: %v", name, err)
	}
	return ss
}

// arrayLength returns the number of elements of the array reached through
// the child's JSON path in a payload document (0 when absent or not an
// array).
func arrayLength(t *testing.T, data string, path []string) int {
	t.Helper()
	var node any
	if err := json.Unmarshal([]byte(data), &node); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	for _, key := range path {
		obj, ok := node.(map[string]any)
		if !ok {
			return 0
		}
		node = obj[key]
	}
	arr, ok := node.([]any)
	if !ok {
		return 0
	}
	return len(arr)
}

func headerVals(src string) map[string]string {
	return map[string]string{
		"source":      "file://" + src,
		"ingested_at": "2026-09-24T10:00:00Z",
	}
}

func quoteIdent(name string) string {
	return `"` + name + `"`
}

// dropTables drops the scenario's views and tables (children before root),
// each with CASCADE so leftover table/view artifacts from previous runs or
// the demos are also removed.
func dropTables(t *testing.T, db *sql.DB, sc scenario) {
	t.Helper()
	model, err := service.PlanModel(sc.schemaDoc(t), sc.Table, sc.PK, sc.Headers)
	if err != nil {
		t.Fatalf("PlanModel in dropTables: %v", err)
	}
	for _, child := range model.Root.Arrays {
		view := "v_" + model.Root.Name + "_" + strings.Join(child.Path, "_")
		mustExec(t, db, fmt.Sprintf("DROP VIEW IF EXISTS %q", view))
	}
	// Deepest-first: ensure every parent is dropped after its children.
	for i := len(model.Tables) - 1; i >= 0; i-- {
		mustExec(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", model.Tables[i].Name))
	}
	mustExec(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", sc.Table))
	mustExec(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", sc.Table+"_registry"))
}
