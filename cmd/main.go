// Command schema-to-table turns JSON documents into PostgreSQL landing
// table statements (CREATE / INSERT / UPSERT) derived from a JSON Schema,
// optionally executing them against a live database.
//
// The pipeline stages are: parse schema → plan columns → collect payload
// values → render SQL (see internal/service).
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/laurentpoirierfr/schema-to-table/internal/service"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	schemaPath := flag.String("schema", "schemas/order/schema.json", "JSON Schema (draft 2020-12) file")
	dataDir := flag.String("data", "schemas/order/datas", "directory of JSON payload files")
	tableName := flag.String("table", "landing_order", "target table name")
	complexType := flag.String("complex-type", "JSONB", "SQL type for arrays/generic objects (TEXT, JSONB…)")
	conflictColumn := flag.String("conflict", "id", "conflict column for upsert mode")
	mode := flag.String("mode", "all", "create | insert | upsert | all")
	headers := flag.String("headers", "", "ingestion header types, e.g. \"source=TEXT,ingested_at=TIMESTAMPTZ\"")
	headerValues := flag.String("header-values", "", "ingestion header values for DML, e.g. \"source=batch-1,ingested_at=2026-01-01T00:00:00Z\"")
	dsn := flag.String("dsn", "", "if set, execute statements against this PostgreSQL DSN instead of only printing")
	drop := flag.Bool("drop", false, "DROP TABLE IF EXISTS before create (only with -dsn)")
	pk := flag.String("pk", "", "comma-separated columns to declare PRIMARY KEY after create (required for ON CONFLICT upserts)")
	modelMode := flag.Bool("model", false, "use the normalized fully-tabular model (one typed table per array + denormalized root×children views) instead of a flat table + complexType column")
	flag.Parse()

	switch *mode {
	case "create", "insert", "upsert", "all":
	default:
		return fmt.Errorf("invalid -mode %q (create|insert|upsert|all)", *mode)
	}

	schemaDoc, err := os.ReadFile(*schemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}

	headerTypes, err := parseKV(*headers)
	if err != nil {
		return fmt.Errorf("-headers: %w", err)
	}
	headerVals, err := parseKV(*headerValues)
	if err != nil {
		return fmt.Errorf("-header-values: %w", err)
	}
	for k := range headerVals {
		if _, ok := headerTypes[k]; !ok {
			return fmt.Errorf("header value %q has no matching -headers type", k)
		}
	}
	for k := range headerTypes {
		if _, ok := headerVals[k]; !ok && *mode != "create" {
			headerVals[k] = "" // present but empty: still materialized as ''
		}
	}

	files, err := dataFiles(*dataDir)
	if err != nil {
		return err
	}

	// Stage 4 output: SQL statements, in execution order.
	var stmts []stmt

	if *modelMode {
		stmts, err = buildModelStatements(string(schemaDoc), *tableName, *pk, *conflictColumn, *mode, headerTypes, headerVals, files)
		if err != nil {
			return err
		}
	} else {
		tbl, err := service.New(string(schemaDoc), *complexType)
		if err != nil {
			return err
		}
		if *mode == "create" || *mode == "all" {
			s, err := tbl.CreateTable(*tableName, headerTypes)
			if err != nil {
				return err
			}
			stmts = append(stmts, stmt{"create", s})
			// ON CONFLICT upserts need a unique/PK constraint on the conflict
			// column — landing tables declare it right after creation.
			if *pk != "" {
				cols := strings.Split(*pk, ",")
				quoted := make([]string, len(cols))
				for i, c := range cols {
					quoted[i] = quoteIdent(strings.TrimSpace(c))
				}
				alter := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);\n",
					quoteIdent(*tableName), strings.Join(quoted, ", "))
				stmts = append(stmts, stmt{"create primary key", alter})
			}
		}
		if *mode == "insert" || *mode == "upsert" || *mode == "all" {
			for _, f := range files {
				data, err := os.ReadFile(f)
				if err != nil {
					return fmt.Errorf("read %s: %w", f, err)
				}
				if *mode == "insert" || *mode == "all" {
					s, err := tbl.Insert(*tableName, headerVals, string(data))
					if err != nil {
						return fmt.Errorf("%s: %w", f, err)
					}
					stmts = append(stmts, stmt{"insert " + filepath.Base(f), s})
				}
				if *mode == "upsert" || *mode == "all" {
					s, err := tbl.Upsert(*tableName, headerVals, string(data), *conflictColumn)
					if err != nil {
						return fmt.Errorf("%s: %w", f, err)
					}
					stmts = append(stmts, stmt{"upsert " + filepath.Base(f), s})
				}
			}
		}
	}

	for _, st := range stmts {
		fmt.Printf("-- %s\n%s\n", st.label, st.sql)
	}

	if *dsn == "" {
		return nil
	}

	// Optional live execution against PostgreSQL.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if *drop {
		if *modelMode {
			// Re-plan just for names: views first, then tables deepest-first.
			m2, err := service.PlanModel(string(schemaDoc), *tableName, orDefault(*pk, "id"), headerTypes)
			if err != nil {
				return fmt.Errorf("drop/plan: %w", err)
			}
			for _, child := range m2.Root.Arrays {
				view := "v_" + m2.Root.Name + "_" + strings.TrimPrefix(child.Name, m2.Root.Name+"_")
				if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %q", view)); err != nil {
					return fmt.Errorf("drop view %s: %w", view, err)
				}
			}
			for i := len(m2.Tables) - 1; i >= 0; i-- {
				if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", m2.Tables[i].Name)); err != nil {
					return fmt.Errorf("drop %s: %w", m2.Tables[i].Name, err)
				}
			}
			// The schema registry documents the model — drop it too.
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", *tableName+"_registry")); err != nil {
				return fmt.Errorf("drop registry: %w", err)
			}
		} else {
			// CASCADE removes any generated views depending on the table.
			q := fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", *tableName)
			if _, err := db.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("drop %s: %w", *tableName, err)
			}
		}
	}
	for _, st := range stmts {
		if _, err := db.ExecContext(ctx, st.sql); err != nil {
			return fmt.Errorf("exec %s: %w", st.label, err)
		}
	}
	fmt.Fprintf(os.Stderr, "executed %d statement(s)\n", len(stmts))
	return nil
}

// parseKV parses "k=v,k2=v2" flag payloads into a map.
func parseKV(s string) (map[string]string, error) {
	m := map[string]string{}
	s = strings.TrimSpace(s)
	if s == "" {
		return m, nil
	}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid pair %q (want key=value)", pair)
		}
		m[k] = strings.TrimSpace(v)
	}
	return m, nil
}

// orDefault returns s, or fallback when s is empty.
func orDefault(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

// dataFiles lists *.json files in dir, sorted for deterministic runs.
func dataFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no *.json files in %s", dir)
	}
	return files, nil
}

// quoteIdent double-quotes a SQL identifier (CLI-local copy).
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// stmt is a labeled SQL statement pending printing/execution.
type stmt struct{ label, sql string }

// buildModelStatements renders the normalized model statements: one DDL
// per table, the denormalized root×children views, then per-data-file
// INSERT/UPSERT batches.
func buildModelStatements(schemaDoc, tableName, pk, conflict, mode string, headerTypes, headerVals map[string]string, files []string) ([]stmt, error) {
	if pk == "" {
		pk = "id"
	}
	model, err := service.PlanModel(schemaDoc, tableName, pk, headerTypes)
	if err != nil {
		return nil, err
	}
	var stmts []stmt

	// Discriminator informational line (STI type tag), when detected.
	if model.Discriminator != "" {
		stmts = append(stmts, stmt{"schema", fmt.Sprintf("-- STI discriminator: %s\n", model.Discriminator)})
	}

	if mode == "create" || mode == "all" {
		dmls, err := model.CreateStatements()
		if err != nil {
			return nil, err
		}
		for _, d := range dmls {
			stmts = append(stmts, stmt{"create", d})
		}
		vws, err := model.ViewStatements()
		if err != nil {
			return nil, err
		}
		for _, v := range vws {
			stmts = append(stmts, stmt{"create view", v})
		}
		reg, err := model.RegistryStatements()
		if err != nil {
			return nil, err
		}
		for _, r := range reg {
			stmts = append(stmts, stmt{"create registry", r})
		}
	}
	if mode == "insert" || mode == "upsert" || mode == "all" {
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", f, err)
			}
			if mode == "insert" || mode == "all" {
				s, err := model.InsertStatements(headerVals, string(data))
				if err != nil {
					return nil, fmt.Errorf("%s: %w", f, err)
				}
				for _, one := range s {
					stmts = append(stmts, stmt{"insert " + filepath.Base(f), one})
				}
			}
			if mode == "upsert" || mode == "all" {
				s, err := model.UpsertStatements(headerVals, string(data), conflict)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", f, err)
				}
				for _, one := range s {
					stmts = append(stmts, stmt{"upsert " + filepath.Base(f), one})
				}
			}
		}
	}
	return stmts, nil
}
