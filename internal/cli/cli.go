// Package cli implements the schema-to-table command: flag parsing lives in
// cmd/main.go, everything else — statement building, live database execution
// and drop logic — lives here.
package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/laurentpoirierfr/schema-to-table/internal/landing"
	"github.com/laurentpoirierfr/schema-to-table/internal/normalized"
	"github.com/laurentpoirierfr/schema-to-table/internal/schema"
)

// Flags mirrors the CLI flags (defined in cmd/main.go).
type Flags struct {
	SchemaPath     string
	DataDir        string
	TableName      string
	ComplexType    string
	ConflictColumn string
	Mode           string
	Headers        string
	HeaderValues   string
	DSN            string
	Drop           bool
	PK             string
	ModelMode      bool
}

// Run executes the CLI.
func Run(f Flags) error {
	switch f.Mode {
	case "create", "insert", "upsert", "all":
	default:
		return fmt.Errorf("invalid -mode %q (create|insert|upsert|all)", f.Mode)
	}

	schemaDoc, err := os.ReadFile(f.SchemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}

	headerTypes, err := parseKV(f.Headers)
	if err != nil {
		return fmt.Errorf("-headers: %w", err)
	}
	headerVals, err := parseKV(f.HeaderValues)
	if err != nil {
		return fmt.Errorf("-header-values: %w", err)
	}
	for k := range headerVals {
		if _, ok := headerTypes[k]; !ok {
			return fmt.Errorf("header value %q has no matching -headers type", k)
		}
	}
	for k := range headerTypes {
		if _, ok := headerVals[k]; !ok && f.Mode != "create" {
			headerVals[k] = "" // present but empty: still materialized as ''
		}
	}

	files, err := dataFiles(f.DataDir)
	if err != nil {
		return err
	}

	// SQL statements, in execution order.
	var stmts []stmt

	if f.ModelMode {
		stmts, err = buildModelStatements(string(schemaDoc), f.TableName, f.PK, f.ConflictColumn, f.Mode, headerTypes, headerVals, files)
		if err != nil {
			return err
		}
	} else {
		tbl, err := landing.Parse(string(schemaDoc), f.ComplexType)
		if err != nil {
			return err
		}
		if f.Mode == "create" || f.Mode == "all" {
			s, err := tbl.CreateTable(f.TableName, headerTypes)
			if err != nil {
				return err
			}
			stmts = append(stmts, stmt{"create", s})
			// ON CONFLICT upserts need a unique/PK constraint on the conflict
			// column — landing tables declare it right after creation.
			if f.PK != "" {
				cols := strings.Split(f.PK, ",")
				quoted := make([]string, len(cols))
				for i, c := range cols {
					quoted[i] = schema.QuoteIdent(strings.TrimSpace(c))
				}
				alter := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);\n",
					schema.QuoteIdent(f.TableName), strings.Join(quoted, ", "))
				stmts = append(stmts, stmt{"create primary key", alter})
			}
		}
		if f.Mode == "insert" || f.Mode == "upsert" || f.Mode == "all" {
			for _, file := range files {
				data, err := os.ReadFile(file)
				if err != nil {
					return fmt.Errorf("read %s: %w", file, err)
				}
				if f.Mode == "insert" || f.Mode == "all" {
					s, err := tbl.Insert(f.TableName, headerVals, string(data))
					if err != nil {
						return fmt.Errorf("%s: %w", file, err)
					}
					stmts = append(stmts, stmt{"insert " + filepath.Base(file), s})
				}
				if f.Mode == "upsert" || f.Mode == "all" {
					s, err := tbl.Upsert(f.TableName, headerVals, string(data), f.ConflictColumn)
					if err != nil {
						return fmt.Errorf("%s: %w", file, err)
					}
					stmts = append(stmts, stmt{"upsert " + filepath.Base(file), s})
				}
			}
		}
	}

	for _, st := range stmts {
		fmt.Printf("-- %s\n%s\n", st.label, st.sql)
	}

	if f.DSN == "" {
		return nil
	}

	// Optional live execution against PostgreSQL.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", f.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if f.Drop {
		if f.ModelMode {
			// Re-plan just for names: views first, then tables deepest-first.
			m2, err := normalized.Plan(string(schemaDoc), f.TableName, orDefault(f.PK, "id"), headerTypes)
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
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", f.TableName+"_registry")); err != nil {
				return fmt.Errorf("drop registry: %w", err)
			}
		} else {
			// CASCADE removes any generated views depending on the table.
			q := fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", f.TableName)
			if _, err := db.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("drop %s: %w", f.TableName, err)
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
