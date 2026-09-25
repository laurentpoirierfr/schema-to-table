// Command schema-to-table turns JSON documents into PostgreSQL landing
// table statements (CREATE / INSERT / UPSERT) derived from a JSON Schema,
// optionally executing them against a live database.
//
// The pipeline stages are: parse schema → plan columns → collect payload
// values → render SQL (see internal/schema, internal/landing and
// internal/normalized).
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/laurentpoirierfr/schema-to-table/internal/cli"
)

func main() {
	var f cli.Flags
	flag.StringVar(&f.SchemaPath, "schema", "schemas/order/schema.json", "JSON Schema (draft 2020-12) file")
	flag.StringVar(&f.DataDir, "data", "schemas/order/datas", "directory of JSON payload files")
	flag.StringVar(&f.TableName, "table", "landing_order", "target table name")
	flag.StringVar(&f.ComplexType, "complex-type", "JSONB", "SQL type for arrays/generic objects (TEXT, JSONB…)")
	flag.StringVar(&f.ConflictColumn, "conflict", "id", "conflict column for upsert mode")
	flag.StringVar(&f.Mode, "mode", "all", "create | insert | upsert | all")
	flag.StringVar(&f.Headers, "headers", "", "ingestion header types, e.g. \"source=TEXT,ingested_at=TIMESTAMPTZ\"")
	flag.StringVar(&f.HeaderValues, "header-values", "", "ingestion header values for DML, e.g. \"source=batch-1,ingested_at=2026-01-01T00:00:00Z\"")
	flag.StringVar(&f.DSN, "dsn", "", "if set, execute statements against this PostgreSQL DSN instead of only printing")
	flag.BoolVar(&f.Drop, "drop", false, "DROP TABLE IF EXISTS before create (only with -dsn)")
	flag.StringVar(&f.PK, "pk", "", "comma-separated columns to declare PRIMARY KEY after create (required for ON CONFLICT upserts)")
	flag.BoolVar(&f.ModelMode, "model", false, "use the normalized fully-tabular model (one typed table per array + denormalized root×children views) instead of a flat table + complexType column")
	flag.Parse()

	if err := cli.Run(f); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
