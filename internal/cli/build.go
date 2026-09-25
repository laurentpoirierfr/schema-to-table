package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/laurentpoirierfr/schema-to-table/internal/normalized"
)

// stmt is a labeled SQL statement pending printing/execution.
type stmt struct{ label, sql string }

// buildModelStatements renders the normalized model statements: one DDL
// per table, the denormalized root×children views, then per-data-file
// INSERT/UPSERT batches.
func buildModelStatements(schemaDoc, tableName, pk, conflict, mode string, headerTypes, headerVals map[string]string, files []string) ([]stmt, error) {
	if pk == "" {
		pk = "id"
	}
	model, err := normalized.Plan(schemaDoc, tableName, pk, headerTypes)
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
