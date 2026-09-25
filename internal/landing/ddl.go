package landing

import (
	"fmt"
	"strings"

	"github.com/laurentpoirierfr/schema-to-table/internal/schema"
)

// CreateTable renders a "CREATE TABLE" statement for a PostgreSQL landing
// table named tableName, from the planned column list produced by Columns.
//
// headers is the same map accepted by Columns: key → SQL type for each
// extra ingestion column ("header_<key>").
func (t *Table) CreateTable(tableName string, headers map[string]string) (string, error) {
	if strings.TrimSpace(tableName) == "" {
		return "", fmt.Errorf("CreateTable: tableName is required")
	}
	cols, err := t.Columns(headers)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", schema.QuoteIdent(tableName))
	for i, c := range cols {
		sep := ","
		if i == len(cols)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "    %s %s%s\n", schema.QuoteIdent(c.Name), c.Type, sep)
	}
	b.WriteString(");\n")
	return b.String(), nil
}
