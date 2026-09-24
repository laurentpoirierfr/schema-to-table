//go:build integration

package processors

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/warpstreamlabs/bento/v4/public/service"
)

// TestProcessorsIntegration runs the two processors end-to-end against a
// real PostgreSQL (compose.yaml, see Makefile db-up):
//
//	go test -tags integration ./pkg/processors -run TestProcessorsIntegration
func TestProcessorsIntegration(t *testing.T) {
	dsn := os.Getenv("S2T_DSN")
	if dsn == "" {
		dsn = "postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable"
	}

	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	const root = "landing_bento_it"
	drop := fmt.Sprintf(`
DROP VIEW IF EXISTS %s;
DROP TABLE IF EXISTS %s_tags;
DROP TABLE IF EXISTS %s_registry;
DROP TABLE IF EXISTS %s CASCADE;`, "v_"+root, root, root, root)
	if _, err := db.ExecContext(ctx, drop); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), drop)
	})

	srv, _ := schemaServer(t, testSchema)
	yaml := fmt.Sprintf(`dsn: %s
schema_url_header: schema_url
table_name_header: table_name
pk: id
headers:
  source: TEXT
  trace_id: TEXT
create_model: true
`, dsn)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha","tags":["a","b"]}`, map[string]string{
			"table_name": root, "schema_url": srv.URL, "source": "api", "trace_id": "t-0001",
		}),
		message(`{"id":2,"name":"beta","tags":[]}`, map[string]string{
			"table_name": root, "schema_url": srv.URL, "source": "api2", "trace_id": "t-0002",
		}),
	}

	// --- Insert pass, model created on the fly ------------------------------
	procInsert := newTestProcessor(t, false, yaml, newFakeSink())
	procInsert.sink = nil // force a real connection on the first batch
	procInsert.opened = false

	if _, err := procInsert.ProcessBatch(ctx, batch); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	counts, err := rowCounts(ctx, db, root)
	if err != nil {
		t.Fatalf("counts after insert: %v", err)
	}
	if counts[root] != 2 || counts[root+"_tags"] != 2 {
		t.Errorf("after insert: root=%d tags=%d, want 2/2", counts[root], counts[root+"_tags"])
	}
	if !tableExists(ctx, db, "v_"+root+"_tags") || !tableExists(ctx, db, root+"_registry") {
		t.Error("expected the denormalized view and the schema registry to exist")
	}
	var regRows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s_registry", root)).Scan(&regRows); err != nil {
		t.Fatalf("query registry: %v", err)
	}
	if regRows != 3 { // root table + child table + view
		t.Errorf("registry rows = %d, want 3", regRows)
	}

	// The technical headers of each incoming message are stored as
	// header_<name> columns of the root table.
	var src, trace string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT "header_source", "header_trace_id" FROM "%s" WHERE "id" = 1`, root)).
		Scan(&src, &trace); err != nil {
		t.Fatalf("read header columns: %v", err)
	}
	if src != "api" || trace != "t-0001" {
		t.Errorf("message 1 technical headers stored as %q/%q, want api/t-0001", src, trace)
	}
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT "header_source", "header_trace_id" FROM "%s" WHERE "id" = 2`, root)).
		Scan(&src, &trace); err != nil {
		t.Fatalf("read header columns: %v", err)
	}
	if src != "api2" || trace != "t-0002" {
		t.Errorf("message 2 technical headers stored as %q/%q, want api2/t-0002", src, trace)
	}

	// --- Upsert pass on the same model --------------------------------------
	procUpsert := newTestProcessor(t, true, yaml, newFakeSink())
	procUpsert.sink = nil
	procUpsert.opened = false

	if _, err := procUpsert.ProcessBatch(ctx, batch); err != nil {
		t.Fatalf("upsert batch: %v", err)
	}
	counts, err = rowCounts(ctx, db, root)
	if err != nil {
		t.Fatalf("counts after upsert: %v", err)
	}
	if counts[root] != 2 || counts[root+"_tags"] != 2 {
		t.Errorf("after upsert: root=%d tags=%d, want 2/2 (replaced, not duplicated)", counts[root], counts[root+"_tags"])
	}
}

func rowCounts(ctx context.Context, db *sql.DB, root string) (map[string]int, error) {
	out := map[string]int{}
	for _, tbl := range []string{root, root + "_tags"} {
		var n int
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s"`, tbl)).Scan(&n); err != nil {
			return nil, fmt.Errorf("%s: %w", tbl, err)
		}
		out[tbl] = n
	}
	return out, nil
}

func tableExists(ctx context.Context, db *sql.DB, name string) bool {
	var ok bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)`,
		name).Scan(&ok)
	return err == nil && ok
}
