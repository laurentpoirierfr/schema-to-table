package processors

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/warpstreamlabs/bento/v4/public/service"

	st "github.com/laurentpoirierfr/schema-to-table/internal/service"
)

const testSchema = `{
	"type": "object",
	"properties": {
		"id": {"type": "integer"},
		"name": {"type": "string"},
		"tags": {"type": "array", "items": {"type": "string"}}
	}
}`

const testHeaders = "source=TEXT"

// ---------------------------------------------------------------------------
// fake PostgreSQL sink / transaction
// ---------------------------------------------------------------------------

type fakeSink struct {
	exists    map[string]bool // ^ pre-existing root tables
	created   map[string]bool // ^ tables created through the fake
	execs     []string        // every executed statement, in order
	txns      int
	commits   int
	rollbacks int
	failExec  error
}

func newFakeSink() *fakeSink {
	return &fakeSink{exists: map[string]bool{}, created: map[string]bool{}}
}

func (s *fakeSink) Ping(context.Context) error { return nil }
func (s *fakeSink) Begin(context.Context) (tx, error) {
	s.txns++
	return &fakeTx{s: s}, nil
}
func (s *fakeSink) Close() error { return nil }

type fakeTx struct{ s *fakeSink }

func (t *fakeTx) Exec(_ context.Context, query string) error {
	if t.s.failExec != nil {
		return t.s.failExec
	}
	t.s.execs = append(t.s.execs, query)
	if name := createdTableName(query); name != "" {
		t.s.created[name] = true
	}
	return nil
}

func (t *fakeTx) TableExists(_ context.Context, name string) (bool, error) {
	return t.s.exists[name] || t.s.created[name], nil
}
func (t *fakeTx) Commit() error {
	t.s.commits++
	return nil
}
func (t *fakeTx) Rollback() error {
	t.s.rollbacks++
	return nil
}

// createdTableName extracts the table name of a CREATE TABLE statement, for
// the fake's bookkeeping only.
func createdTableName(query string) string {
	q := strings.TrimSpace(query)
	if !strings.HasPrefix(q, "CREATE TABLE ") {
		return ""
	}
	q = q[len("CREATE TABLE "):]
	q = strings.TrimSpace(q)
	if strings.HasPrefix(q, `"`) {
		q = strings.TrimPrefix(q, `"`)
		if i := strings.Index(q, `"`); i >= 0 {
			return q[:i]
		}
	}
	if i := strings.IndexAny(q, " (,"); i >= 0 {
		return q[:i]
	}
	return q
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// schemaServer serves schemaDoc and counts the number of GET requests.
func schemaServer(t *testing.T, schemaDoc string) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, schemaDoc)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// newTestProcessor builds the processor the way Bento would (config parsing
// + registered constructor), then swaps in a fake sink.
func newTestProcessor(t *testing.T, upsert bool, yaml string, sink *fakeSink) *batchProcessor {
	t.Helper()
	var (
		pConf *service.ParsedConfig
		err   error
	)
	if upsert {
		pConf, err = upsertSpec().ParseYAML(yaml, nil)
	} else {
		pConf, err = insertSpec().ParseYAML(yaml, nil)
	}
	if err != nil {
		t.Fatalf("parse yaml: %v", err)
	}

	var proc service.BatchProcessor
	if upsert {
		proc, err = newUpsertProcessor(pConf, nil)
	} else {
		proc, err = newInsertProcessor(pConf, nil)
	}
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	bp := proc.(*batchProcessor)
	bp.sink = sink
	bp.opened = true
	t.Cleanup(func() { _ = bp.Close(context.Background()) })
	return bp
}

func message(payload string, meta map[string]string) *service.Message {
	m := service.NewMessage([]byte(payload))
	for k, v := range meta {
		m.MetaSet(k, v)
	}
	return m
}

func baseYAML() string {
	return `dsn: postgres://user:pass@localhost:5432/db?sslmode=disable
schema_url_header: schema_url
table_name_header: table_name
pk: id
headers:
  source: TEXT
create_model: true
`
}

type testMsg struct {
	payload string
	headers map[string]string // values for the configured ingestion headers
}

// expectedModelExecs renders exactly what the processor must execute for a
// batch: the model DDL (tables, views, registry) followed by each message's
// DML, computed through the very same pipeline the processor uses.
func expectedModelExecs(t *testing.T, schemaDoc, table, pk string, upsert bool, msgs []testMsg) []string {
	t.Helper()
	model, err := st.PlanModel(schemaDoc, table, pk, map[string]string{"source": "TEXT"})
	if err != nil {
		t.Fatalf("PlanModel: %v", err)
	}

	var want []string
	for _, render := range []func() ([]string, error){model.CreateStatements, model.ViewStatements, model.RegistryStatements} {
		stmts, err := render()
		if err != nil {
			t.Fatalf("render DDL: %v", err)
		}
		want = append(want, stmts...)
	}
	for _, m := range msgs {
		var (
			stmts []string
			err   error
		)
		if upsert {
			stmts, err = model.UpsertStatements(m.headers, m.payload, pk)
		} else {
			stmts, err = model.InsertStatements(m.headers, m.payload)
		}
		if err != nil {
			t.Fatalf("render DML: %v", err)
		}
		want = append(want, stmts...)
	}
	return want
}

// expectedDMLExecs renders only the per-message DML (model already exists or
// creation disabled).
func expectedDMLExecs(t *testing.T, schemaDoc, table, pk string, upsert bool, msgs []testMsg) []string {
	t.Helper()
	model, err := st.PlanModel(schemaDoc, table, pk, map[string]string{"source": "TEXT"})
	if err != nil {
		t.Fatalf("PlanModel: %v", err)
	}
	var want []string
	for _, m := range msgs {
		var (
			stmts []string
			err   error
		)
		if upsert {
			stmts, err = model.UpsertStatements(m.headers, m.payload, pk)
		} else {
			stmts, err = model.InsertStatements(m.headers, m.payload)
		}
		if err != nil {
			t.Fatalf("render DML: %v", err)
		}
		want = append(want, stmts...)
	}
	return want
}

func assertExecs(t *testing.T, sink *fakeSink, want []string) {
	t.Helper()
	if len(sink.execs) != len(want) {
		t.Fatalf("executed %d statements, want %d:\ngot:\n%s\n\nwant:\n%s",
			len(sink.execs), len(want), strings.Join(sink.execs, "\n---\n"), strings.Join(want, "\n---\n"))
	}
	for i := range want {
		if sink.execs[i] != want[i] {
			t.Errorf("statement %d mismatch:\ngot:\n%s\n\nwant:\n%s", i, sink.execs[i], want[i])
		}
	}
}

func twoMessages(url string) service.MessageBatch {
	m1 := message(`{"id":1,"name":"alpha","tags":["a","b"]}`, map[string]string{
		"table_name": "landing_test",
		"schema_url": url,
		"source":     "api",
	})
	m2 := message(`{"id":2,"name":"beta","tags":[]}`, map[string]string{
		"table_name": "landing_test",
		"schema_url": url,
	})
	return service.MessageBatch{m1, m2}
}

func twoMessageSpecs() []testMsg {
	return []testMsg{
		{payload: `{"id":1,"name":"alpha","tags":["a","b"]}`, headers: map[string]string{"source": "api"}},
		{payload: `{"id":2,"name":"beta","tags":[]}`, headers: map[string]string{"source": ""}},
	}
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestRegistration(t *testing.T) {
	if registrationErr != nil {
		t.Fatalf("init registration failed: %v", registrationErr)
	}
}

func TestConfigValidation(t *testing.T) {
	pConf, err := insertSpec().ParseYAML(`dsn: ""`, nil)
	if err != nil {
		t.Fatalf("parse empty dsn yaml: %v", err)
	}
	if _, err := newInsertProcessor(pConf, nil); err == nil {
		t.Fatal("expected dsn validation error, got nil")
	}

	// Defaults apply.
	pConf, err = insertSpec().ParseYAML(`dsn: postgres://h/db`, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg, err := parseConfig(pConf)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.schemaURLHeader != "schema_url" || cfg.tableNameHeader != "table_name" {
		t.Errorf("unexpected default headers: %q / %q", cfg.schemaURLHeader, cfg.tableNameHeader)
	}
	if cfg.pk != "id" || !cfg.createModel {
		t.Errorf("unexpected defaults: pk=%q createModel=%v", cfg.pk, cfg.createModel)
	}

	// Empty pk is refused.
	pConf, err = insertSpec().ParseYAML("dsn: postgres://h/db\npk: \"\"", nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := newInsertProcessor(pConf, nil); err == nil {
		t.Fatal("expected empty pk error, got nil")
	}
}

func TestAlreadyExists(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "42P07"}, true},
		{&pgconn.PgError{Code: "42P16"}, true},
		{&pgconn.PgError{Code: "23505"}, false},
		{errors.New("relation \"x\" already exists"), true},
		{errors.New("other"), false},
	} {
		if got := alreadyExists(tt.err); got != tt.want {
			t.Errorf("alreadyExists(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestInsertProcessorBatch(t *testing.T) {
	srv, hits := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	out, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL))
	if err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	if len(out) != 1 || len(out[0]) != 2 {
		t.Fatalf("expected the 2 messages to pass through, got batch of %d", len(out[0]))
	}

	if *hits != 1 {
		t.Errorf("schema fetched %d times, want 1 (cached)", *hits)
	}
	if sink.txns != 1 || sink.commits != 1 || sink.rollbacks != 0 {
		t.Errorf("txn accounting: txns=%d commits=%d rollbacks=%d", sink.txns, sink.commits, sink.rollbacks)
	}

	want := expectedModelExecs(t, testSchema, "landing_test", "id", false, twoMessageSpecs())
	assertExecs(t, sink, want)

	// The model must include the denormalized view and the schema registry.
	all := strings.Join(sink.execs, "\n")
	if !strings.Contains(all, `CREATE VIEW "v_landing_test_tags"`) {
		t.Error("expected the denormalized v_landing_test_tags view in the DDL")
	}
	if !strings.Contains(all, `CREATE TABLE "landing_test_registry"`) {
		t.Error("expected the landing_test_registry table in the DDL")
	}
}

func TestTechnicalHeadersStored(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	yaml := `dsn: postgres://user:pass@localhost:5432/db?sslmode=disable
schema_url_header: schema_url
table_name_header: table_name
pk: id
headers:
  source: TEXT
  trace_id: TEXT
create_model: true
`
	proc := newTestProcessor(t, false, yaml, sink)

	spec := map[string]string{"source": "TEXT", "trace_id": "TEXT"}
	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{
			"table_name": "landing_test", "schema_url": srv.URL, "source": "api", "trace_id": "t-0001",
		}),
		message(`{"id":2,"name":"beta"}`, map[string]string{
			"table_name": "landing_test", "schema_url": srv.URL, "source": "api2", "trace_id": "t-0002",
		}),
	}
	if _, err := proc.ProcessBatch(context.Background(), batch); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}

	model, err := st.PlanModel(testSchema, "landing_test", "id", spec)
	if err != nil {
		t.Fatalf("PlanModel: %v", err)
	}
	var want []string
	for _, render := range []func() ([]string, error){model.CreateStatements, model.ViewStatements, model.RegistryStatements} {
		stmts, err := render()
		if err != nil {
			t.Fatalf("render DDL: %v", err)
		}
		want = append(want, stmts...)
	}
	for _, m := range []testMsg{
		{payload: `{"id":1,"name":"alpha"}`, headers: map[string]string{"source": "api", "trace_id": "t-0001"}},
		{payload: `{"id":2,"name":"beta"}`, headers: map[string]string{"source": "api2", "trace_id": "t-0002"}},
	} {
		stmts, err := model.InsertStatements(m.headers, m.payload)
		if err != nil {
			t.Fatalf("render DML: %v", err)
		}
		want = append(want, stmts...)
	}
	assertExecs(t, sink, want)

	// Both technical headers must be materialized as columns and fed per message.
	all := strings.Join(sink.execs, "\n")
	if !strings.Contains(all, `"header_source" TEXT`) || !strings.Contains(all, `"header_trace_id" TEXT`) {
		t.Error("technical header columns missing from the root table DDL")
	}
	if !strings.Contains(sink.execs[len(sink.execs)-2], "'t-0001'") ||
		!strings.Contains(sink.execs[len(sink.execs)-1], "'t-0002'") {
		t.Errorf("per-message header values not propagated:\n%s", all)
	}
}

func TestInsertProcessorExistingModel(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	sink.exists["landing_test"] = true
	proc := newTestProcessor(t, false, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	want := expectedDMLExecs(t, testSchema, "landing_test", "id", false, twoMessageSpecs())
	assertExecs(t, sink, want)
}

func TestInsertProcessorCreateModelDisabled(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	yaml := strings.Replace(baseYAML(), "create_model: true", "create_model: false", 1)
	proc := newTestProcessor(t, false, yaml, sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	want := expectedDMLExecs(t, testSchema, "landing_test", "id", false, twoMessageSpecs())
	assertExecs(t, sink, want)
}

func TestUpsertProcessorBatch(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, true, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	want := expectedModelExecs(t, testSchema, "landing_test", "id", true, twoMessageSpecs())
	assertExecs(t, sink, want)
	if sink.commits != 1 || sink.rollbacks != 0 {
		t.Errorf("txn accounting: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

func TestUpsertProcessorExistingModel(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	sink.exists["landing_test"] = true
	proc := newTestProcessor(t, true, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	want := expectedDMLExecs(t, testSchema, "landing_test", "id", true, twoMessageSpecs())
	assertExecs(t, sink, want)
}

func TestMissingTableNameHeader(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{"schema_url": srv.URL}),
	}
	if _, err := proc.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatal("expected error for missing table_name header, got nil")
	}
	if sink.rollbacks != 1 || sink.commits != 0 || len(sink.execs) != 0 {
		t.Errorf("expected full rollback: txns=%d commits=%d rollbacks=%d execs=%d",
			sink.txns, sink.commits, sink.rollbacks, len(sink.execs))
	}
}

func TestMissingPkInPayload(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	batch := service.MessageBatch{
		message(`{"name":"alpha"}`, map[string]string{"table_name": "landing_test", "schema_url": srv.URL}),
	}
	if _, err := proc.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatal("expected error for payload missing the pk, got nil")
	}
	if sink.rollbacks != 1 || sink.commits != 0 {
		t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

func TestExecFailureRollsBackBatch(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	sink.failExec = errors.New("boom")
	proc := newTestProcessor(t, false, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err == nil {
		t.Fatal("expected exec error, got nil")
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

func TestSchemaTooLarge(t *testing.T) {
	huge := testSchema + strings.Repeat(" ", maxSchemaBytes)
	srv, _ := schemaServer(t, huge)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err == nil {
		t.Fatal("expected oversized schema error, got nil")
	}
	if sink.rollbacks != 1 {
		t.Errorf("expected rollback, got rollbacks=%d", sink.rollbacks)
	}
}

func TestMultiTableBatch(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{
			"table_name": "landing_one", "schema_url": srv.URL, "source": "x",
		}),
		message(`{"id":2,"name":"beta"}`, map[string]string{
			"table_name": "landing_two", "schema_url": srv.URL,
		}),
	}
	if _, err := proc.ProcessBatch(context.Background(), batch); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}

	// One full model (2 tables + view + registry) per root table, plus one
	// root insert each: the processor derives per-message models.
	want := expectedModelExecs(t, testSchema, "landing_one", "id", false, []testMsg{
		{payload: `{"id":1,"name":"alpha"}`, headers: map[string]string{"source": "x"}},
	})
	want = append(want, expectedModelExecs(t, testSchema, "landing_two", "id", false, []testMsg{
		{payload: `{"id":2,"name":"beta"}`, headers: map[string]string{"source": ""}},
	})...)
	assertExecs(t, sink, want)
}
