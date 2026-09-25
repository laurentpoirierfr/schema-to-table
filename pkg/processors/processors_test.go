package processors

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/warpstreamlabs/bento/v4/public/service"

	normalized "github.com/laurentpoirierfr/schema-to-table/internal/normalized"
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
	failErrs  []error // per-Exec error queue, consumed in order
	pingErr   error
	beginErr  error
	commitErr error
	existsErr error
}

func newFakeSink() *fakeSink {
	return &fakeSink{exists: map[string]bool{}, created: map[string]bool{}}
}

func (s *fakeSink) Ping(context.Context) error { return s.pingErr }
func (s *fakeSink) Begin(context.Context) (tx, error) {
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	s.txns++
	return &fakeTx{s: s}, nil
}
func (s *fakeSink) Close() error { return nil }

type fakeTx struct{ s *fakeSink }

func (t *fakeTx) Exec(_ context.Context, query string) error {
	if len(t.s.failErrs) > 0 {
		err := t.s.failErrs[0]
		t.s.failErrs = t.s.failErrs[1:]
		if err != nil {
			return err
		}
	} else if t.s.failExec != nil {
		return t.s.failExec
	}
	t.s.execs = append(t.s.execs, query)
	if name := createdTableName(query); name != "" {
		t.s.created[name] = true
	}
	return nil
}

func (t *fakeTx) TableExists(_ context.Context, name string) (bool, error) {
	if t.s.existsErr != nil {
		return false, t.s.existsErr
	}
	return t.s.exists[name] || t.s.created[name], nil
}
func (t *fakeTx) Commit() error {
	if t.s.commitErr != nil {
		return t.s.commitErr
	}
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
	model, err := normalized.Plan(schemaDoc, table, pk, map[string]string{"source": "TEXT"})
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
	model, err := normalized.Plan(schemaDoc, table, pk, map[string]string{"source": "TEXT"})
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
	// Both components must be resolvable through the global environment.
	env := service.NewEnvironment()
	if _, ok := env.GetProcessorConfig(insertProcessorName); !ok {
		t.Errorf("%s not registered", insertProcessorName)
	}
	if _, ok := env.GetProcessorConfig(upsertProcessorName); !ok {
		t.Errorf("%s not registered", upsertProcessorName)
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

	// Empty header field names are refused.
	for _, field := range []string{"schema_url_header", "table_name_header"} {
		pConf, err = insertSpec().ParseYAML(fmt.Sprintf("dsn: postgres://h/db\n%s: \"\"", field), nil)
		if err != nil {
			t.Fatalf("parse (%s): %v", field, err)
		}
		if _, err := newInsertProcessor(pConf, nil); err == nil {
			t.Errorf("expected empty %s error, got nil", field)
		}
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

	model, err := normalized.Plan(testSchema, "landing_test", "id", spec)
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

func onErrorBatch(srvURL string) service.MessageBatch {
	return service.MessageBatch{
		message(`{"id":1,"name":"alpha","tags":["a"]}`, map[string]string{
			"table_name": "landing_test", "schema_url": srvURL, "source": "api",
		}),
		message(`{"name":"bad-no-pk"}`, map[string]string{
			"table_name": "landing_test", "schema_url": srvURL, "source": "api",
		}),
		message(`{"id":3,"name":"gamma","tags":[]}`, map[string]string{
			"table_name": "landing_test", "schema_url": srvURL, "source": "batch",
		}),
	}
}

// TestOnErrorPerMessageKeepsValidMessages: with on_error: per_message the
// permanently-failing message is rejected individually — its error is set and
// it contributes no DML — while the valid messages are committed.
func TestOnErrorPerMessageKeepsValidMessages(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML()+"on_error: per_message\n", sink)

	batch := onErrorBatch(srv.URL)
	out, err := proc.ProcessBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	if len(out) != 1 || len(out[0]) != 3 {
		t.Fatalf("expected the 3 messages through, got %d", len(out))
	}
	if out[0][0].GetError() != nil || out[0][2].GetError() != nil {
		t.Errorf("valid messages must be marked clean, got errors %v / %v", out[0][0].GetError(), out[0][2].GetError())
	}
	if out[0][1].GetError() == nil {
		t.Error("the message missing its pk must carry a permanent error")
	}

	if sink.commits != 1 || sink.rollbacks != 0 {
		t.Errorf("expected a single commit, no rollback: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}

	want := expectedModelExecs(t, testSchema, "landing_test", "id", false, []testMsg{
		{payload: `{"id":1,"name":"alpha","tags":["a"]}`, headers: map[string]string{"source": "api"}},
		{payload: `{"id":3,"name":"gamma","tags":[]}`, headers: map[string]string{"source": "batch"}},
	})
	assertExecs(t, sink, want)
}

// TestOnErrorDefaultAbort: the default aborts the whole batch on any failure:
// nothing after the failing message is stored and the transaction rolls back.
func TestOnErrorDefaultAbort(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), onErrorBatch(srv.URL)); err == nil {
		t.Fatal("expected a batch error, got nil")
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected full rollback: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

// TestOnErrorPerMessageRecoverableStillAborts: even in per_message mode an
// infrastructure failure (here an HTTP 500 answering the schema URL) aborts
// the batch instead of rejecting a single message.
func TestOnErrorPerMessageRecoverableStillAborts(t *testing.T) {
	boom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(boom.Close)

	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML()+"on_error: per_message\n", sink)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{
			"table_name": "landing_test", "schema_url": boom.URL,
		}),
	}
	if _, err := proc.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatal("expected a batch error for the HTTP 500, got nil")
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected full rollback: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

func TestOnErrorValueValidated(t *testing.T) {
	pConf, err := insertSpec().ParseYAML("dsn: postgres://h/db\non_error: bogus", nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := newInsertProcessor(pConf, nil); err == nil {
		t.Fatal("expected an on_error validation error, got nil")
	}
}

// TestWireResources: a Bento Resources wires the logger, the metric counters
// (including the schema loader's) into the processor.
func TestWireResources(t *testing.T) {
	yaml := "dsn: postgres://user:pass@localhost:5432/db?sslmode=disable"
	for _, name := range []string{insertProcessorName, upsertProcessorName} {
		t.Run(name, func(t *testing.T) {
			spec := insertSpec()
			if name == upsertProcessorName {
				spec = upsertSpec()
			}
			pConf, err := spec.ParseYAML(yaml, nil)
			if err != nil {
				t.Fatalf("parse yaml: %v", err)
			}
			var (
				proc service.BatchProcessor
				err2 error
			)
			if name == upsertProcessorName {
				proc, err2 = newUpsertProcessor(pConf, service.MockResources())
			} else {
				proc, err2 = newInsertProcessor(pConf, service.MockResources())
			}
			if err2 != nil {
				t.Fatalf("constructor: %v", err2)
			}
			bp := proc.(*batchProcessor)
			if bp.logger == nil {
				t.Error("logger must be wired from Resources")
			}
			if bp.loader == nil || bp.loader.cacheHits == nil || bp.loader.fetches == nil {
				t.Error("loader and its schema counters must be wired")
			}
			for name, c := range map[string]*service.MetricCounter{
				"batches_processed": bp.batchesProcessed, "batches_failed": bp.batchesFailed,
				"messages_processed": bp.messagesProcessed, "permanent_errors": bp.permanentErrors,
			} {
				if c == nil {
					t.Errorf("counter %s must be wired", name)
				}
			}
		})
	}
}

// TestOnErrorPerMessageWired: with Bento resources wired, a rejected
// permanent failure logs and increments the counter while keeping the
// transaction for the valid messages.
func TestOnErrorPerMessageWired(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	batch := service.MessageBatch{
		message(`{"name":"bad-no-pk"}`, map[string]string{"table_name": "landing_test", "schema_url": srv.URL}),
	}

	pConf, err := insertSpec().ParseYAML(baseYAML()+"on_error: per_message\n", nil)
	if err != nil {
		t.Fatalf("parse yaml: %v", err)
	}
	proc, err := newInsertProcessor(pConf, service.MockResources())
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	bp := proc.(*batchProcessor)
	bp.sink = newFakeSink()
	bp.opened = true

	out, err := bp.ProcessBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	if len(out) != 1 || out[0][0].GetError() == nil {
		t.Fatalf("expected the rejected message to carry its permanent error")
	}
}

func TestMetaGetFold(t *testing.T) {
	msg := service.NewMessage([]byte("{}"))
	msg.MetaSet("Schema_Url", "http://x/schema.json")
	msg.MetaSet("table_name", "landing_t")
	msg.MetaSet("SOURCE", "kafka")

	for key, want := range map[string]string{
		// the configured spelling must match the actual (canonicalized) one.
		"schema_url": "http://x/schema.json",
		"SCHEMA_URL": "http://x/schema.json",
		"table_name": "landing_t",
		"TABLE_NAME": "landing_t",
		"source":     "kafka",
		"Source":     "kafka",
	} {
		got, ok := metaGetFold(msg, key)
		if !ok || got != want {
			t.Errorf("metaGetFold(%q) = %q,%v ; want %q,true", key, got, ok, want)
		}
	}

	if got, ok := metaGetFold(msg, "absent"); ok || got != "" {
		t.Errorf("metaGetFold(absent) = %q,%v ; want empty,false", got, ok)
	}

	// an exact match always wins over a case-fold match.
	msg.MetaSet("schema_url", "exact")
	if got, ok := metaGetFold(msg, "Schema_Url"); !ok {
		t.Error("metaGetFold(Schema_Url) lost, ok=false")
	} else if got != "http://x/schema.json" {
		t.Errorf("exact match should win: got %q", got)
	}
	if got, _ := metaGetFold(msg, "schema_url"); got != "exact" {
		t.Errorf("exact match should win: got %q", got)
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

func TestMissingSchemaURLHeader(t *testing.T) {
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{"table_name": "landing_test"}),
	}
	if _, err := proc.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatal("expected error for missing schema_url header, got nil")
	}
	if sink.rollbacks != 1 || sink.commits != 0 || len(sink.execs) != 0 {
		t.Errorf("expected full rollback: txns=%d commits=%d rollbacks=%d execs=%d",
			sink.txns, sink.commits, sink.rollbacks, len(sink.execs))
	}
}

func TestBlankTableNameHeader(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{
			"table_name": "   ", "schema_url": srv.URL,
		}),
	}
	if _, err := proc.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatal("expected error for blank table_name header, got nil")
	}
	if sink.rollbacks != 1 || sink.commits != 0 {
		t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

func TestSchemaFetchError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)
	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{
			"table_name": "landing_test", "schema_url": srv.URL,
		}),
	}
	_, err := proc.ProcessBatch(context.Background(), batch)
	if err == nil {
		t.Fatal("expected error fetching the schema, got nil")
	}
	if !strings.Contains(err.Error(), "HTTP 500") {
		t.Errorf("expected HTTP status in the error, got: %v", err)
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

func TestInvalidSchemaDocument(t *testing.T) {
	srv, _ := schemaServer(t, "{ not json")
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{
			"table_name": "landing_test", "schema_url": srv.URL,
		}),
	}
	_, err := proc.ProcessBatch(context.Background(), batch)
	if err == nil {
		t.Fatal("expected error planning the model from an invalid schema, got nil")
	}
	if !strings.Contains(err.Error(), "plan model") {
		t.Errorf("expected a plan-model error, got: %v", err)
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

// TestDdlAlreadyExistsTolerated checks that a concurrent stream already
// owning the model does not abort the batch: CREATE ... already exists is
// skipped, the rest of the model and the DML still run and commit.
func TestDdlAlreadyExistsTolerated(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	sink.failErrs = []error{
		&pgconn.PgError{Code: "42P07"},
		&pgconn.PgError{Code: "42P16"},
	}
	proc := newTestProcessor(t, true, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	if sink.commits != 1 || sink.rollbacks != 0 {
		t.Errorf("txn accounting: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}

	// Both per-table DDL strings hit "already exists" and were skipped; only
	// the view, the registry and the DML statements were executed and stored.
	model, err := normalized.Plan(testSchema, "landing_test", "id", map[string]string{"source": "TEXT"})
	if err != nil {
		t.Fatalf("PlanModel: %v", err)
	}
	var want []string
	for _, render := range []func() ([]string, error){model.ViewStatements, model.RegistryStatements} {
		stmts, err := render()
		if err != nil {
			t.Fatalf("render DDL: %v", err)
		}
		want = append(want, stmts...)
	}
	for _, m := range []testMsg{
		{payload: `{"id":1,"name":"alpha","tags":["a","b"]}`, headers: map[string]string{"source": "api"}},
		{payload: `{"id":2,"name":"beta","tags":[]}`, headers: map[string]string{"source": ""}},
	} {
		stmts, err := model.UpsertStatements(m.headers, m.payload, "id")
		if err != nil {
			t.Fatalf("render DML: %v", err)
		}
		want = append(want, stmts...)
	}
	assertExecs(t, sink, want)
}

func TestBeginError(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	sink.beginErr = errors.New("no connections left")
	proc := newTestProcessor(t, false, baseYAML(), sink)

	_, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL))
	if err == nil || !strings.Contains(err.Error(), "begin transaction") {
		t.Fatalf("expected a begin-transaction error, got: %v", err)
	}
	if sink.txns != 0 || sink.commits != 0 || sink.rollbacks != 0 {
		t.Errorf("no transaction should have started: txns=%d commits=%d rollbacks=%d",
			sink.txns, sink.commits, sink.rollbacks)
	}
}

func TestCommitError(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	sink.commitErr = errors.New("commit boom")
	proc := newTestProcessor(t, false, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err == nil {
		t.Fatal("expected commit error, got nil")
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected the deferred rollback after a failed commit: commits=%d rollbacks=%d",
			sink.commits, sink.rollbacks)
	}
}

// TestSinkPingFailure exercises the lazy-connect path of ensureSink against a
// port that is guaranteed closed (a listener we create and immediately drop).
func TestSinkPingFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	pConf, err := insertSpec().ParseYAML(fmt.Sprintf(
		"dsn: postgres://u:p@127.0.0.1:%d/db?sslmode=disable&connect_timeout=1\npk: id\n", port), nil)
	if err != nil {
		t.Fatalf("parse yaml: %v", err)
	}
	cfg, err := parseConfig(pConf)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	bp := &batchProcessor{cfg: cfg, loader: newSchemaLoader(time.Hour)}
	t.Cleanup(func() { _ = bp.Close(context.Background()) })

	batch := service.MessageBatch{message(`{"id":1}`, nil)}
	if _, err := bp.ProcessBatch(context.Background(), batch); err == nil || !strings.Contains(err.Error(), "connect:") {
		t.Fatalf("expected a connect error, got: %v", err)
	}
	if bp.opened || bp.sink != nil {
		t.Error("processor must stay unopened after a failed connect")
	}
}

func TestCloseWhenNotOpened(t *testing.T) {
	bp := &batchProcessor{cfg: &processorConfig{dsn: "postgres://u:p@localhost/db"}, loader: newSchemaLoader(time.Hour)}
	if err := bp.Close(context.Background()); err != nil {
		t.Fatalf("Close on a never-opened processor must return nil, got: %v", err)
	}
}

func TestExecutionOrderPreserved(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	proc := newTestProcessor(t, false, baseYAML(), sink)

	if _, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL)); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}

	// Every statement of the first message's model must precede the DML of the
	// first message, well before the DML of the second.
	all := strings.Join(sink.execs, "\n")
	first := strings.Index(all, "'alpha'")
	second := strings.Index(all, "'beta'")
	if first < 0 || second < 0 || first > second {
		t.Errorf("expected per-message DML in batch order:\n%s", all)
	}
}

// TestDdlStageFailure covers the propagation (and rollback) of a DDL failure
// in every stage of the on-hold model creation: per-table CREATE, the
// denormalized views and the registry.
func TestDdlStageFailure(t *testing.T) {
	stages := []struct {
		name string
		// failures are consumed before each expected statement while the
		// model is created for testSchema: 2 table DDL, 1 view, 1 registry.
		failures []error
		wantText string
	}{
		{"create", []error{&pgconn.PgError{Code: "23505"}}, "ensure model"},
		{"view", []error{nil, nil, errors.New("view boom")}, "ensure model"},
		{"registry", []error{nil, nil, nil, errors.New("registry boom")}, "ensure model"},
	}
	for _, tt := range stages {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := schemaServer(t, testSchema)
			sink := newFakeSink()
			sink.failErrs = tt.failures
			proc := newTestProcessor(t, false, baseYAML(), sink)

			batch := service.MessageBatch{
				message(`{"id":1,"name":"alpha"}`, map[string]string{
					"table_name": "landing_test", "schema_url": srv.URL, "source": "api",
				}),
			}
			_, err := proc.ProcessBatch(context.Background(), batch)
			if err == nil {
				t.Fatal("expected a DDL failure, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Errorf("error %q does not mention %q", err, tt.wantText)
			}
			if sink.commits != 0 || sink.rollbacks != 1 {
				t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
			}
		})
	}
}

// TestDmlFailureAfterDdl covers the error branch of the per-message statement
// loop: the model DDL succeeds, then a DML statement fails and the whole
// batch rolls back (nothing committed, rollback called).
func TestDmlFailureAfterDdl(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	// 2 table DDL + 1 view + 1 registry succeed, then the root INSERT fails.
	sink.failErrs = []error{nil, nil, nil, nil, errors.New("dml boom")}
	proc := newTestProcessor(t, false, baseYAML(), sink)

	batch := service.MessageBatch{
		message(`{"id":1,"name":"alpha"}`, map[string]string{
			"table_name": "landing_test", "schema_url": srv.URL, "source": "api",
		}),
	}
	_, err := proc.ProcessBatch(context.Background(), batch)
	if err == nil || !strings.Contains(err.Error(), "dml boom") {
		t.Fatalf("expected the dml failure, got: %v", err)
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
	// Only the DDL statements were attempted; the failed INSERT did not land.
	if len(sink.execs) != 4 {
		t.Errorf("expected exactly the 4 DDL statements to be recorded, got %d", len(sink.execs))
	}
}

// TestTableExistsError covers the propagation (and rollback) of a failure
// while probing the existence of the root table.
func TestTableExistsError(t *testing.T) {
	srv, _ := schemaServer(t, testSchema)
	sink := newFakeSink()
	sink.existsErr = errors.New("catalog gone")
	proc := newTestProcessor(t, false, baseYAML(), sink)

	_, err := proc.ProcessBatch(context.Background(), twoMessages(srv.URL))
	if err == nil || !strings.Contains(err.Error(), "catalog gone") {
		t.Fatalf("expected the catalog error, got: %v", err)
	}
	if sink.commits != 0 || sink.rollbacks != 1 {
		t.Errorf("expected rollback, no commit: commits=%d rollbacks=%d", sink.commits, sink.rollbacks)
	}
}

// TestParseConfigMissingFields covers the defensive field-access errors of
// parseConfig, reached when the config does not declare every field the
// processors rely on.
func TestParseConfigMissingFields(t *testing.T) {
	ctor := map[string]func(name string) *service.ConfigField{
		"schema_url_header": service.NewStringField,
		"table_name_header": service.NewStringField,
		"pk":                service.NewStringField,
		"headers":           service.NewStringMapField,
		"create_model":      service.NewBoolField,
		"schema_ttl":        service.NewDurationField,
		"on_error":          service.NewStringField,
	}
	// ParseYAML is fed a spec without the target field: the accessor inside
	// parseConfig then fails with "field ... was not present".
	tests := []struct {
		name   string
		absent string
		kept   []string
	}{
		{"no schema_url_header", "schema_url_header", []string{"table_name_header", "pk", "create_model", "schema_ttl", "on_error"}},
		{"no table_name_header", "table_name_header", []string{"schema_url_header", "pk", "create_model", "schema_ttl", "on_error"}},
		{"no pk", "pk", []string{"schema_url_header", "table_name_header", "create_model", "schema_ttl", "on_error"}},
		{"no headers", "headers", []string{"schema_url_header", "table_name_header", "pk", "create_model", "schema_ttl", "on_error"}},
		{"no create_model", "create_model", []string{"schema_url_header", "table_name_header", "pk", "headers", "schema_ttl", "on_error"}},
		{"no schema_ttl", "schema_ttl", []string{"schema_url_header", "table_name_header", "pk", "headers", "create_model", "on_error"}},
		{"no on_error", "on_error", []string{"schema_url_header", "table_name_header", "pk", "headers", "create_model", "schema_ttl"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := []*service.ConfigField{service.NewStringField("dsn")}
			yaml := "dsn: x\n"
			for _, k := range tt.kept {
				fields = append(fields, ctor[k](k))
				switch k {
				case "headers":
					yaml += "headers: {}\n"
				case "schema_ttl":
					yaml += "schema_ttl: 1h\n"
				case "on_error":
					yaml += "on_error: abort\n"
				default:
					yaml += k + ": y\n"
				}
			}
			conf, err := service.NewConfigSpec().Fields(fields...).ParseYAML(yaml, nil)
			if err != nil {
				t.Fatalf("parse config: %v", err)
			}
			if _, err := parseConfig(conf); err == nil || !strings.Contains(err.Error(), tt.absent) {
				t.Errorf("expected a field error mentioning %q, got: %v", tt.absent, err)
			}
		})
	}
}
