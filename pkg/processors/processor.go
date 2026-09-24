package processors

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/warpstreamlabs/bento/v4/public/service"

	st "github.com/laurentpoirierfr/schema-to-table/internal/service"
)

// batchProcessor implements service.BatchProcessor for both the insert and
// upsert variants: it plans the *normalized* relational model of the JSON
// Schema fetched at each message's schema_url_header URL (typed tables,
// denormalized views, schema registry), creates it on demand, and stores
// every message of the batch inside a single database transaction.
type batchProcessor struct {
	cfg    *processorConfig
	upsert bool // true → replace rows keyed on the root primary key
	loader *schemaLoader
	sink   sink // nil until the first batch opens the DSN connection

	mu     sync.Mutex
	opened bool
}

// ProcessBatch stores every message of batch inside one transaction.
// A missing header, an unreachable schema or a statement failure aborts the
// whole transaction and returns an error, so Bento marks the batch as
// failed without leaving partial rows behind.
func (p *batchProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if err := p.ensureSink(ctx); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	tr, err := p.sink.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tr.Rollback()
		}
	}()

	ensured := map[string]bool{}
	for _, msg := range batch {
		if err := p.storeMessage(ctx, tr, msg, ensured); err != nil {
			return nil, fmt.Errorf("store message: %w", err)
		}
	}

	if err := tr.Commit(); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	return []service.MessageBatch{batch}, nil
}

// storeMessage plans the landing model and executes the INSERT/UPSERT batch
// for one message.
func (p *batchProcessor) storeMessage(ctx context.Context, tr tx, msg *service.Message, ensured map[string]bool) error {
	tableName, ok := msg.MetaGet(p.cfg.tableNameHeader)
	tableName = strings.TrimSpace(tableName)
	if !ok || tableName == "" {
		return fmt.Errorf("metadata header %q (table name) is required", p.cfg.tableNameHeader)
	}

	schemaURL, ok := msg.MetaGet(p.cfg.schemaURLHeader)
	schemaURL = strings.TrimSpace(schemaURL)
	if !ok || schemaURL == "" {
		return fmt.Errorf("metadata header %q (schema URL) is required", p.cfg.schemaURLHeader)
	}

	schemaDoc, err := p.loader.Load(ctx, schemaURL)
	if err != nil {
		return err
	}

	model, err := st.PlanModel(schemaDoc, tableName, p.cfg.pk, p.cfg.headers)
	if err != nil {
		return fmt.Errorf("plan model from schema %s: %w", schemaURL, err)
	}

	// headerValues feeds the ingestion columns: one value per configured
	// header key, read from the message metadata (absent → empty).
	headerValues := make(map[string]string, len(p.cfg.headers))
	for k := range p.cfg.headers {
		headerValues[k], _ = msg.MetaGet(k)
	}

	if err := p.ensureModel(ctx, tr, tableName, model, ensured); err != nil {
		return fmt.Errorf("ensure model %s: %w", tableName, err)
	}

	data, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("read message contents: %w", err)
	}

	var stmts []string
	if p.upsert {
		stmts, err = model.UpsertStatements(headerValues, string(data), p.cfg.pk)
	} else {
		stmts, err = model.InsertStatements(headerValues, string(data))
	}
	if err != nil {
		return fmt.Errorf("render statements for %s: %w", tableName, err)
	}

	for i, stmt := range stmts {
		if err := tr.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec %s statement %d/%d: %w", tableName, i+1, len(stmts), err)
		}
	}
	return nil
}

// ensureModel creates the normalized model on demand (inside the same
// transaction): every typed table first, then the denormalized views, then
// the schema registry. The root table's existence guards the whole object,
// so creations are skipped once it is known to exist for this table.
func (p *batchProcessor) ensureModel(ctx context.Context, tr tx, tableName string, model *st.Model, ensured map[string]bool) error {
	if !p.cfg.createModel {
		return nil
	}
	if ensured[tableName] {
		return nil
	}

	exists, err := tr.TableExists(ctx, tableName)
	if err != nil {
		return err
	}
	if !exists {
		if err := p.execStatements(ctx, tr, model.CreateStatements); err != nil {
			return err
		}
		if err := p.execStatements(ctx, tr, model.ViewStatements); err != nil {
			return err
		}
		if err := p.execStatements(ctx, tr, model.RegistryStatements); err != nil {
			return err
		}
	}
	ensured[tableName] = true
	return nil
}

// execStatements renders and executes a batch of DDL statements, tolerating
// "already exists" races from concurrent streams sharing the same schema.
func (p *batchProcessor) execStatements(ctx context.Context, tr tx, render func() ([]string, error)) error {
	stmts, err := render()
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if err := tr.Exec(ctx, stmt); err != nil {
			if alreadyExists(err) {
				continue
			}
			return err
		}
	}
	return nil
}

// ensureSink lazily opens the database connection on first use.
func (p *batchProcessor) ensureSink(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.opened {
		return nil
	}
	s, err := openPGSink(p.cfg.dsn)
	if err != nil {
		return err
	}
	if err := s.Ping(ctx); err != nil {
		return err
	}
	p.sink = s
	p.opened = true
	return nil
}

// Close releases the database connection, if any.
func (p *batchProcessor) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sink != nil {
		err := p.sink.Close()
		p.sink = nil
		p.opened = false
		return err
	}
	return nil
}
