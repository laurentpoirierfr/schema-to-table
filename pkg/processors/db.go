package processors

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// tx is a database transaction, so a whole batch is stored atomically:
// DDL (create table) and DML run inside the same transaction and either
// all succeed or none does.
type tx interface {
	Exec(ctx context.Context, query string) error
	TableExists(ctx context.Context, name string) (bool, error)
	Commit() error
	Rollback() error
}

// sink is the PostgreSQL surface the processors need.
type sink interface {
	Ping(ctx context.Context) error
	Begin(ctx context.Context) (tx, error)
	Close() error
}

// pgSink adapts *sql.DB (pgx driver) to the sink interface.
type pgSink struct {
	db *sql.DB
}

func openPGSink(dsn string) (sink, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	return &pgSink{db: db}, nil
}

func (s *pgSink) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *pgSink) Close() error                   { return s.db.Close() }

func (s *pgSink) Begin(ctx context.Context) (tx, error) {
	t, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &pgTx{tx: t}, nil
}

type pgTx struct {
	tx *sql.Tx
}

func (t *pgTx) Exec(ctx context.Context, query string) error {
	_, err := t.tx.ExecContext(ctx, query)
	return err
}

func (t *pgTx) TableExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	err := t.tx.QueryRowContext(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1
		)`, name).Scan(&exists)
	return exists, err
}

func (t *pgTx) Commit() error   { return t.tx.Commit() }
func (t *pgTx) Rollback() error { return t.tx.Rollback() }

// alreadyExists reports whether err means the object already exists (e.g. a
// concurrent CREATE from another stream, or an already-applied constraint),
// which our create-if-missing strategy tolerates.
func alreadyExists(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42P07", "42P16": // duplicate_table, invalid_table_definition
			return true
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}
