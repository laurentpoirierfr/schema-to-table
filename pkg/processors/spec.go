// Package processors provides Bento stream processor plugins that sink
// message batches into PostgreSQL landing tables derived from JSON Schema
// documents served over HTTP.
//
// Two batch processors are registered (blank-import this package in a
// Bento-enabled binary):
//
//   - schema_to_table_insert  — bulk INSERT of every message into a model
//   - schema_to_table_upsert  — bulk UPSERT (INSERT … ON CONFLICT DO UPDATE)
//
// Each processor builds the *normalized* relational model of the fetched
// JSON Schema — one typed table per array, denormalized root × child views
// (`v_<root>_<child>`) and the schema registry (`<root>_registry`), exactly
// like the CLI `-model` mode. The target root table name and the JSON Schema
// URL are read from each message's metadata headers (the header names are
// configured, see the component documentation). The model is created on the
// fly when the root table does not yet exist, and each processed batch runs
// inside a single database transaction so a failure leaves no partial rows
// behind.
package processors

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

// Bento component names.
const (
	insertProcessorName = "schema_to_table_insert"
	upsertProcessorName = "schema_to_table_upsert"

	onErrorAbort      = "abort"
	onErrorPerMessage = "per_message"
)

// processorConfig is the validated configuration of either processor.
type processorConfig struct {
	dsn             string            // PostgreSQL DSN (pgx)
	schemaURLHeader string            // message header carrying the http(s) JSON Schema URL
	tableNameHeader string            // message header carrying the target root table name
	pk              string            // root primary key (upsert conflict column)
	headers         map[string]string // header key → SQL type (root ingestion columns)
	createModel     bool              // create tables + views + registry when missing
	schemaTTL       time.Duration     // schema cache lifetime per URL (<=0 disables)
	onError         string            // abort | per_message
}

// baseFields returns the ConfigFields shared by both processors.
func baseFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("dsn").
			Description("PostgreSQL connection string (pgx DSN), e.g. `postgres://user:pass@host:5432/db?sslmode=disable`."),
		service.NewStringField("schema_url_header").
			Default("schema_url").
			Description("Name of the incoming message metadata header that holds the `http://` (or `https://`) URL of the JSON Schema (draft 2020-12) describing the messages of this stream."),
		service.NewStringField("table_name_header").
			Default("table_name").
			Description("Name of the incoming message metadata header that selects the destination root SQL table for that message."),
		service.NewStringField("pk").
			Default("id").
			Description("Root primary key column planned from the schema (also the `ON CONFLICT` target of the upsert processor); it must be present in the message payload."),
		service.NewStringMapField("headers").
			Default(map[string]string{}).
			Description("Extra ingestion columns injected from message headers, keyed by header name with the SQL type as value (each becomes a `header_<name>` column of the root table)."),
		service.NewBoolField("create_model").
			Default(true).
			Description("Whether to create the normalized model when the root table does not exist yet: every typed table, the denormalized `v_<root>_<child>` views and the `<root>_registry` schema registry."),
		service.NewDurationField("schema_ttl").
			Default("1h").
			Description("How long a fetched JSON Schema document is cached per URL before it is fetched again (e.g. `1h`, `30m`). Use `0` to disable the cache and fetch the schema on every message."),
		service.NewStringField("on_error").
			Default(onErrorAbort).
			Description("How a failed message is handled. `abort` (default) rolls the whole batch back and marks every message as failed: the batch is all-or-nothing. `per_message` keeps the transaction for messages that validate: a permanent failure (missing metadata header, unreachable or invalid schema document, or a payload that does not fit the model) rejects only the offending message with `message.SetError`, so a `switch`/`fallback` output routing `errored()` messages can dead-letter it. Infrastructure failures (connectivity, HTTP 5xx, transaction or SQL errors) always abort the batch."),
	}
}

// baseDescription is the paragraph shared by both processors: how the
// destination table and its schema are selected per message.
const baseDescription = `
This processor loads each incoming message into a PostgreSQL landing model
derived from a JSON Schema document: one typed table per array of the
schema, the denormalized root × child views (` + "`v_<root>_<child>`" + `) and the
schema registry (` + "`<root>_registry`" + `). The schema URL and the root table
name are taken from each message's metadata headers (the header names are
configurable, defaulting to ` + "`schema_url`" + ` and ` + "`table_name`" + `). When
the root table does not exist yet the whole model is created on demand
(governed by ` + "`create_model`" + `), and the batch is stored inside a single
transaction: if any message fails the transaction rolls back and no partial
rows remain.

Message content must be a JSON document matching the schema, and must carry
the primary key property (` + "`pk`" + `).
`

// insertSpec returns the configuration documentation for the insert processor.
func insertSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Sinks a batch of messages into a PostgreSQL landing model derived from a JSON Schema fetched over HTTP, using INSERT statements.").
		Description(baseDescription).
		Fields(baseFields()...).
		Example("Insert batch into a landing model",
			"Two JSON documents are stored into the per-message model with a source header:",
			`pipeline:
  processors:
    - schema_to_table_insert:
        dsn: postgres://user:pass@localhost:5432/db?sslmode=disable
        schema_url_header: schema_url
        table_name_header: table_name
        pk: id
        headers:
          source: TEXT
        create_model: true
        schema_ttl: 1h
        on_error: abort
`)
}

// upsertSpec returns the configuration documentation for the upsert processor.
func upsertSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Replaces rows of a PostgreSQL landing model keyed by the primary key, using UPSERT (INSERT … ON CONFLICT DO UPDATE) statements.").
		Description(baseDescription+`
The upsert processor behaves like the insert processor except it emits
`+"`INSERT … ON CONFLICT (pk) DO UPDATE`"+` statements keyed on the primary
key: each document replaces the previous rows sharing the same `+"`pk`"+`
value, including its child rows (arrays are replaced wholesale).
`).
		Fields(baseFields()...).
		Example("Upsert a batch keyed on id",
			"Documents replace the previous rows with the same `id`, and the id column becomes the model primary key at creation:",
			`pipeline:
  processors:
    - schema_to_table_upsert:
        dsn: postgres://user:pass@localhost:5432/db?sslmode=disable
        schema_url_header: schema_url
        table_name_header: table_name
        pk: id
        headers:
          source: TEXT
        schema_ttl: 1h
        on_error: abort
`)
}

// parseConfig validates and extracts the shared configuration.
func parseConfig(conf *service.ParsedConfig) (*processorConfig, error) {
	cfg := &processorConfig{
		headers:     map[string]string{},
		createModel: true,
	}
	var err error
	if cfg.dsn, err = conf.FieldString("dsn"); err != nil {
		return nil, err
	}
	if cfg.schemaURLHeader, err = conf.FieldString("schema_url_header"); err != nil {
		return nil, err
	}
	if cfg.tableNameHeader, err = conf.FieldString("table_name_header"); err != nil {
		return nil, err
	}
	if cfg.pk, err = conf.FieldString("pk"); err != nil {
		return nil, err
	}
	if cfg.headers, err = conf.FieldStringMap("headers"); err != nil {
		return nil, err
	}
	if cfg.createModel, err = conf.FieldBool("create_model"); err != nil {
		return nil, err
	}
	if cfg.schemaTTL, err = conf.FieldDuration("schema_ttl"); err != nil {
		return nil, err
	}
	if cfg.onError, err = conf.FieldString("on_error"); err != nil {
		return nil, err
	}
	if cfg.onError != onErrorAbort && cfg.onError != onErrorPerMessage {
		return nil, fmt.Errorf("on_error must be %q or %q, got %q", onErrorAbort, onErrorPerMessage, cfg.onError)
	}

	if strings.TrimSpace(cfg.dsn) == "" {
		return nil, errors.New("dsn is required")
	}
	if strings.TrimSpace(cfg.schemaURLHeader) == "" {
		return nil, errors.New("schema_url_header is required")
	}
	if strings.TrimSpace(cfg.tableNameHeader) == "" {
		return nil, errors.New("table_name_header is required")
	}
	if strings.TrimSpace(cfg.pk) == "" {
		return nil, errors.New("pk is required")
	}

	cfg.dsn = strings.TrimSpace(cfg.dsn)
	cfg.schemaURLHeader = strings.TrimSpace(cfg.schemaURLHeader)
	cfg.tableNameHeader = strings.TrimSpace(cfg.tableNameHeader)
	cfg.pk = strings.TrimSpace(cfg.pk)
	return cfg, nil
}
