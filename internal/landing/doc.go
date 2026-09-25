// Package landing converts a JSON Schema into a single PostgreSQL "landing"
// (staging) table: nested arrays and generic objects collapse into one
// complexType column, and it renders the matching CREATE / INSERT / UPSERT
// statements.
//
// The pipeline stages are: parse schema → plan columns → collect payload
// values → render SQL (primitives live in internal/schema).
package landing
