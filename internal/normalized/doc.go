// Package normalized plans and renders the fully-tabular ("normalized")
// relational model of a JSON Schema: one typed table per array, the
// denormalized root × child views and the schema registry. It renders the
// DDL and the INSERT / UPSERT batches loading one JSON document into the
// model (primitives live in internal/schema).
package normalized
