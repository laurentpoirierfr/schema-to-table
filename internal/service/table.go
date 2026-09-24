// Package service converts a JSON Schema (draft 2020-12) into PostgreSQL
// "landing" (staging) table DDL and DML statements, so a table can be fed
// from JSON documents plus ingestion headers.
//
// The pipeline is split into explicit stages:
//
//  1. schema parsing     — Schema / resolveRef        (schema.go)
//  2. column planning    — walkSchema / Columns       (columns.go)
//  3. value collection   — collectValues              (values.go)
//  4. SQL rendering      — CreateTable / Insert / Upsert (sql.go)
package service
