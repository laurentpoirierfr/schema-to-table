package schema

// Column is one planned SQL column: Name is the final (sanitized)
// identifier, Type the SQL type (or, once rendered, the literal is kept
// separately by the value stage).
type Column struct {
	Name string
	Type string
}
