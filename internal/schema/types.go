package schema

import "encoding/json"

// PrimaryType returns the node's declared type, taking the first
// non-null entry when "type" is an array (e.g. ["string","null"]).
func PrimaryType(s *Node) string {
	if s == nil || len(s.Type) == 0 {
		return ""
	}
	var single string
	if err := json.Unmarshal(s.Type, &single); err == nil {
		return single
	}
	var multi []string
	if err := json.Unmarshal(s.Type, &multi); err == nil {
		for _, t := range multi {
			if t != "null" {
				return t
			}
		}
	}
	return ""
}

// SQLType maps a leaf JSON Schema node to a PostgreSQL column type.
// complexType is used for a generic/untyped object (no declared properties).
func SQLType(s *Node, complexType string) string {
	switch PrimaryType(s) {
	case "integer":
		return "BIGINT"
	case "number":
		return "NUMERIC"
	case "boolean":
		return "BOOLEAN"
	case "object":
		return complexType // generic/untyped object
	case "string":
		switch s.Format {
		case "uuid":
			return "UUID"
		case "date-time":
			return "TIMESTAMPTZ"
		case "date":
			return "DATE"
		default:
			return "TEXT"
		}
	default:
		return "TEXT"
	}
}
