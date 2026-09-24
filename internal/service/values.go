package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// collectValues (stage 3) mirrors walkSchema, but reads actual values out
// of dataNode (the unmarshalled JSON payload) at each property path and
// stores them in out, keyed by the sanitized column name. Columns present
// in the schema but absent from the payload simply never appear — the SQL
// rendering stage turns those into NULL, guaranteeing that value order
// always matches the planned column order.
func collectValues(prefix string, s, root *jsonSchema, dataNode interface{}, complexType string, out map[string]string, visited map[*jsonSchema]bool) error {
	if s == nil {
		return nil
	}

	resolved, err := resolveRef(s, root)
	if err != nil {
		return err
	}
	if visited[resolved] {
		return nil // cycle guard
	}
	visited[resolved] = true
	defer delete(visited, resolved)

	for _, sub := range resolved.AllOf {
		if err := collectValues(prefix, sub, root, dataNode, complexType, out, visited); err != nil {
			return err
		}
	}

	for _, sub := range append(append([]*jsonSchema{}, resolved.OneOf...), resolved.AnyOf...) {
		if err := collectValues(prefix, sub, root, dataNode, complexType, out, visited); err != nil {
			return err
		}
	}

	if len(resolved.Properties) == 0 {
		return nil
	}

	for _, key := range sortedKeys(resolved.Properties) {
		prop := resolved.Properties[key]
		segment := toSnake(key)
		childPrefix := segment
		if prefix != "" {
			childPrefix = prefix + "_" + segment
		}
		childData := getChild(dataNode, key)

		propResolved, err := resolveRef(prop, root)
		if err != nil {
			return err
		}

		switch {
		case propResolved.Items != nil || primaryType(propResolved) == "array":
			out[sanitizeIdent(childPrefix)] = serializeComplex(childData, complexType)
		case len(propResolved.Properties) > 0 || len(propResolved.AllOf) > 0 ||
			len(propResolved.OneOf) > 0 || len(propResolved.AnyOf) > 0:
			if err := collectValues(childPrefix, propResolved, root, childData, complexType, out, visited); err != nil {
				return err
			}
		case primaryType(propResolved) == "object":
			out[sanitizeIdent(childPrefix)] = serializeComplex(childData, complexType)
		default:
			out[sanitizeIdent(childPrefix)] = formatLiteral(childData, propResolved)
		}
	}

	return nil
}

// parsePayload unmarshals a JSON document for the value stage.
func parsePayload(data string) (interface{}, error) {
	var dataNode interface{}
	if err := json.Unmarshal([]byte(data), &dataNode); err != nil {
		return nil, fmt.Errorf("invalid data: %w", err)
	}
	return dataNode, nil
}

// getChild reads key out of dataNode when it's a JSON object; returns nil
// for any other case, including a missing key.
func getChild(dataNode interface{}, key string) interface{} {
	m, ok := dataNode.(map[string]interface{})
	if !ok {
		return nil
	}
	return m[key]
}

// serializeComplex renders an array/object value as a JSON-text SQL
// literal, cast to complexType when it looks like a JSON(B) column.
func serializeComplex(v interface{}, complexType string) string {
	if v == nil {
		return "NULL"
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return "NULL"
	}
	lit := quoteLiteral(string(raw))
	if strings.Contains(strings.ToUpper(complexType), "JSON") {
		return lit + "::" + complexType
	}
	return lit
}

// formatLiteral renders a scalar JSON value (as produced by
// encoding/json's default unmarshalling into interface{}) as a SQL literal.
func formatLiteral(v interface{}, s *jsonSchema) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return quoteLiteral(val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case float64:
		if primaryType(s) == "integer" {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		// Unexpected shape (schema/data mismatch) — fall back to JSON text.
		raw, err := json.Marshal(val)
		if err != nil {
			return "NULL"
		}
		return quoteLiteral(string(raw))
	}
}
