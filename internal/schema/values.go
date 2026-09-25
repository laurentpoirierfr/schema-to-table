package schema

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// CollectValues mirrors the column-planning walks, but reads actual values
// out of dataNode (the unmarshalled JSON payload) at each property path and
// stores them in out, keyed by the sanitized column name. Columns present
// in the schema but absent from the payload simply never appear — the SQL
// rendering stage turns those into NULL, guaranteeing that value order
// always matches the planned column order.
func CollectValues(prefix string, s, root *Node, dataNode interface{}, complexType string, out map[string]string, visited map[*Node]bool) error {
	if s == nil {
		return nil
	}

	resolved, err := Resolve(s, root)
	if err != nil {
		return err
	}
	if visited[resolved] {
		return nil // cycle guard
	}
	visited[resolved] = true
	defer delete(visited, resolved)

	for _, sub := range resolved.AllOf {
		if err := CollectValues(prefix, sub, root, dataNode, complexType, out, visited); err != nil {
			return err
		}
	}

	for _, sub := range append(append([]*Node{}, resolved.OneOf...), resolved.AnyOf...) {
		if err := CollectValues(prefix, sub, root, dataNode, complexType, out, visited); err != nil {
			return err
		}
	}

	if len(resolved.Properties) == 0 {
		return nil
	}

	for _, key := range SortedKeys(resolved.Properties) {
		prop := resolved.Properties[key]
		segment := ToSnake(key)
		childPrefix := segment
		if prefix != "" {
			childPrefix = prefix + "_" + segment
		}
		childData := getChild(dataNode, key)

		propResolved, err := Resolve(prop, root)
		if err != nil {
			return err
		}

		switch {
		case propResolved.Items != nil || PrimaryType(propResolved) == "array":
			out[SanitizeIdent(childPrefix)] = SerializeComplex(childData, complexType)
		case len(propResolved.Properties) > 0 || len(propResolved.AllOf) > 0 ||
			len(propResolved.OneOf) > 0 || len(propResolved.AnyOf) > 0:
			if err := CollectValues(childPrefix, propResolved, root, childData, complexType, out, visited); err != nil {
				return err
			}
		case PrimaryType(propResolved) == "object":
			out[SanitizeIdent(childPrefix)] = SerializeComplex(childData, complexType)
		default:
			out[SanitizeIdent(childPrefix)] = FormatLiteral(childData, propResolved)
		}
	}

	return nil
}

// ParsePayload unmarshals a JSON document for the value stage.
func ParsePayload(data string) (interface{}, error) {
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

// SerializeComplex renders an array/object value as a JSON-text SQL
// literal, cast to complexType when it looks like a JSON(B) column.
func SerializeComplex(v interface{}, complexType string) string {
	if v == nil {
		return "NULL"
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return "NULL"
	}
	lit := QuoteLiteral(string(raw))
	if strings.Contains(strings.ToUpper(complexType), "JSON") {
		return lit + "::" + complexType
	}
	return lit
}

// FormatLiteral renders a scalar JSON value (as produced by
// encoding/json's default unmarshalling into interface{}) as a SQL literal.
func FormatLiteral(v interface{}, s *Node) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return QuoteLiteral(val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case float64:
		if PrimaryType(s) == "integer" {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		// Unexpected shape (schema/data mismatch) — fall back to JSON text.
		raw, err := json.Marshal(val)
		if err != nil {
			return "NULL"
		}
		return QuoteLiteral(string(raw))
	}
}
