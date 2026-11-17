// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

var yamlErrorLine = regexp.MustCompile(`line ([0-9]+)`)

// Never forward decoder error text: it can quote passwords, PEM data, or other
// supplied values. Walk the document only after a decode failure to locate the
// bad field without changing the decoder's accepted configuration format.
func configurationError(data []byte, format string, decodeErr error, schema reflect.Type) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err == nil && len(document.Content) == 1 {
		remaining := 10000
		if err := diagnoseNode(document.Content[0], schema, format, "", 0, &remaining); err != nil {
			return err
		}
	}
	if format == "json" {
		var syntax *json.SyntaxError
		var mismatch *json.UnmarshalTypeError
		switch {
		case errors.As(decodeErr, &syntax):
			return fmt.Errorf("invalid configuration JSON at byte %d: check syntax", syntax.Offset)
		case errors.As(decodeErr, &mismatch):
			return fmt.Errorf("configuration JSON at byte %d, field %s: expected %s", mismatch.Offset, mismatch.Field, mismatch.Type)
		}
		return errors.New("invalid configuration JSON: expected a complete object; check syntax and field types")
	}
	if line := yamlErrorLine.FindStringSubmatch(decodeErr.Error()); len(line) > 1 {
		return fmt.Errorf("invalid configuration YAML at line %s: check syntax, indentation, and field types", line[1])
	}
	return errors.New("invalid configuration YAML: expected a complete mapping; check syntax and field types")
}

func diagnoseNode(node *yaml.Node, typ reflect.Type, format, path string, depth int, remaining *int) error {
	// Bound aliases, including recursive aliases. The YAML decoder is still the
	// authority on alias expansion and merge semantics.
	if depth > 100 || *remaining <= 0 {
		return nil
	}
	*remaining--
	if node.Kind == yaml.AliasNode {
		return diagnoseNode(node.Alias, typ, format, path, depth+1, remaining)
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if node.Tag == "!!null" {
		return nil
	}
	location := func(message string) error { return nodeError(format, node, path, message) }
	var issues []error
	switch typ.Kind() {
	case reflect.Struct:
		// Custom scalar types such as time.Time own their decoding.
		if node.Kind != yaml.MappingNode {
			if decodeValue(node, typ, format) != nil {
				return location("expected a mapping or valid scalar for this field")
			}
			return nil
		}
		fields := configFields(typ, format)
		seen := make(map[string]bool)
		for i := 0; i < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			if format == "yaml" && seen[key.Value] {
				issues = append(issues, nodeError(format, key, path, fmt.Sprintf("duplicate field %q", key.Value)))
				continue
			}
			seen[key.Value] = true
			if key.Tag == "!!merge" {
				continue
			}
			fieldType, ok := fields[key.Value]
			if !ok && format == "json" {
				// encoding/json also accepts case-insensitive field names.
				for name, candidate := range fields {
					if strings.EqualFold(name, key.Value) {
						fieldType, ok = candidate, true
						break
					}
				}
			}
			childPath := key.Value
			if path != "" {
				childPath = path + "." + childPath
			}
			if !ok {
				allowed := strings.Join(slices.Sorted(maps.Keys(fields)), ", ")
				issues = append(issues, nodeError(format, key, childPath, "unknown field; allowed fields: "+allowed))
				continue
			}
			if err := diagnoseNode(value, fieldType, format, childPath, depth+1, remaining); err != nil {
				issues = append(issues, err)
			}
		}
	case reflect.Slice:
		if node.Kind != yaml.SequenceNode {
			return location("expected a sequence")
		}
		for i, value := range node.Content {
			if err := diagnoseNode(value, typ.Elem(), format, fmt.Sprintf("%s[%d]", path, i), depth+1, remaining); err != nil {
				issues = append(issues, err)
			}
		}
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			return location("expected a mapping")
		}
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Tag == "!!merge" {
				continue
			}
			if err := diagnoseNode(node.Content[i+1], typ.Elem(), format, fmt.Sprintf("%s[%q]", path, node.Content[i].Value), depth+1, remaining); err != nil {
				issues = append(issues, err)
			}
		}
	default:
		if err := decodeValue(node, typ, format); errors.Is(err, errNotScalar) {
			return location("expected " + typ.String())
		} else if err != nil {
			return location("expected " + typ.String() + " (check the value's type and range)")
		}
	}
	return errors.Join(issues...)
}

var errNotScalar = errors.New("expected a scalar")

// decodeValue decodes one node into typ with the decoder for format, so the
// diagnostics accept the same values as the configuration reader.
func decodeValue(node *yaml.Node, typ reflect.Type, format string) error {
	target := reflect.New(typ).Interface()
	if format == "yaml" {
		return node.Decode(target)
	}
	if node.Kind != yaml.ScalarNode {
		return errNotScalar
	}
	data := []byte(node.Value)
	if node.Tag == "!!str" {
		data, _ = json.Marshal(node.Value)
	}
	return json.Unmarshal(data, target)
}

// nodeError locates a message at node without quoting the node's value.
func nodeError(format string, node *yaml.Node, path, message string) error {
	if path == "" {
		path = "<root>"
	}
	return fmt.Errorf("configuration %s at line %d, column %d (%s): %s", strings.ToUpper(format), node.Line, node.Column, path, message)
}

// Use the schema tags, including embedded YAML inline fields and embedded JSON
// fields, so diagnostics stay in sync when new settings are added.
func configFields(typ reflect.Type, format string) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get(format)
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if (format == "yaml" && slices.Contains(strings.Split(tag, ","), "inline")) || (format == "json" && field.Anonymous && name == "") {
			for name, typ := range configFields(field.Type, format) {
				fields[name] = typ
			}
			continue
		}
		if name == "" {
			name = field.Name
			if format == "yaml" {
				name = strings.ToLower(name)
			}
		}
		fields[name] = field.Type
	}
	return fields
}
