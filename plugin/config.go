// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import (
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/blinklabs-io/dingo/internal/secretfile"
	"gopkg.in/yaml.v3"
)

// Selection is the canonical configuration for one capability.
type Selection struct {
	Provider string         `yaml:"provider"`
	Config   map[string]any `yaml:"config"`
}

// EnvironmentPrefix returns the generic environment prefix for capability.
func EnvironmentPrefix(capability Capability) string {
	return "DINGO_PLUGINS_" + strings.ToUpper(
		strings.ReplaceAll(string(capability), ".", "_"),
	) + "_"
}

// ApplyEnvironment overlays generic plugin environment entries on a YAML
// selection. CLI provider selectors are intentionally applied by composition
// after this function, giving selector CLI > environment > YAML precedence.
//
// A CONFIG_<FIELD>_FILE entry sets <FIELD> to the contents of the named file
// (see secretfile.Read), so a secret such as a password need not appear in
// the environment. The contents are a plain string, never parsed as YAML.
// Setting both forms of one field is an error.
func ApplyEnvironment(
	capability Capability,
	selection *Selection,
	environ []string,
) error {
	if !capability.Valid() {
		return fmt.Errorf("unknown plugin capability %q", capability)
	}
	if selection == nil {
		return errorsNewNilSelection(capability)
	}
	prefix := EnvironmentPrefix(capability)
	setBy := make(map[string]string)
	for _, entry := range environ {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(name, prefix) {
			continue
		}
		path := strings.TrimPrefix(name, prefix)
		switch {
		case path == "PROVIDER":
			selection.Provider = value
		case strings.HasPrefix(path, "CONFIG_"):
			fieldPath := strings.TrimPrefix(path, "CONFIG_")
			if fieldPath == "" {
				return fmt.Errorf(
					"empty plugin config environment path: %s",
					name,
				)
			}
			fieldPath, fileBacked := strings.CutSuffix(fieldPath, "_FILE")
			components := strings.Split(fieldPath, "_")
			if slices.Contains(components, "") {
				// Repeated or leading/trailing underscores (e.g.
				// DATA__DIR or DATA_DIR_) would otherwise silently
				// collapse to a valid field name and override the
				// wrong setting. Fail startup on the typo instead.
				return fmt.Errorf(
					"malformed plugin config environment path %s: empty path component",
					name,
				)
			}
			field := environmentFieldName(components)
			if previous, ok := setBy[field]; ok {
				return fmt.Errorf(
					"%s and %s both set plugin config field %q; set only one",
					previous,
					name,
					field,
				)
			}
			setBy[field] = name
			var scalar any
			// An empty path is treated like an empty literal, which
			// clears the field, so presence means the same in both forms.
			if fileBacked && value != "" {
				contents, err := secretfile.Read(value)
				if err != nil {
					return fmt.Errorf("read %s: %w", name, err)
				}
				scalar = contents
			} else if err := yaml.Unmarshal(
				[]byte(value),
				&scalar,
			); err != nil {
				return fmt.Errorf("parse %s: %w", name, err)
			}
			if selection.Config == nil {
				selection.Config = make(map[string]any)
			}
			selection.Config[field] = scalar
		default:
			return fmt.Errorf("unknown plugin environment path %s", name)
		}
	}
	return nil
}

func errorsNewNilSelection(capability Capability) error {
	return fmt.Errorf("nil plugin selection for capability %s", capability)
}

func environmentFieldName(words []string) string {
	// Environment paths flatten camelCase YAML names to underscore-separated
	// words. Provider configs are currently flat, so the full suffix maps to a
	// single lowerCamel field (DATA_DIR -> dataDir). Nested provider fields can
	// still be represented by defining a map-valued field and setting it in
	// YAML; this function deliberately avoids guessing ambiguous boundaries.
	for i := range words {
		words[i] = strings.ToLower(words[i])
	}
	var field strings.Builder
	field.WriteString(words[0])
	for _, word := range words[1:] {
		runes := []rune(word)
		if len(runes) > 0 {
			runes[0] = unicode.ToUpper(runes[0])
		}
		field.WriteString(string(runes))
	}
	return field.String()
}
