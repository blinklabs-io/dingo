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
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"
)

// Selection is the canonical configuration for one capability.
type Selection struct {
	Provider string         `yaml:"provider"`
	Config   map[string]any `yaml:"config"`
}

// EnvironmentPrefix returns the generic environment prefix for capability.
func EnvironmentPrefix(capability Capability) string {
	return "DINGO_PLUGINS_" + strings.ToUpper(strings.ReplaceAll(string(capability), ".", "_")) + "_"
}

// ApplyEnvironment overlays generic plugin environment entries on a YAML
// selection. CLI provider selectors are intentionally applied by composition
// after this function, giving selector CLI > environment > YAML precedence.
func ApplyEnvironment(capability Capability, selection *Selection, environ []string) error {
	if !capability.Valid() {
		return fmt.Errorf("unknown plugin capability %q", capability)
	}
	if selection == nil {
		return errorsNewNilSelection(capability)
	}
	prefix := EnvironmentPrefix(capability)
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
				return fmt.Errorf("empty plugin config environment path: %s", name)
			}
			if selection.Config == nil {
				selection.Config = make(map[string]any)
			}
			components := strings.Split(fieldPath, "_")
			for _, component := range components {
				if component == "" {
					// Repeated or leading/trailing underscores (e.g.
					// DATA__DIR or DATA_DIR_) would otherwise silently
					// collapse to a valid field name and override the
					// wrong setting. Fail startup on the typo instead.
					return fmt.Errorf(
						"malformed plugin config environment path %s: empty path component",
						name,
					)
				}
			}
			var scalar any
			if err := yaml.Unmarshal([]byte(value), &scalar); err != nil {
				return fmt.Errorf("parse %s: %w", name, err)
			}
			setEnvironmentPath(selection.Config, components, scalar)
		default:
			return fmt.Errorf("unknown plugin environment path %s", name)
		}
	}
	return nil
}

func errorsNewNilSelection(capability Capability) error {
	return fmt.Errorf("nil plugin selection for capability %s", capability)
}

func setEnvironmentPath(dst map[string]any, words []string, value any) {
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
	dst[field.String()] = value
}
