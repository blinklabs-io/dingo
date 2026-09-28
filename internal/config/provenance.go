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

package config

import (
	"fmt"
	"maps"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Source identifies where a gated configuration field's value came from.
type Source int

const (
	// SourceDefault means the field still holds its built-in default; no
	// operator-supplied value (CLI flag, environment variable, or YAML
	// key) was ever observed for it.
	SourceDefault Source = iota
	// SourceYAML means the field was set by a key in the loaded YAML
	// config file. A config file is an operator statement, so YAML counts
	// as explicit, the same as a flag or environment variable.
	SourceYAML
	// SourceEnv means the field was set by an environment variable.
	SourceEnv
	// SourceFlag means the field was set by an explicit CLI flag.
	SourceFlag
)

// Provenance records, for each gated Config field (keyed by dotted field
// path, e.g. "HistoryExpiry.Enabled"), whether its value came from an
// operator or is still the built-in default.
type Provenance map[string]Source

// IsExplicit reports whether field was set by an operator (CLI flag,
// environment variable, or YAML file) rather than left at its built-in
// default. A field that was never recorded is absent from the map, which
// looks up as SourceDefault (the zero value) and is therefore not
// explicit.
func (p Provenance) IsExplicit(field string) bool {
	return p[field] != SourceDefault
}

// gatedFieldPaths is the fixed set of Config fields provenance tracks. It
// must match the extractors that key persisted gate state by these same
// dotted paths.
var gatedFieldPaths = []string{
	"Network",
	"NetworkMagic",
	"StartEra",
	"StorageMode",
	"HistoryExpiry.Enabled",
	"PledgeLeverageEnabled",
	"PledgeLeverage",
	"FullPotRewardsEnabled",
	"DelegatorInactivityEnabled",
	"DelegatorInactivity",
	"MinPoolMargin",
	"ValidateHistorical",
	"StrictUtxoValidation",
}

// GatedFieldPaths returns the dotted Config field paths that gate
// provenance tracks. The returned slice is a defensive copy.
func GatedFieldPaths() []string {
	out := make([]string, len(gatedFieldPaths))
	copy(out, gatedFieldPaths)
	return out
}

// FieldExists reports whether path resolves to a field on cfg, so that a
// future rename of a gated field cannot silently disable its gate. It
// wraps the unexported fieldByPath walk flag registration already uses
// (flags.go), rather than duplicating it.
func FieldExists(cfg *Config, path string) bool {
	v := reflect.ValueOf(cfg).Elem()
	for p := range strings.SplitSeq(path, ".") {
		if !v.IsValid() {
			return false
		}
		v = v.FieldByName(p)
	}
	return v.IsValid()
}

// Provenance returns a copy of the provenance recorded for c's gated
// fields so far. A gated field never recorded is simply absent from the
// result (equivalent to SourceDefault via IsExplicit).
func (c *Config) Provenance() Provenance {
	out := make(Provenance, len(c.provenance))
	maps.Copy(out, c.provenance)
	return out
}

// recordProvenance sets the provenance for a gated field, initializing the
// backing map on first use.
func (c *Config) recordProvenance(field string, source Source) {
	if c.provenance == nil {
		c.provenance = make(Provenance, len(gatedFieldPaths))
	}
	c.provenance[field] = source
}

// gatedFieldPathSet indexes gatedFieldPaths for the per-flag lookup ApplyFlags
// does over every registered flag, not just the gated ones.
var gatedFieldPathSet = func() map[string]struct{} {
	set := make(map[string]struct{}, len(gatedFieldPaths))
	for _, path := range gatedFieldPaths {
		set[path] = struct{}{}
	}
	return set
}()

// isGatedField reports whether path is one of the field paths provenance is
// documented to track. Callers that iterate something broader than
// gatedFieldPaths -- ApplyFlags walks every flagSpec -- must consult this
// before recording, or the map fills with fields nothing ever reads and stops
// matching its own contract. recordProvenance itself stays unguarded so
// SetProvenanceForTest can set any path a test needs.
func isGatedField(path string) bool {
	_, ok := gatedFieldPathSet[path]
	return ok
}

// SetProvenanceForTest directly sets a gated field's recorded provenance,
// bypassing CLI/env/YAML detection entirely.
//
// Test-only: production code must only ever populate provenance through
// ApplyFlags' and RecordSourceProvenance's own detection, and must only
// ever observe it through Provenance/IsExplicit.
func (c *Config) SetProvenanceForTest(field string, source Source) {
	c.recordProvenance(field, source)
}

// RecordSourceProvenance records, for gated fields only, whether c's
// current values came from a YAML config file or an environment variable
// (see Provenance/IsExplicit). ApplyFlags layers SourceFlag for any
// CLI-set field on top afterward, and unconditionally overwrites any
// entry RecordSourceProvenance made for the same field, so callers must
// invoke this before ApplyFlags for CLI > env > YAML precedence to hold
// (see cmd/dingo/main.go, which calls it between LoadConfig and
// ApplyFlags).
//
// This is deliberately NOT called by LoadConfig itself:
// TestLoad_CompareFullStruct (config_test.go) DeepEquals the whole
// *Config LoadConfig returns against a hand-built struct literal, and a
// literal cannot populate an unexported field, so LoadConfig populating
// provenance would break that test. LoadConfig leaving provenance nil is
// pinned by TestLoadConfig_LeavesProvenanceEmpty
// (provenance_internal_test.go).
//
// configFile is resolved exactly as LoadConfig resolves it — via the
// shared resolveConfigFile (config.go) — so this inspects the same file
// LoadConfig actually read. A configFile that resolves to "" (none
// explicitly given and neither the user nor system default path exists)
// or that has since been removed is not an error: dingo runs fine with no
// config file, in which case only the environment layer is recorded.
func (c *Config) RecordSourceProvenance(configFile string) error {
	resolved := resolveConfigFile(configFile)
	if resolved != "" {
		buf, err := os.ReadFile(resolved)
		switch {
		case err == nil:
			recordYAMLProvenance(c, buf)
		case os.IsNotExist(err):
			// No config file present: nothing to record from YAML.
		default:
			return fmt.Errorf("error reading config file: %w", err)
		}
	}
	recordEnvProvenance(c)
	return nil
}

// recordYAMLProvenance marks each gated field present as a key in the
// parsed YAML document as SourceYAML. It handles both YAML shapes a config
// file may use — settings at the top level, or nested under a top-level
// `config:` key — exactly like collectMidnightYAMLFields/mappingValue
// already do for the Midnight settings, reusing mappingValue rather than
// writing a third yaml.Node walker. When both shapes somehow resolve a
// path, the config:-wrapped value wins, matching
// collectMidnightYAMLFields's own override behavior.
func recordYAMLProvenance(cfg *Config, buf []byte) {
	var doc yaml.Node
	if err := yaml.Unmarshal(buf, &doc); err != nil || len(doc.Content) == 0 {
		return
	}
	root := doc.Content[0]
	configNode := mappingValue(root, "config")
	for _, path := range gatedFieldPaths {
		segments := yamlSegmentsForField(path)
		if segments == nil {
			continue
		}
		node := yamlNodeForPath(root, segments)
		if configNode != nil {
			if wrapped := yamlNodeForPath(configNode, segments); wrapped != nil {
				node = wrapped
			}
		}
		if node != nil {
			cfg.recordProvenance(path, SourceYAML)
		}
	}
}

// yamlNodeForPath walks a chain of mapping keys from node, returning nil as
// soon as any segment is absent.
func yamlNodeForPath(node *yaml.Node, segments []string) *yaml.Node {
	for _, seg := range segments {
		node = mappingValue(node, seg)
		if node == nil {
			return nil
		}
	}
	return node
}

// yamlSegmentsForField returns the YAML key path for a dotted Config field
// path (e.g. "HistoryExpiry.Enabled" -> []string{"historyExpiry",
// "enabled"}), derived from each segment's own `yaml` struct tag. It
// returns nil if any segment does not resolve on Config, so a rename
// cannot silently mis-detect provenance instead of failing loudly (caught
// by TestGatedFieldPathsResolveOnConfig).
func yamlSegmentsForField(path string) []string {
	t := reflect.TypeFor[Config]()
	segments := make([]string, 0, strings.Count(path, ".")+1)
	for name := range strings.SplitSeq(path, ".") {
		field, ok := t.FieldByName(name)
		if !ok {
			return nil
		}
		key, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if key == "" {
			key = name
		}
		segments = append(segments, key)
		t = field.Type
	}
	return segments
}

// recordEnvProvenance marks each gated field whose resolved environment
// variable is present in the environment as SourceEnv. It runs after
// recordYAMLProvenance, so an environment variable overwrites a YAML
// provenance entry for the same field, matching the documented CLI > env >
// YAML > defaults priority.
func recordEnvProvenance(cfg *Config) {
	for _, path := range gatedFieldPaths {
		for _, name := range envVarCandidatesForField(path) {
			if _, ok := os.LookupEnv(name); ok {
				cfg.recordProvenance(path, SourceEnv)
				break
			}
		}
	}
}

// envWordRegexp and envAcronymRegexp are copied verbatim from
// envconfig.gatherInfo (github.com/kelseyhightower/envconfig@v1.4.0) so
// split_words name derivation here matches envconfig's own algorithm
// exactly, rather than an independent guess that could silently diverge.
var (
	envWordRegexp    = regexp.MustCompile("([^A-Z]+|[A-Z]+[^A-Z]+|[A-Z]+)")
	envAcronymRegexp = regexp.MustCompile("([A-Z]+)([A-Z][^A-Z]+)")
)

// envVarCandidatesForField returns the environment variable name(s) that
// actually set a gated field, derived from envconfigKeyAndAlt (verified
// empirically per field, and per returned candidate, in
// TestGatedFieldEnvProvenance; see the Task 5 report for the full table).
// The primary Key is always returned; the bare Alt fallback is included
// too when the leaf field carries an explicit envconfig tag (envconfig has
// no Alt for a plain or split_words field).
func envVarCandidatesForField(path string) []string {
	key, alt, ok := envconfigKeyAndAlt(path)
	if !ok {
		return nil
	}
	if alt != "" {
		return []string{key, alt}
	}
	return []string{key}
}

// envconfigKeyAndAlt derives the same "Key" (checked first) and "Alt"
// (checked as a fallback, present only for an explicit envconfig tag) that
// envconfig.Process("cardano", cfg) itself derives for a dotted Config
// field path. It walks the path exactly as envconfig's gatherInfo
// recursion does: each non-leaf segment's own derived key becomes the
// accumulated prefix for the next segment, rather than the outer
// "cardano" prefix being applied directly to the leaf.
//
// This matters for any gated field nested under a container with no
// envconfig tag of its own — e.g. HistoryExpiry.Enabled: the container
// field's derived key ("CARDANO_HISTORYEXPIRY") becomes the leaf's real
// prefix, so the leaf's working Key is
// "CARDANO_HISTORYEXPIRY_DINGO_HISTORY_EXPIRY_ENABLED", not
// "CARDANO_DINGO_HISTORY_EXPIRY_ENABLED" (which envconfig never checks at
// all — a prior version of this function returned exactly that wrong
// name; see TestHistoryExpiryEnabledEnv_FlatFormDoesNotApply). Deriving
// the prefix generically by walking the path, rather than special-casing
// this one field by name, means a future nested gate is covered for free.
func envconfigKeyAndAlt(path string) (key string, alt string, ok bool) {
	t := reflect.TypeFor[Config]()
	prefix := "cardano"
	segments := strings.Split(path, ".")
	for i, name := range segments {
		field, found := t.FieldByName(name)
		if !found {
			return "", "", false
		}
		localKey := field.Name
		if isTrueTag(field.Tag.Get("split_words")) {
			localKey = splitWordsJoin(field.Name)
		}
		leafAlt := ""
		if tag := field.Tag.Get("envconfig"); tag != "" {
			leafAlt = strings.ToUpper(tag)
			localKey = leafAlt
		}
		combined := strings.ToUpper(prefix + "_" + localKey)
		if i == len(segments)-1 {
			return combined, leafAlt, true
		}
		prefix = combined
		t = field.Type
	}
	return "", "", false
}

// isTrueTag matches envconfig's own isTrue helper for reading boolean
// struct tag values such as split_words.
func isTrueTag(s string) bool {
	b, _ := strconv.ParseBool(s)
	return b
}

// splitWordsJoin reproduces envconfig's split_words word-splitting
// algorithm (gatherInfo in envconfig.go) verbatim, joining the derived
// words with "_" in their original case. envconfigKeyAndAlt applies a
// single final strings.ToUpper to the whole combined key, exactly
// matching envconfig's own single final ToUpper of Key, so the case here
// is irrelevant — only the word boundaries (where the underscores land)
// matter.
func splitWordsJoin(fieldName string) string {
	words := envWordRegexp.FindAllStringSubmatch(fieldName, -1)
	name := make([]string, 0, len(words))
	for _, w := range words {
		if m := envAcronymRegexp.FindStringSubmatch(w[0]); len(m) == 3 {
			name = append(name, m[1], m[2])
		} else {
			name = append(name, w[0])
		}
	}
	return strings.Join(name, "_")
}
