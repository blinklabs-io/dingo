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
	"reflect"
	"slices"
	"strings"
	"testing"
)

// TestEffectiveConfigFieldsArePlain pins that the effective-configuration
// allowlist reads only Config fields the redaction table classifies as plain,
// and that every source path is a real Config field.
func TestEffectiveConfigFieldsArePlain(t *testing.T) {
	t.Parallel()

	classes := configLogClasses()
	leaves := configLeafFieldPaths(t)
	for _, path := range EffectiveConfigSourceFields {
		if !slices.Contains(leaves, path) {
			t.Errorf("%s is not a Config field", path)
		}
		class, ok := classes[path]
		if !ok || class != logPlain {
			t.Errorf(
				"%s feeds the effective-configuration line but is not "+
					"classified logPlain",
				path,
			)
		}
	}
}

// TestEffectiveLogAttrsPinsFieldSet pins the exact attribute keys and the
// post-default values, since a harness greps this line.
func TestEffectiveLogAttrsPinsFieldSet(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Logging:              LoggingConfig{Level: "info", Format: "json"},
		BindAddr:             "0.0.0.0",
		HealthPort:           12799,
		MetricsPort:          12798,
		MetricsBindAddr:      "0.0.0.0",
		DebugPort:            6060,
		PrivateBindAddr:      "127.0.0.1",
		ValidateHistorical:   true,
		StrictUtxoValidation: true,
		// Secrets and unrelated fields that must never appear.
		MetricsAuthToken: "SENTINEL-METRICS-TOKEN",
		KoiosParity:      KoiosParityConfig{APIKey: "SENTINEL-KOIOS"},
		BarkBaseUrl:      "https://u:SENTINEL-BARK@bark.example",
	}
	cfg.GenesisBootstrap.Enabled = true
	cfg.GenesisBootstrap.CorroborationPeers = 2

	attrs := cfg.EffectiveLogAttrs(true)
	got := make(map[string]string, len(attrs))
	keys := make([]string, 0, len(attrs))
	for _, a := range attrs {
		got[a.Key] = a.Value.String()
		keys = append(keys, a.Key)
	}
	wantKeys := []string{
		"genesis_selection_mode",
		"genesis_bootstrap_enabled",
		"genesis_window_slots_override",
		"genesis_corroboration_peers",
		"intersect_tip",
		"validate_historical",
		"strict_utxo_validation",
		"block_pipeline_enabled",
		"block_pipeline_validate_enabled",
		"storage_mode",
		"health_ready_gap_slots",
		"logging_level",
		"logging_format",
		"metrics_addr",
		"debug_addr",
		"health_addr",
		"private_ntc_tcp_enabled",
		"private_addr",
	}
	if !slices.Equal(keys, wantKeys) {
		t.Fatalf("field set changed:\n got %v\nwant %v", keys, wantKeys)
	}
	for k, want := range map[string]string{
		"genesis_selection_mode":      "true",
		"genesis_corroboration_peers": "2",
		"storage_mode":                "core",
		// 0 maps to the default, as NewHealthServer applies it.
		"health_ready_gap_slots":  "1000",
		"metrics_addr":            "0.0.0.0:12798",
		"debug_addr":              "127.0.0.1:6060",
		"health_addr":             "0.0.0.0:12799",
		"private_ntc_tcp_enabled": "false",
		"private_addr":            "",
	} {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}

	rendered := ""
	for _, a := range attrs {
		rendered += a.String() + "\n"
	}
	if strings.Contains(rendered, "SENTINEL") {
		t.Errorf("secret reached the effective configuration: %s", rendered)
	}

	cfg.HealthReadyGapSlots = 100
	cfg.PrivatePort = 3001
	for _, a := range cfg.EffectiveLogAttrs(false) {
		switch a.Key {
		case "health_ready_gap_slots":
			if a.Value.String() != "100" {
				t.Errorf("health_ready_gap_slots = %s, want 100", a.Value)
			}
		case "private_ntc_tcp_enabled":
			if a.Value.String() != "true" {
				t.Errorf("private_ntc_tcp_enabled = %s", a.Value)
			}
		case "private_addr":
			if a.Value.String() != "127.0.0.1:3001" {
				t.Errorf("private_addr = %s", a.Value)
			}
		}
	}
}

// perturbField sets the Config leaf at the dotted path to a non-zero value of
// its kind, allocating pointers on the way, and reports whether the kind is
// one it can set.
func perturbField(cfg *Config, path string) bool {
	v := reflect.ValueOf(cfg).Elem()
	for name := range strings.SplitSeq(path, ".") {
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.FieldByName(name)
	}
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString("PERTURBED")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(7)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(7)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
	default:
		return false
	}
	return true
}

func renderEffectiveAttrs(cfg *Config) string {
	var b strings.Builder
	for _, a := range cfg.EffectiveLogAttrs(false) {
		b.WriteString(a.String())
		b.WriteString("\n")
	}
	return b.String()
}

// TestEffectiveLogAttrsReadOnlyListedFields ties EffectiveLogAttrs to
// EffectiveConfigSourceFields: changing any Config field outside the list
// must leave the line unchanged, so a field read without being listed (and
// therefore without the plain-classification check) fails here.
func TestEffectiveLogAttrsReadOnlyListedFields(t *testing.T) {
	t.Parallel()

	baseline := renderEffectiveAttrs(&Config{})
	for _, path := range configLeafFieldPaths(t) {
		if slices.Contains(EffectiveConfigSourceFields, path) {
			continue
		}
		cfg := &Config{}
		if !perturbField(cfg, path) {
			continue
		}
		if got := renderEffectiveAttrs(cfg); got != baseline {
			t.Errorf(
				"EffectiveLogAttrs reads %s, which is not in "+
					"EffectiveConfigSourceFields",
				path,
			)
		}
	}
}
