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
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/spf13/cobra"
)

func TestRegisterFlags_CoversAllExportedConfigFields(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)

	specFields := map[string]string{}
	for _, spec := range flagSpecs {
		if prev, ok := specFields[spec.field]; ok {
			t.Fatalf(
				"duplicate flag spec for field %q: %q and %q",
				spec.field, prev, spec.name,
			)
		}
		specFields[spec.field] = spec.name
		if cmd.PersistentFlags().Lookup(spec.name) == nil {
			t.Fatalf(
				"flag %q for field %q is not registered",
				spec.name, spec.field,
			)
		}
	}

	leafFields := map[string]struct{}{}
	collectExportedLeafFields(reflect.TypeFor[Config](), "", leafFields)

	for fieldPath := range leafFields {
		if _, ok := specFields[fieldPath]; !ok {
			t.Fatalf("config field %q has no flag spec", fieldPath)
		}
	}
	for fieldPath := range specFields {
		if _, ok := leafFields[fieldPath]; !ok {
			t.Fatalf(
				"flag spec field %q does not exist on Config",
				fieldPath,
			)
		}
	}
}

func TestApplyFlags_PriorityOrderFlagsOverrideEnv(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_MEMPOOL_CAPACITY", "123456")
	t.Setenv("DINGO_DATABASE_WORKERS", "9")

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "dingo.yaml")
	if err := os.WriteFile(configFile, []byte(""), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	if cfg.MempoolCapacity != 123456 {
		t.Fatalf(
			"expected env var to set mempoolCapacity=123456, got %d",
			cfg.MempoolCapacity,
		)
	}
	if cfg.DatabaseWorkers != 9 {
		t.Fatalf(
			"expected env var to set databaseWorkers=9, got %d",
			cfg.DatabaseWorkers,
		)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{
		"--mempool-capacity=7890",
		"--data-dir=/tmp/override",
	}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("failed to apply flags: %v", err)
	}

	if cfg.MempoolCapacity != 7890 {
		t.Fatalf(
			"expected flag to override env mempoolCapacity to 7890, got %d",
			cfg.MempoolCapacity,
		)
	}
	if cfg.DatabaseWorkers != 9 {
		t.Fatalf(
			"expected unchanged databaseWorkers to stay env value 9, got %d",
			cfg.DatabaseWorkers,
		)
	}
	if cfg.DatabasePath != "/tmp/override" {
		t.Fatalf(
			"expected --data-dir to set databasePath, got %q",
			cfg.DatabasePath,
		)
	}
}

func TestApplyFlags_NetworkMagicRejectsOverflow(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	overflow := uint64(math.MaxUint32) + 1
	if err := cmd.ParseFlags([]string{
		"--network-magic=" + strconv.FormatUint(overflow, 10),
	}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	cfg := &Config{}
	err := ApplyFlags(cmd, cfg)
	if err == nil {
		t.Fatalf("expected overflow error, got nil")
	}
}

func collectExportedLeafFields(
	t reflect.Type,
	prefix string,
	out map[string]struct{},
) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		if field.Type.Kind() == reflect.Struct {
			collectExportedLeafFields(field.Type, path, out)
			continue
		}
		out[path] = struct{}{}
	}
}
