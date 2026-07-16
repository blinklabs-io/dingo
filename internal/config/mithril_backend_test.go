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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultMithrilBackendAtInit captures the production default before
// any test mutates or resets globalConfig.
var defaultMithrilBackendAtInit = globalConfig.Mithril.Backend

func TestMithrilBackendDefault(t *testing.T) {
	assert.Equal(
		t,
		"v2",
		defaultMithrilBackendAtInit,
		"default Mithril backend should be v2",
	)
}

func writeMithrilBackendTestConfig(t *testing.T, yamlContent string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "test-dingo.yaml")
	require.NoError(t, os.WriteFile(tmpFile, []byte(yamlContent), 0o644))
	return tmpFile
}

func TestMithrilBackendYAML(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeMithrilBackendTestConfig(t, `
network: "preview"
mithril:
  backend: "v1"
`)
	cfg, err := LoadConfig(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, "v1", cfg.Mithril.Backend)
}

// TestMithrilBackendInvalidValueRejected verifies ValidateRuntimeConfig
// rejects an invalid Mithril backend. LoadConfig alone does not run this
// validation -- see ValidateRuntimeConfig's doc comment -- so it is called
// explicitly here, as a non-CLI caller of this package would.
func TestMithrilBackendInvalidValueRejected(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeMithrilBackendTestConfig(t, `
network: "preview"
mithril:
  backend: "v3"
`)
	cfg, err := LoadConfig(tmpFile)
	require.NoError(t, err)
	err = ValidateRuntimeConfig(cfg)
	require.Error(t, err, "invalid Mithril backend should be rejected")
}

func TestMithrilBackendEnvOverride(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("DINGO_MITHRIL_BACKEND", "v1")
	tmpFile := writeMithrilBackendTestConfig(t, `
network: "preview"
mithril:
  backend: "v2"
`)
	cfg, err := LoadConfig(tmpFile)
	require.NoError(t, err)
	assert.Equal(
		t,
		"v1",
		cfg.Mithril.Backend,
		"env var should override YAML",
	)
}
