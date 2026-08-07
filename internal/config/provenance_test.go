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

package config_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

func TestProvenanceDefaultIsNotExplicit(t *testing.T) {
	prov := config.Provenance{}
	require.False(t, prov.IsExplicit("Network"))
}

func TestProvenanceFlagIsExplicit(t *testing.T) {
	prov := config.Provenance{"Network": config.SourceFlag}
	require.True(t, prov.IsExplicit("Network"))
}

func TestProvenanceYAMLIsExplicit(t *testing.T) {
	// A config file is an operator statement, so a YAML key conflicting
	// with a persisted gate is an error rather than an override.
	prov := config.Provenance{"Network": config.SourceYAML}
	require.True(t, prov.IsExplicit("Network"))
}

func TestProvenanceEnvIsExplicit(t *testing.T) {
	prov := config.Provenance{"StorageMode": config.SourceEnv}
	require.True(t, prov.IsExplicit("StorageMode"))
}

func TestGatedFieldPathsResolveOnConfig(t *testing.T) {
	// Every gated path must actually exist on Config, so a rename cannot
	// silently disable a gate.
	cfg := config.GetConfig()
	for _, path := range config.GatedFieldPaths() {
		require.True(
			t, config.FieldExists(cfg, path),
			"gated field path %q does not resolve on Config", path,
		)
	}
}
