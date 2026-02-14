// Copyright 2024 Blink Labs Software
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

package cardano

import (
	"bytes"
	"os"
	"path"
	"reflect"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDataDir = "testdata"
)

var expectedCardanoNodeConfig = &CardanoNodeConfig{
	path:               testDataDir,
	AlonzoGenesisFile:  "alonzo-genesis.json",
	AlonzoGenesisHash:  "7e94a15f55d1e82d10f09203fa1d40f8eede58fd8066542cf6566008068ed874",
	ByronGenesisFile:   "byron-genesis.json",
	ByronGenesisHash:   "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761",
	ConwayGenesisFile:  "conway-genesis.json",
	ConwayGenesisHash:  "9cc5084f02e27210eacba47af0872e3dba8946ad9460b6072d793e1d2f3987ef",
	ShelleyGenesisFile: "shelley-genesis.json",
	ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
}

func TestCardanoNodeConfig(t *testing.T) {
	tmpPath := path.Join(
		testDataDir,
		"config.json",
	)
	cfg, err := NewCardanoNodeConfigFromFile(tmpPath)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Create temp config without parsed genesis files to make comparison easier
	tmpCfg := *cfg
	tmpCfg.byronGenesis = nil
	tmpCfg.shelleyGenesis = nil
	tmpCfg.alonzoGenesis = nil
	tmpCfg.conwayGenesis = nil
	if !reflect.DeepEqual(&tmpCfg, expectedCardanoNodeConfig) {
		t.Fatalf(
			"did not get expected object\n     got: %#v\n  wanted: %#v\n",
			tmpCfg,
			expectedCardanoNodeConfig,
		)
	}
	t.Run("Byron genesis", func(t *testing.T) {
		g := cfg.ByronGenesis()
		if g == nil {
			t.Fatalf("got nil instead of ByronGenesis")
		}
	})
	t.Run("Shelley genesis", func(t *testing.T) {
		g := cfg.ShelleyGenesis()
		if g == nil {
			t.Fatalf("got nil instead of ShelleyGenesis")
		}
	})
	t.Run("Alonzo genesis", func(t *testing.T) {
		g := cfg.AlonzoGenesis()
		if g == nil {
			t.Fatalf("got nil instead of AlonzoGenesis")
		}
	})
	t.Run("Conway genesis", func(t *testing.T) {
		g := cfg.ConwayGenesis()
		if g == nil {
			t.Fatalf("got nil instead of ConwayGenesis")
		}
	})
}

func TestCardanoNodeConfigMissingGenesisHashes(t *testing.T) {
	cfgBytes := []byte(`{
  "AlonzoGenesisFile": "alonzo-genesis.json",
  "ByronGenesisFile": "byron-genesis.json",
  "ConwayGenesisFile": "conway-genesis.json",
  "ShelleyGenesisFile": "shelley-genesis.json"
}`)
	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader(cfgBytes))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	cfg.path = testDataDir
	if err := cfg.loadGenesisConfigs(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if cfg.ByronGenesisHash != expectedCardanoNodeConfig.ByronGenesisHash {
		t.Fatalf(
			"unexpected Byron genesis hash: got %s, wanted %s",
			cfg.ByronGenesisHash,
			expectedCardanoNodeConfig.ByronGenesisHash,
		)
	}
	if cfg.ShelleyGenesisHash != expectedCardanoNodeConfig.ShelleyGenesisHash {
		t.Fatalf(
			"unexpected Shelley genesis hash: got %s, wanted %s",
			cfg.ShelleyGenesisHash,
			expectedCardanoNodeConfig.ShelleyGenesisHash,
		)
	}
	if cfg.AlonzoGenesisHash != expectedCardanoNodeConfig.AlonzoGenesisHash {
		t.Fatalf(
			"unexpected Alonzo genesis hash: got %s, wanted %s",
			cfg.AlonzoGenesisHash,
			expectedCardanoNodeConfig.AlonzoGenesisHash,
		)
	}
	if cfg.ConwayGenesisHash != expectedCardanoNodeConfig.ConwayGenesisHash {
		t.Fatalf(
			"unexpected Conway genesis hash: got %s, wanted %s",
			cfg.ConwayGenesisHash,
			expectedCardanoNodeConfig.ConwayGenesisHash,
		)
	}
}

func TestValidateGenesisHashCorrect(t *testing.T) {
	// Correct hash should pass validation and return the hash
	data := []byte("test genesis data")
	expectedHash := lcommon.Blake2b256Hash(data).String()

	hash, err := validateGenesisHash("Test", expectedHash, data)
	require.NoError(t, err)
	assert.Equal(t, expectedHash, hash)
}

func TestValidateGenesisHashWrong(t *testing.T) {
	// Wrong hash should return an error
	data := []byte("test genesis data")
	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"

	_, err := validateGenesisHash("Test", wrongHash, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Test genesis hash mismatch")
	assert.Contains(t, err.Error(), wrongHash)
}

func TestValidateGenesisHashEmpty(t *testing.T) {
	// Empty expected hash should skip validation and return computed hash
	data := []byte("test genesis data")
	expectedHash := lcommon.Blake2b256Hash(data).String()

	hash, err := validateGenesisHash("Test", "", data)
	require.NoError(t, err)
	assert.Equal(t, expectedHash, hash)
}

func TestGenesisHashMismatchFromFile(t *testing.T) {
	// Config with wrong Byron genesis hash should fail to load
	cfgBytes := []byte(`{
  "ByronGenesisFile": "byron-genesis.json",
  "ByronGenesisHash": "0000000000000000000000000000000000000000000000000000000000000000"
}`)
	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader(cfgBytes))
	require.NoError(t, err)
	cfg.path = testDataDir
	err = cfg.loadGenesisConfigs()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Byron genesis hash mismatch")
}

func TestGenesisHashMismatchShelley(t *testing.T) {
	// Config with wrong Shelley genesis hash should fail to load
	cfgBytes := []byte(`{
  "ShelleyGenesisFile": "shelley-genesis.json",
  "ShelleyGenesisHash": "0000000000000000000000000000000000000000000000000000000000000000"
}`)
	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader(cfgBytes))
	require.NoError(t, err)
	cfg.path = testDataDir
	err = cfg.loadGenesisConfigs()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Shelley genesis hash mismatch")
}

func TestGenesisHashMismatchAlonzo(t *testing.T) {
	// Config with wrong Alonzo genesis hash should fail to load
	cfgBytes := []byte(`{
  "AlonzoGenesisFile": "alonzo-genesis.json",
  "AlonzoGenesisHash": "0000000000000000000000000000000000000000000000000000000000000000"
}`)
	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader(cfgBytes))
	require.NoError(t, err)
	cfg.path = testDataDir
	err = cfg.loadGenesisConfigs()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Alonzo genesis hash mismatch")
}

func TestGenesisHashMismatchConway(t *testing.T) {
	// Config with wrong Conway genesis hash should fail to load
	cfgBytes := []byte(`{
  "ConwayGenesisFile": "conway-genesis.json",
  "ConwayGenesisHash": "0000000000000000000000000000000000000000000000000000000000000000"
}`)
	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader(cfgBytes))
	require.NoError(t, err)
	cfg.path = testDataDir
	err = cfg.loadGenesisConfigs()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Conway genesis hash mismatch")
}

func TestCanonicalizeByronGenesisJSON(t *testing.T) {
	// Test 1: Verify deterministic canonicalization
	// Different whitespace/formatting should produce identical canonical bytes
	originalJSON := `{
  "blockVersionData": {
    "unlockStakeEpoch": "18446744073709551615"
  },
  "startTime": 1564431616
}`

	compactJSON := `{"blockVersionData":{"unlockStakeEpoch":"18446744073709551615"},"startTime":1564431616}`

	canonical1, err := canonicalizeByronGenesisJSON([]byte(originalJSON))
	if err != nil {
		t.Fatalf("Failed to canonicalize original JSON: %v", err)
	}

	canonical2, err := canonicalizeByronGenesisJSON([]byte(compactJSON))
	if err != nil {
		t.Fatalf("Failed to canonicalize compact JSON: %v", err)
	}

	if !bytes.Equal(canonical1, canonical2) {
		t.Errorf(
			"Canonicalization is not deterministic:\n  canonical1: %s\n  canonical2: %s",
			string(canonical1),
			string(canonical2),
		)
	}

	// Test 2: Verify large numbers stored as strings are preserved
	if !bytes.Contains(canonical1, []byte(`"18446744073709551615"`)) {
		t.Errorf(
			"Canonicalization corrupted large string-encoded number: %s",
			string(canonical1),
		)
	}

	// Test 3: Verify hash stability with actual Byron genesis
	// Read actual Byron genesis file
	byronGenesisPath := "testdata/byron-genesis.json"
	originalBytes, err := os.ReadFile(byronGenesisPath)
	if err != nil {
		t.Skipf("Skipping actual file test: %v", err)
	}

	canonicalBytes, err := canonicalizeByronGenesisJSON(originalBytes)
	if err != nil {
		t.Fatalf("Failed to canonicalize Byron genesis: %v", err)
	}

	// Compute hash
	hash := lcommon.Blake2b256Hash(canonicalBytes).String()

	// Re-canonicalize to ensure idempotence
	canonical2nd, err := canonicalizeByronGenesisJSON(canonicalBytes)
	if err != nil {
		t.Fatalf("Failed second canonicalization: %v", err)
	}

	hash2nd := lcommon.Blake2b256Hash(canonical2nd).String()

	if hash != hash2nd {
		t.Errorf("Canonicalization is not idempotent: %s != %s", hash, hash2nd)
	}
}
