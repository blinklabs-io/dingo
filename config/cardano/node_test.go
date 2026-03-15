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
	"path/filepath"
	"reflect"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDataDir = "testdata"
)

func ptrBool(v bool) *bool       { return &v }
func ptrUint64(v uint64) *uint64 { return &v }

var expectedCardanoNodeConfig = &CardanoNodeConfig{
	path:              testDataDir,
	AlonzoGenesisFile: "alonzo-genesis.json",
	AlonzoGenesisHash: "7e94a15f55d1e82d10f09203fa1d40f8eede58fd8066542cf6566008068ed874",
	ByronGenesisFile:  "byron-genesis.json",
	ByronGenesisHash:  "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761",
	ConwayGenesisFile: "conway-genesis.json",
	ConwayGenesisHash: "9cc5084f02e27210eacba47af0872e3dba8946ad9460b6072d793e1d2f3987ef",
	MithrilGenesisVerificationKey:              "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d",
	MithrilGenesisVerificationKeyFile:           "genesis.vkey",
	MithrilGenesisAncillaryVerificationKey:      "5b3138392c3139322c3231362c3135302c3131342c3231362c3233372c3231302c34352c31382c32312c3139362c3230382c3234362c3134362c322c3235322c3234332c3235312c3139372c32382c3135372c3230342c3134352c33302c31342c3232382c3136382c3132392c38332c3133362c33365d",
	MithrilGenesisAncillaryVerificationKeyFile:  "ancillary.vkey",
	ShelleyGenesisFile:                          "shelley-genesis.json",
	ShelleyGenesisHash:                          "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	ExperimentalHardForksEnabled:                ptrBool(false),
	TestShelleyHardForkAtEpoch:                  ptrUint64(0),
	TestAllegraHardForkAtEpoch:                  ptrUint64(0),
	TestMaryHardForkAtEpoch:                     ptrUint64(0),
	TestAlonzoHardForkAtEpoch:                   ptrUint64(0),
}

func TestCardanoNodeConfig(t *testing.T) {
	tmpPath := filepath.Join(
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

func TestCardanoNodeConfigLoadsMithrilVerificationKeysFromDisk(t *testing.T) {
	tmpDir := t.TempDir()
	genesisKey := "genesis-vkey-data\n"
	ancillaryKey := "ancillary-vkey-data\n"
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(tmpDir, "genesis.vkey"),
			[]byte(genesisKey),
			0o600,
		),
	)
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(tmpDir, "ancillary.vkey"),
			[]byte(ancillaryKey),
			0o600,
		),
	)

	cfgBytes := []byte(`{
  "MithrilGenesisVerificationKeyFile": "genesis.vkey",
  "MithrilGenesisAncillaryVerificationKeyFile": "ancillary.vkey"
}`)
	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader(cfgBytes))
	require.NoError(t, err)
	cfg.path = tmpDir
	require.NoError(t, cfg.loadGenesisConfigs())
	require.Equal(t, genesisKey, cfg.MithrilGenesisVerificationKey)
	require.Equal(
		t,
		ancillaryKey,
		cfg.MithrilGenesisAncillaryVerificationKey,
	)
}

func TestCardanoNodeConfigAutoDiscoversMithrilVerificationKeys(t *testing.T) {
	tmpDir := t.TempDir()
	genesisKey := "genesis-vkey-data\n"
	ancillaryKey := "ancillary-vkey-data\n"
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(tmpDir, "genesis.vkey"),
			[]byte(genesisKey),
			0o600,
		),
	)
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(tmpDir, "ancillary.vkey"),
			[]byte(ancillaryKey),
			0o600,
		),
	)

	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	cfg.path = tmpDir
	require.NoError(t, cfg.loadGenesisConfigs())
	require.Equal(
		t,
		defaultMithrilGenesisVerificationKeyFile,
		cfg.MithrilGenesisVerificationKeyFile,
	)
	require.Equal(
		t,
		defaultMithrilGenesisAncillaryVerificationKeyFile,
		cfg.MithrilGenesisAncillaryVerificationKeyFile,
	)
	require.Equal(t, genesisKey, cfg.MithrilGenesisVerificationKey)
	require.Equal(
		t,
		ancillaryKey,
		cfg.MithrilGenesisAncillaryVerificationKey,
	)
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

func TestHardForkEpoch(t *testing.T) {
	t.Run("enabled with all eras at epoch 0", func(t *testing.T) {
		cfg := &CardanoNodeConfig{
			ExperimentalHardForksEnabled: ptrBool(true),
			TestShelleyHardForkAtEpoch:   ptrUint64(0),
			TestAllegraHardForkAtEpoch:   ptrUint64(0),
			TestMaryHardForkAtEpoch:      ptrUint64(0),
			TestAlonzoHardForkAtEpoch:    ptrUint64(0),
			TestBabbageHardForkAtEpoch:   ptrUint64(0),
			TestConwayHardForkAtEpoch:    ptrUint64(0),
		}
		for _, era := range []string{
			"shelley", "allegra", "mary",
			"alonzo", "babbage", "conway",
		} {
			epoch, ok := cfg.HardForkEpoch(era)
			assert.True(t, ok, "era %s should be configured", era)
			assert.Equal(
				t, uint64(0), epoch,
				"era %s should fork at epoch 0", era,
			)
		}
	})

	t.Run("disabled ignores all epochs", func(t *testing.T) {
		cfg := &CardanoNodeConfig{
			ExperimentalHardForksEnabled: ptrBool(false),
			TestShelleyHardForkAtEpoch:   ptrUint64(0),
			TestConwayHardForkAtEpoch:    ptrUint64(0),
		}
		_, ok := cfg.HardForkEpoch("shelley")
		assert.False(t, ok,
			"should not report configured when disabled")
	})

	t.Run("nil flag ignores all epochs", func(t *testing.T) {
		cfg := &CardanoNodeConfig{
			TestShelleyHardForkAtEpoch: ptrUint64(0),
		}
		_, ok := cfg.HardForkEpoch("shelley")
		assert.False(t, ok,
			"should not report configured when flag is nil")
	})

	t.Run("partial configuration", func(t *testing.T) {
		cfg := &CardanoNodeConfig{
			ExperimentalHardForksEnabled: ptrBool(true),
			TestShelleyHardForkAtEpoch:   ptrUint64(0),
			// Conway not set
		}
		_, shelleyOk := cfg.HardForkEpoch("shelley")
		assert.True(t, shelleyOk, "shelley should be configured")

		_, conwayOk := cfg.HardForkEpoch("conway")
		assert.False(t, conwayOk, "conway should not be configured")
	})

	t.Run("unknown era", func(t *testing.T) {
		cfg := &CardanoNodeConfig{
			ExperimentalHardForksEnabled: ptrBool(true),
		}
		_, ok := cfg.HardForkEpoch("unknown")
		assert.False(t, ok, "unknown era should not be configured")
	})

	t.Run("non-zero epoch", func(t *testing.T) {
		cfg := &CardanoNodeConfig{
			ExperimentalHardForksEnabled: ptrBool(true),
			TestConwayHardForkAtEpoch:    ptrUint64(5),
		}
		epoch, ok := cfg.HardForkEpoch("conway")
		assert.True(t, ok)
		assert.Equal(t, uint64(5), epoch)
	})
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
