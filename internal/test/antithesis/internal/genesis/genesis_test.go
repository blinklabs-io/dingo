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

package genesis

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testYAML = `# Test configuration
--- # Required Testnet Parameters
poolCount: 5
networkMagic: 42

--- # Byron Genesis Override
protocolConsts:
  k: 40

--- # Shelley Genesis Override
epochLength: 500
slotLength: 1
activeSlotsCoeff: 0.4
securityParam: 40

--- # Alonzo Genesis Override

--- # Conway Genesis Override

--- # Node Config Override
TestShelleyHardForkAtEpoch: 0
`

func TestParse(t *testing.T) {
	cfg, err := Parse([]byte(testYAML))
	require.NoError(t, err)
	assert.Equal(t, 5, cfg.PoolCount)
	assert.Equal(t, uint32(42), cfg.NetworkMagic)
	assert.Equal(t, uint64(500), cfg.EpochLength)
	assert.Equal(t, float64(1), cfg.SlotLength)
	assert.InDelta(t, 0.4, cfg.ActiveSlotsCoeff, 0.001)
	assert.Equal(t, uint64(40), cfg.SecurityParam)
}

func TestParseFallbackByronK(t *testing.T) {
	yaml := `# header
--- # params
poolCount: 3
networkMagic: 99

--- # byron
protocolConsts:
  k: 100

--- # shelley
epochLength: 1500
slotLength: 1
activeSlotsCoeff: 0.4
`
	cfg, err := Parse([]byte(yaml))
	require.NoError(t, err)
	assert.Equal(t, uint64(100), cfg.SecurityParam, "should fall back to Byron k")
}

func TestParseInsufficientDocs(t *testing.T) {
	_, err := Parse([]byte("only one doc"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected at least")
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testnet.yaml")
	require.NoError(t, os.WriteFile(path, []byte(testYAML), 0644))

	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), cfg.EpochLength)
}

func TestLoadActualTestnetYAML(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	testnetPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "testnet.yaml")
	// Parse the actual antithesis testnet.yaml to catch drift.
	cfg, err := Load(testnetPath)
	require.NoError(t, err)
	assert.Equal(t, 5, cfg.PoolCount)
	assert.Equal(t, uint32(42), cfg.NetworkMagic)
	assert.Equal(t, uint64(500), cfg.EpochLength)
	assert.Equal(t, uint64(40), cfg.SecurityParam)
}
