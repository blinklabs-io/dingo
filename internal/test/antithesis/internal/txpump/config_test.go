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

package txpump

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const testGenesisConfigYAML = `--- # params
poolCount: 3
networkMagic: 314159
systemStartUnix: 1700000000

--- # byron
protocolConsts:
  k: 100

--- # shelley
epochLength: 1500
slotLength: 1
activeSlotsCoeff: 0.4
securityParam: 100
`

func TestLoadConfig_LoadsGenesisSystemStartUnix(t *testing.T) {
	clearTxpumpEnv(t)

	dir := t.TempDir()
	genesisPath := filepath.Join(dir, "testnet.yaml")
	require.NoError(
		t,
		os.WriteFile(genesisPath, []byte(testGenesisConfigYAML), 0o644),
	)
	t.Setenv("TXPUMP_GENESIS_FILE", genesisPath)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, uint32(314159), cfg.NetworkMagic)
	require.Equal(t, uint64(1500), cfg.EpochLength)
	require.Equal(t, int64(1700000000), cfg.SystemStartUnix)
}

func clearTxpumpEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"TXPUMP_NODE_ADDR",
		"TXPUMP_NETWORK_MAGIC",
		"TXPUMP_TX_COUNT_MIN",
		"TXPUMP_TX_COUNT_MAX",
		"TXPUMP_COOLDOWN_MIN",
		"TXPUMP_COOLDOWN_MAX",
		"TXPUMP_TYPES",
		"TXPUMP_LOG_DIR",
		"TXPUMP_FALLBACK_ADDR",
		"TXPUMP_GENESIS_UTXO_FILE",
		"TXPUMP_GENESIS_FILE",
		"TXPUMP_DELEGATION_STAKE_KEY_HASH",
		"TXPUMP_DELEGATION_POOL_KEY_HASH",
	} {
		t.Setenv(key, "")
	}
}
