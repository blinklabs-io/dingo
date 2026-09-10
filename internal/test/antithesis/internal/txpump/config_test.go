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
	"strconv"
	"testing"
	"time"

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

// TestLoadConfig_LoadsGenesisSystemStartUnix verifies that txpump loads the
// network magic, epoch length, slot length, and system start from genesis.
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
	require.Equal(t, time.Second, cfg.SlotLength)
	require.Equal(t, int64(1700000000), cfg.SystemStartUnix)
}

// TestConfigConfirmationDelay verifies that the output-quarantine duration is
// derived from confirmation slots and the genesis slot length, and that zero
// confirmation slots preserve the explicitly requested immediate behavior.
func TestConfigConfirmationDelay(t *testing.T) {
	cfg := Config{ConfirmationSlots: 30, SlotLength: 500 * time.Millisecond}
	require.Equal(t, 15*time.Second, cfg.confirmationDelay())

	cfg.ConfirmationSlots = 0
	require.Zero(t, cfg.confirmationDelay())
}

func TestLoadConfig_LoadsConfirmationSlots(t *testing.T) {
	clearTxpumpEnv(t)
	t.Setenv("TXPUMP_CONFIRMATION_SLOTS", "42")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, uint64(42), cfg.ConfirmationSlots)
}

func TestLoadConfig_StartUpTimeout(t *testing.T) {
	clearTxpumpEnv(t)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, 60*time.Second, cfg.StartupTimeout)

	t.Setenv("TXPUMP_STARTUP_TIMEOUT", "7")
	cfg, err = LoadConfig()
	require.NoError(t, err)
	require.Equal(t, 7*time.Second, cfg.StartupTimeout)

	t.Setenv("TXPUMP_STARTUP_TIMEOUT", "0")
	cfg, err = LoadConfig()
	require.NoError(t, err)
	require.Zero(t, cfg.StartupTimeout)
}

func TestLoadConfig_RejectsInvalidStartupTimeout(t *testing.T) {
	clearTxpumpEnv(t)
	t.Setenv("TXPUMP_STARTUP_TIMEOUT", "-1")

	_, err := LoadConfig()
	require.ErrorContains(t, err, "TXPUMP_STARTUP_TIMEOUT")
}

func TestParseStartupTimeoutBounds(t *testing.T) {
	timeout, err := parseStartupTimeout(
		strconv.FormatInt(maxStartupTimeoutSeconds, 10),
	)
	require.NoError(t, err)
	require.Equal(
		t,
		time.Duration(maxStartupTimeoutSeconds)*time.Second,
		timeout,
	)

	_, err = parseStartupTimeout(
		strconv.FormatInt(maxStartupTimeoutSeconds+1, 10),
	)
	require.Error(t, err)
	_, err = parseStartupTimeout("-1")
	require.Error(t, err)
}

func TestStopStartupTimeoutRemainsDisabledPastDeadline(t *testing.T) {
	timer := time.NewTimer(10 * time.Millisecond)
	deadline := timer.C
	stopStartupTimeout(&timer, &deadline)

	select {
	case <-deadline:
		t.Fatal("startup deadline remained active after readiness")
	case <-time.After(30 * time.Millisecond):
	}
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
		"TXPUMP_CONFIRMATION_SLOTS",
		"TXPUMP_STARTUP_TIMEOUT",
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
