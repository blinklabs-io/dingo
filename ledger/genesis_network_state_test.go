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

package ledger

import (
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestCreateGenesisBlockInitializesMusashiNetworkState(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	require.NoError(t, ls.createGenesisBlock())

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(0), state.Slot)
	require.Equal(t, uint64(0), uint64(state.Treasury))
	require.Equal(
		t,
		uint64(14_999_999_100_000_000),
		uint64(state.Reserves),
	)
	requireTreasuryValue(t, ls, nil, 0)
}

func TestCreateGenesisBlockPersistsMusashiExtraConfigStaking(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesisPools, poolDelegators, err := nodeCfg.ShelleyGenesis().InitialPools()
	require.NoError(t, err)
	require.Len(t, genesisPools, 1)
	require.Len(t, poolDelegators, 1)

	var poolID string
	for id := range genesisPools {
		poolID = id
	}
	poolKeyHash, err := hex.DecodeString(poolID)
	require.NoError(t, err)
	delegators, ok := poolDelegators[poolID]
	require.True(t, ok)
	require.Len(t, delegators, 1)
	delegatorHash := delegators[0].StakeKeyHash()
	expectedStakeDelegations := map[string]string{
		hex.EncodeToString(delegatorHash[:]): poolID,
	}
	actualStakeDelegations, err := genesisStakeDelegations(poolDelegators)
	require.NoError(t, err)
	require.Equal(t, expectedStakeDelegations, actualStakeDelegations)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	require.NoError(t, ls.createGenesisBlock())

	pool, err := db.GetPool(lcommon.PoolKeyHash(poolKeyHash), false, nil)
	require.NoError(t, err)
	require.NotNil(t, pool)
	_, delegatorCount, err := db.Metadata().GetStakeByPool(poolKeyHash, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), delegatorCount)
}

func TestGenesisStakeDelegationsRejectsConflictingPools(t *testing.T) {
	delegator, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		make([]byte, lcommon.AddressHashSize),
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)

	_, err = genesisStakeDelegations(map[string][]lcommon.Address{
		"01": {delegator},
		"02": {delegator},
	})
	require.ErrorContains(t, err, "delegated to multiple genesis pools")
}

func TestCreateGenesisBlockBackfillsMissingNetworkState(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	require.NoError(t, db.SetGenesisCbor(
		0,
		genesisHash[:],
		[]byte{0x80},
		nil,
	))

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(0), state.Slot)
	require.Equal(t, uint64(0), uint64(state.Treasury))
	require.Equal(
		t,
		uint64(14_999_999_100_000_000),
		uint64(state.Reserves),
	)
}

func TestGenesisReserveBalanceRejectsInvalidInputs(t *testing.T) {
	_, err := genesisReserveBalance(1, []lcommon.Utxo{{}})
	require.ErrorContains(t, err, "has no output")

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	utxos, err := nodeCfg.ShelleyGenesis().GenesisUtxos()
	require.NoError(t, err)
	require.NotEmpty(t, utxos)
	_, err = genesisReserveBalance(0, utxos)
	require.ErrorContains(t, err, "exceeds max lovelace supply")
}

// TestCreateGenesisBlockSeedsEpochZeroRewardAdaPots pins the epoch-0
// reward_ada_pots row against the same slot-0 baseline as the network state.
// The delayed reward calculation reads the pots row for epoch newEpoch-1, so
// without a row for epoch 0 the 0->1 boundary has no pot inputs and its
// monetary expansion is skipped (dingo #3381). Fees are 0 because no epoch
// precedes epoch 0.
func TestCreateGenesisBlockSeedsEpochZeroRewardAdaPots(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	require.NoError(t, ls.createGenesisBlock())

	pots, err := db.Metadata().GetRewardAdaPots(0, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(0), pots.Epoch)
	require.Equal(t, uint64(0), uint64(pots.Treasury))
	require.Equal(
		t,
		uint64(14_999_999_100_000_000),
		uint64(pots.Reserves),
	)
	require.Equal(t, uint64(0), uint64(pots.Fees))
	require.Equal(t, uint64(0), pots.CapturedSlot)
}

// TestCreateGenesisBlockBackfillsMissingEpochZeroRewardAdaPots covers the
// pre-existing-genesis-database path, which reaches ensureGenesisNetworkState
// instead of the full genesis write.
func TestCreateGenesisBlockBackfillsMissingEpochZeroRewardAdaPots(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	require.NoError(t, db.SetGenesisCbor(
		0,
		genesisHash[:],
		[]byte{0x80},
		nil,
	))

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	pots, err := db.Metadata().GetRewardAdaPots(0, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(0), uint64(pots.Treasury))
	require.Equal(
		t,
		uint64(14_999_999_100_000_000),
		uint64(pots.Reserves),
	)
	require.Equal(t, uint64(0), uint64(pots.Fees))
}
