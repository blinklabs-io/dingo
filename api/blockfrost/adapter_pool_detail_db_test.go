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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/hex"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newDBBackedAdapterWithProtocolParams builds a NodeAdapter over a real
// LedgerState that has actually completed Start(), using the embedded
// devnet genesis (all hard forks at epoch 0, nOpt = 100 per
// config/cardano/devnet/shelley-genesis.json). Unlike newDBBackedAdapter
// (adapter_block_db_test.go), this loads real protocol parameters, which
// PoolDetail now requires in order to compute live_saturation -- a
// required, non-nullable schema field that cannot be faked with a zero
// placeholder. Start() completes synchronously against the empty, freshly
// created database (no chain synced yet), so this stays a fast unit test.
func newDBBackedAdapterWithProtocolParams(
	t *testing.T,
) (*NodeAdapter, *sqliteplugin.MetadataStoreSqlite, *database.Database) {
	t.Helper()
	cfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS, "devnet/config.json",
	)
	require.NoError(t, err)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: cfg,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DatabaseWorkerPoolConfig: ledger.DatabaseWorkerPoolConfig{
			Disabled: true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ls.Start(t.Context()))

	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)

	return adapter, store, db
}

// TestNodeAdapterPoolDetailFullResponse seeds a pool with a real
// transaction-backed registration, an owner delegating its own stake back
// to the pool, a separate delegator, a captured active-stake snapshot, and
// observed block production across two epochs, then verifies every
// PoolDetailInfo field the adapter computes from real DB state, including
// live_saturation computed from the real devnet nOpt (100).
func TestNodeAdapterPoolDetailFullResponse(t *testing.T) {
	adapter, store, db := newDBBackedAdapterWithProtocolParams(t)

	poolKeyHash := fill32(0x11)[:28]
	vrfKeyHash := fill32(0x22)
	rewardAccountHash := fill32(0x33)[:28]
	ownerKeyHash := fill32(0x44)[:28]
	delegatorStakingKey := fill32(0x55)[:28]

	pool := &models.Pool{
		PoolKeyHash:   poolKeyHash,
		VrfKeyHash:    vrfKeyHash,
		RewardAccount: rewardAccountHash,
		Pledge:        types.Uint64(5_000_000_000),
		Cost:          types.Uint64(340_000_000),
		Margin:        &types.Rat{Rat: big.NewRat(5, 100)},
		Registration: []models.PoolRegistration{
			{
				PoolKeyHash:   poolKeyHash,
				VrfKeyHash:    vrfKeyHash,
				RewardAccount: rewardAccountHash,
				Pledge:        types.Uint64(5_000_000_000),
				Cost:          types.Uint64(340_000_000),
				Margin:        &types.Rat{Rat: big.NewRat(5, 100)},
				AddedSlot:     1,
				CertificateID: 90001,
			},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)
	// PoolRegistrationOwner.PoolID is a denormalized convenience column,
	// not the association GORM tracks (that's PoolRegistrationID), so it
	// is not auto-populated by a nested create and must be set explicitly
	// once the parent IDs exist.
	require.NoError(t, store.DB().Create(&models.PoolRegistrationOwner{
		KeyHash:            ownerKeyHash,
		PoolRegistrationID: pool.Registration[0].ID,
		PoolID:             pool.ID,
	}).Error)

	// Registration certificate history: a real transaction backing the
	// registration row's certificate_id = 90001. Both IDs use a large,
	// distinctive value because Start() against the devnet genesis writes
	// its own low-numbered genesis-staking transaction/cert rows, which
	// low hardcoded IDs like 1 would collide with.
	regTxHash := fill32(0x66)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: 90001, Hash: regTxHash, Slot: 1, BlockIndex: 0,
	}).Error)
	require.NoError(t, store.DB().Exec(
		"INSERT INTO certs (id, transaction_id, cert_index, cert_type, slot) VALUES (90001, 90001, 0, 0, 1)",
	).Error)

	// A plain delegator (not an owner) with a live UTxO delegated to the
	// pool.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: delegatorStakingKey,
		Pool:       poolKeyHash,
		Active:     true,
		AddedSlot:  1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       fill32(0x77),
		OutputIdx:  0,
		StakingKey: delegatorStakingKey,
		Amount:     types.Uint64(6_900_000_000),
		AddedSlot:  1,
	}).Error)

	// The pool's own owner, also delegating its stake to the pool: this is
	// what live_pledge reports.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: ownerKeyHash,
		Pool:       poolKeyHash,
		Active:     true,
		AddedSlot:  1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       fill32(0x88),
		OutputIdx:  0,
		StakingKey: ownerKeyHash,
		Amount:     types.Uint64(5_000_000_001),
		AddedSlot:  1,
	}).Error)

	// Two blocks produced: one before the current epoch's start slot, one
	// within it, so blocks_minted (lifetime) and blocks_epoch (current
	// epoch only) diverge.
	pkh := lcommon.PoolKeyHash(poolKeyHash)
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 1, 10, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 2, 500, nil))
	// ls.CurrentEpoch() stays 0 on a freshly started ledger with no synced
	// chain (Start() only loads persisted state; it does not advance the
	// epoch by wall-clock time), so the adapter resolves "current epoch"
	// to epoch_id 0 regardless of slot. Start() against the devnet genesis
	// already wrote the epoch_id = 0 row; update its start_slot to 100 so
	// the block at slot 10 is excluded from blocks_epoch while still
	// counting toward the lifetime total.
	require.NoError(t, store.DB().Model(&models.Epoch{}).
		Where("epoch_id = ?", 0).
		Update("start_slot", 100).Error)

	// A captured Mark active-stake snapshot for epoch 0 (activeStakeEpoch
	// is currentEpoch-2, clamped to 0 below epoch 2): this pool is the only
	// one in the snapshot, so active_size should come out to exactly 1.0.
	const activeStake = uint64(4_200_000_000)
	require.NoError(t, store.DB().Create(&models.PoolStakeSnapshot{
		Epoch:        0,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(activeStake),
	}).Error)

	poolID := hex.EncodeToString(poolKeyHash)
	info, err := adapter.PoolDetail(poolID)
	require.NoError(t, err)

	const wantLiveStake = uint64(11_900_000_001) // 6_900_000_000 + 5_000_000_001
	const wantNOpt = 100                         // config/cardano/devnet/shelley-genesis.json

	assert.Equal(t, poolID, info.Hex)
	assert.Equal(t,
		lcommon.PoolId(lcommon.NewBlake2b224(poolKeyHash)).String(),
		info.PoolID,
	)
	assert.Equal(t, hex.EncodeToString(vrfKeyHash), info.VrfKey)
	assert.Equal(t, uint64(2), info.BlocksMinted)
	assert.Equal(t, uint64(1), info.BlocksEpoch)
	assert.Equal(t, "11900000001", info.LiveStake)
	assert.Equal(t, uint64(2), info.LiveDelegators)
	assert.Equal(t, "5000000000", info.DeclaredPledge)
	assert.Equal(t, "5000000001", info.LivePledge)
	assert.InDelta(t, 0.05, info.MarginCost, 0.0001)
	assert.Equal(t, "340000000", info.FixedCost)
	require.Len(t, info.Owners, 1)
	require.Len(t, info.Registration, 1)
	assert.Equal(t, hex.EncodeToString(regTxHash), info.Registration[0])
	assert.Empty(t, info.Retirement)
	assert.NotNil(t, info.Retirement)
	assert.Nil(t, info.CalidusKey)
	// Active stake and active_size: this pool is the network's only
	// captured snapshot row, so it holds the entire active-stake total.
	assert.Equal(t, "4200000000", info.ActiveStake)
	assert.InDelta(t, 1.0, info.ActiveSize, 1e-9)
	// live_saturation is computed from the real devnet nOpt (100) and real
	// total circulating supply, not a placeholder: saturation_threshold =
	// totalCirculation / nOpt, and live_saturation = liveStake /
	// saturation_threshold. totalCirculation (MaxLovelaceSupply minus
	// Reserves) is deliberately NOT totalActiveStake -- see
	// totalCirculation's doc comment in adapter_pool_detail.go for why the
	// reward calculation requires that distinction. Both figures are
	// derivable from the same devnet-genesis fixture this test already
	// starts a real LedgerState against: MaxLovelaceSupply is fixed by
	// config/cardano/devnet/shelley-genesis.json, and Reserves is whatever
	// ls.Start() computed and persisted as the genesis network state
	// (MaxLovelaceSupply minus the sum of all genesis UTxOs).
	const wantMaxLovelaceSupply = uint64(2_000_000_000_000) // config/cardano/devnet/shelley-genesis.json
	var networkState models.NetworkState
	require.NoError(t, store.DB().Order("slot DESC").First(&networkState).Error)
	wantCirculation := wantMaxLovelaceSupply - uint64(networkState.Reserves)
	wantSaturation := float64(wantLiveStake) / (float64(wantCirculation) / float64(wantNOpt))
	assert.InDelta(t, wantSaturation, info.LiveSaturation, 1e-6)
}

// TestNodeAdapterPoolDetailProtocolParamsUnavailable covers the case where
// protocol parameters have not been loaded yet (e.g. very early in a
// node's life, before Start() has run): live_saturation is a required,
// non-nullable float in the OpenAPI schema, and 0.0 is itself a legitimate
// saturation value, so there is no schema-compatible placeholder for
// "unknown". PoolDetail must fail the whole request rather than guess.
func TestNodeAdapterPoolDetailProtocolParamsUnavailable(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	poolKeyHash := fill32(0xe1)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(0xe2),
		Registration: []models.PoolRegistration{
			{PoolKeyHash: poolKeyHash, AddedSlot: 1},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)
	// A captured active-stake snapshot so this test exercises the
	// protocol-params failure specifically, not the (also required)
	// missing-active-stake-snapshot failure checked just before it.
	require.NoError(t, store.DB().Create(&models.PoolStakeSnapshot{
		Epoch:        0,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(1_000_000),
	}).Error)

	_, err := adapter.PoolDetail(hex.EncodeToString(poolKeyHash))
	require.ErrorContains(t, err, "protocol parameters")
}

// TestNodeAdapterPoolDetailBech32AndHexSameResult is the ID-format
// acceptance criterion: bech32 and hex forms of the same pool key hash must
// resolve to an identical PoolDetailInfo.
func TestNodeAdapterPoolDetailBech32AndHexSameResult(t *testing.T) {
	adapter, store, _ := newDBBackedAdapterWithProtocolParams(t)

	poolKeyHash := fill32(0x99)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(0xaa),
		Registration: []models.PoolRegistration{
			{PoolKeyHash: poolKeyHash, AddedSlot: 1},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)
	// A captured active-stake snapshot: PoolDetail now errors rather than
	// silently reporting active_size == 0.0 when none exists for the
	// current epoch (see the active_size doc comment in
	// adapter_pool_detail.go), and this test wants both ID-format calls to
	// succeed.
	require.NoError(t, store.DB().Create(&models.PoolStakeSnapshot{
		Epoch:        0,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(1_000_000),
	}).Error)

	hexID := hex.EncodeToString(poolKeyHash)
	bech32ID := lcommon.PoolId(lcommon.NewBlake2b224(poolKeyHash)).String()
	require.NotEqual(t, hexID, bech32ID)

	byHex, err := adapter.PoolDetail(hexID)
	require.NoError(t, err)
	byBech32, err := adapter.PoolDetail(bech32ID)
	require.NoError(t, err)

	assert.Equal(t, byHex, byBech32)
	assert.Equal(t, bech32ID, byHex.PoolID)
	assert.Equal(t, hexID, byHex.Hex)
}

// TestNodeAdapterPoolDetailActiveStakeSnapshotUnavailable covers the case
// where activeStakeEpoch has no captured Mark snapshot for any pool
// (GetTotalActiveStake returns 0): active_size is a required, non-nullable
// float in the OpenAPI schema, and 0.0 is itself a legitimate active_size
// value (a pool with no active stake), so there is no schema-compatible
// placeholder for "unknown". PoolDetail must fail the whole request rather
// than silently report every pool as 0% active-saturated. This is
// reachable through the normal path, not a pathological one:
// activeStakeEpoch floors to 0 for currentEpoch < 2, and any node missing
// that epoch's snapshot hits it too.
func TestNodeAdapterPoolDetailActiveStakeSnapshotUnavailable(t *testing.T) {
	adapter, store, _ := newDBBackedAdapterWithProtocolParams(t)

	poolKeyHash := fill32(0xf1)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(0xf2),
		Registration: []models.PoolRegistration{
			{PoolKeyHash: poolKeyHash, AddedSlot: 1},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)
	// Deliberately no PoolStakeSnapshot row for epoch 0.

	_, err := adapter.PoolDetail(hex.EncodeToString(poolKeyHash))
	require.ErrorContains(t, err, "active stake")
}

// TestNodeAdapterPoolDetailEpochRowMissing covers the case where GetEpoch
// returns nil for the current epoch: falling back to epochStartSlot = 0
// would make the blocks_epoch query byte-identical to the blocks_minted
// query above it (both 0..noSlotUpperBound) and silently report the
// pool's entire lifetime block count as its current-epoch count. PoolDetail
// must fail instead, since the state needed to answer blocks_epoch isn't
// there.
func TestNodeAdapterPoolDetailEpochRowMissing(t *testing.T) {
	adapter, store, _ := newDBBackedAdapterWithProtocolParams(t)

	poolKeyHash := fill32(0xf3)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(0xf4),
		Registration: []models.PoolRegistration{
			{PoolKeyHash: poolKeyHash, AddedSlot: 1},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)
	require.NoError(t, store.DB().Create(&models.PoolStakeSnapshot{
		Epoch:        0,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(1_000_000),
	}).Error)
	// Start() against the devnet genesis writes the epoch_id = 0 row;
	// delete it to simulate a node missing the current epoch's row.
	require.NoError(
		t,
		store.DB().Where("epoch_id = ?", 0).Delete(&models.Epoch{}).Error,
	)

	_, err := adapter.PoolDetail(hex.EncodeToString(poolKeyHash))
	// Match the epoch-row branch specifically: earlier failure paths in
	// PoolDetail also mention "epoch" ("get pool stake snapshot for epoch
	// %d", "get total active stake for epoch %d"), so a bare "epoch"
	// substring would pass without ever reaching the branch under test.
	require.ErrorContains(t, err, "no epoch row found")
}

// TestNodeAdapterPoolDetailInvalidID covers a malformed pool ID (neither
// valid bech32 nor 56-character hex).
func TestNodeAdapterPoolDetailInvalidID(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	_, err := adapter.PoolDetail("not-a-pool-id")
	require.ErrorIs(t, err, ErrInvalidPoolID)
}

// TestNodeAdapterPoolDetailNotFound covers a well-formed pool ID with no
// matching pool row.
func TestNodeAdapterPoolDetailNotFound(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	missing := hex.EncodeToString(fill32(0xfe)[:28])
	_, err := adapter.PoolDetail(missing)
	require.ErrorIs(t, err, models.ErrPoolNotFound)
}

// TestNodeAdapterPoolDetailDatabaseFailure guards against a backing-store
// failure being silently swallowed into an incomplete success response: a
// broken stake query must surface as an error.
func TestNodeAdapterPoolDetailDatabaseFailure(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	poolKeyHash := fill32(0xcc)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(0xdd),
		Registration: []models.PoolRegistration{
			{PoolKeyHash: poolKeyHash, AddedSlot: 1},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)

	// Break the store so GetStakeByPools fails; the failure must surface
	// instead of a partial/zeroed response.
	require.NoError(t, store.DB().Exec("DROP TABLE account").Error)

	_, err := adapter.PoolDetail(hex.EncodeToString(poolKeyHash))
	require.ErrorContains(t, err, "get live stake")
}
