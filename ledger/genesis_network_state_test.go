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
	"encoding/base64"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestCreateGenesisBlockInitializesMusashiNetworkState(t *testing.T) {
	t.Parallel()

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

// TestCreateGenesisBlockStoresExactlyOneUtxoPerGenesisOutput guards the
// offset-map and metadata-insert side of dingo#4428: for a valid,
// duplicate-free genesis UTxO set, every output must resolve to exactly one
// live UTxO row -- not zero (a missed offset) and not more than one (an
// overwritten or resurrected offset).
func TestCreateGenesisBlockStoresExactlyOneUtxoPerGenesisOutput(t *testing.T) {
	t.Parallel()

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

	byronUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	require.NoError(t, err)
	shelleyUtxos, err := nodeCfg.ShelleyGenesis().GenesisUtxos()
	require.NoError(t, err)
	genesisUtxos := append(
		append([]lcommon.Utxo{}, byronUtxos...),
		shelleyUtxos...,
	)
	require.NoError(t, rejectDuplicateGenesisUtxos(genesisUtxos))

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

	for i := range genesisUtxos {
		txId := genesisUtxos[i].Id.Id()
		idx := genesisUtxos[i].Id.Index()
		exists, err := db.UtxoExists(txId[:], idx, nil)
		require.NoError(t, err)
		require.True(
			t,
			exists,
			"genesis output %x#%d must be stored exactly once",
			txId,
			idx,
		)

		// Resolve the stored bytes through the same offset the block
		// construction recorded and confirm they are this output's bytes,
		// not another output's -- existence alone would not catch an
		// offset collapsed onto the wrong reference.
		wantCbor, err := cbor.Encode(genesisUtxos[i].Output)
		require.NoError(t, err)
		model, err := db.UtxoByRef(txId[:], idx, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			wantCbor,
			[]byte(model.Cbor),
			"genesis output %x#%d resolved to the wrong bytes",
			txId,
			idx,
		)
	}
}

// TestCreateGenesisBlockRejectsAvvmNonAvvmCollisionBeforeWriting exercises
// the full createGenesisBlock path -- not just the standalone helper -- with
// an AVVM/non-AVVM collision (the case dingo#4428 cites via gouroboros#2378)
// injected into the real musashi genesis config, and checks the failure
// happens before either the genesis network state row or the synthetic
// genesis CBOR is committed. A rejected genesis must not leave partial
// state behind for a retry to trip over.
func TestCreateGenesisBlockRejectsAvvmNonAvvmCollisionBeforeWriting(t *testing.T) {
	t.Parallel()

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

	pubkeyBytes := make([]byte, 32)
	for i := range pubkeyBytes {
		pubkeyBytes[i] = byte(i + 1)
	}
	redeemAddr, err := lcommon.NewByronAddressRedeem(
		pubkeyBytes,
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)

	byronGenesis := nodeCfg.ByronGenesis()
	if byronGenesis.AvvmDistr == nil {
		byronGenesis.AvvmDistr = map[string]string{}
	}
	if byronGenesis.NonAvvmBalances == nil {
		byronGenesis.NonAvvmBalances = map[string]string{}
	}
	byronGenesis.AvvmDistr[base64.URLEncoding.EncodeToString(pubkeyBytes)] = "1000000"
	byronGenesis.NonAvvmBalances[redeemAddr.String()] = "2000000"

	genesisHash, err := GenesisBlockHash(nodeCfg)
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
	err = ls.createGenesisBlock()
	require.ErrorContains(t, err, "duplicate Byron genesis UTxO reference")

	state, stateErr := db.Metadata().GetNetworkState(nil)
	require.NoError(t, stateErr)
	require.Nil(
		t,
		state,
		"genesis network state must not be written when validation fails",
	)
	pots, potsErr := db.Metadata().GetRewardAdaPots(0, nil)
	require.NoError(t, potsErr)
	require.Nil(
		t,
		pots,
		"epoch-0 reserves/ada-pots row must not be written when validation fails",
	)
	require.False(
		t,
		db.HasGenesisCbor(0, genesisHash[:]),
		"synthetic genesis CBOR must not be committed when validation fails",
	)
}

func TestCreateGenesisBlockPersistsMusashiExtraConfigStaking(t *testing.T) {
	t.Parallel()

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
	expectedDeposit := uint64(
		nodeCfg.ShelleyGenesis().ProtocolParameters.KeyDeposit,
	)
	view := &LedgerView{ls: ls}
	deposit, err := view.StakeCredentialDeposit(lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: delegatorHash,
	})
	require.NoError(t, err)
	require.NotNil(t, deposit)
	require.Equal(t, expectedDeposit, *deposit)

	pool, err := db.GetPool(lcommon.PoolKeyHash(poolKeyHash), false, nil)
	require.NoError(t, err)
	require.NotNil(t, pool)
	_, delegatorCount, err := db.Metadata().GetStakeByPool(poolKeyHash, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), delegatorCount)
}

func TestGenesisStakeDelegationsRejectsConflictingPools(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

// TestRejectDuplicateGenesisUtxosDetectsOverlap pins the divergence from
// dingo#4428: two genesis entries sharing a transaction ID and output index
// (here with different amounts, as a duplicate AVVM/non-AVVM or
// Byron/Shelley overlap would produce) must be rejected before any
// downstream view -- reserve summation, synthetic CBOR, or the
// offset-keyed metadata insert -- resolves the overlap differently.
func TestRejectDuplicateGenesisUtxosDetectsOverlap(t *testing.T) {
	t.Parallel()

	hash := "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	utxos := []lcommon.Utxo{
		{
			Id:     shelley.NewShelleyTransactionInput(hash, 0),
			Output: &shelley.ShelleyTransactionOutput{OutputAmount: 1_000_000},
		},
		{
			Id:     shelley.NewShelleyTransactionInput(hash, 0),
			Output: &shelley.ShelleyTransactionOutput{OutputAmount: 2_000_000},
		},
	}
	err := rejectDuplicateGenesisUtxos(utxos)
	require.ErrorContains(t, err, "duplicate genesis UTxO reference")

	_, err = genesisReserveBalance(100_000_000, utxos)
	require.NoError(
		t,
		err,
		"genesisReserveBalance alone does not see the duplicate; "+
			"callers must run rejectDuplicateGenesisUtxos first",
	)

	// A duplicate reference is rejected even when both entries carry the
	// same value -- the check is on the reference, not on whether the
	// values disagree.
	equalValueUtxos := []lcommon.Utxo{
		{
			Id:     shelley.NewShelleyTransactionInput(hash, 0),
			Output: &shelley.ShelleyTransactionOutput{OutputAmount: 1_000_000},
		},
		{
			Id:     shelley.NewShelleyTransactionInput(hash, 0),
			Output: &shelley.ShelleyTransactionOutput{OutputAmount: 1_000_000},
		},
	}
	require.ErrorContains(
		t,
		rejectDuplicateGenesisUtxos(equalValueUtxos),
		"duplicate genesis UTxO reference",
	)
}

// TestRejectDuplicateGenesisUtxosDetectsAvvmNonAvvmCollision pins dingo#4428's
// cited upstream case (gouroboros#2378): an AVVM redeem address and a
// non-AVVM address that resolve to the same underlying address bytes
// produce the same transaction ID through Byron's real
// ByronGenesis.GenesisUtxos() path, not a hand-built duplicate.
func TestRejectDuplicateGenesisUtxosDetectsAvvmNonAvvmCollision(t *testing.T) {
	t.Parallel()

	pubkeyBytes := make([]byte, 32)
	for i := range pubkeyBytes {
		pubkeyBytes[i] = byte(i)
	}
	redeemAddr, err := lcommon.NewByronAddressRedeem(
		pubkeyBytes,
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)

	byronGenesis := byron.ByronGenesis{
		AvvmDistr: map[string]string{
			base64.URLEncoding.EncodeToString(pubkeyBytes): "1000000",
		},
		NonAvvmBalances: map[string]string{
			// Same underlying address bytes as the AVVM redeem entry
			// above, expressed as a plain non-AVVM balance -- the exact
			// overlap gouroboros#2378 rejects at source construction.
			redeemAddr.String(): "2000000",
		},
	}
	_, err = byronGenesis.GenesisUtxos()
	require.ErrorContains(t, err, "duplicate Byron genesis UTxO reference")
}

// TestRejectDuplicateGenesisUtxosAllowsDistinctRefs guards against an
// overly broad check rejecting UTxOs that merely share a transaction hash
// (distinct output indexes) or an output index (distinct transaction
// hashes).
func TestRejectDuplicateGenesisUtxosAllowsDistinctRefs(t *testing.T) {
	t.Parallel()

	hashA := "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	hashB := "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
	utxos := []lcommon.Utxo{
		{Id: shelley.NewShelleyTransactionInput(hashA, 0)},
		{Id: shelley.NewShelleyTransactionInput(hashA, 1)},
		{Id: shelley.NewShelleyTransactionInput(hashB, 0)},
	}
	require.NoError(t, rejectDuplicateGenesisUtxos(utxos))
}

func TestGenesisReserveBalanceRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
