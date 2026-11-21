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
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// RewardBlocksScenario drives block counts and fees through the ledger's
// LedgerDB over the boundary harness, whose epochs are two slots long: epoch
// 5 holds slots 10 and 11, the rollback target is slot 10, and the harness's
// decentralization parameter is 1/2, so the first slot of every epoch is an
// overlay slot.
type RewardBlocksScenario struct {
	DB       *database.Database
	PoolA    []byte
	PoolB    []byte
	PoolIdle []byte
	// Block commits a block minted by pool at slot.
	Block func(slot uint64, pool []byte)
	// Fee commits a valid transaction with the fee at slot.
	Fee func(slot, fee uint64)
	// InvalidTotal commits a phase-2-invalid transaction that declares its
	// total collateral.
	InvalidTotal func(slot, total uint64)
	// InvalidInputs commits a phase-2-invalid transaction whose collateral
	// is the live output id, less a collateral return of ret.
	InvalidInputs func(slot uint64, id byte, ret uint64)
	// Collateral creates an output of amount under id in the metadata store
	// and the set.
	Collateral func(id byte, amount uint64)
	// FeeAroundTheSet and BlockAroundTheSet write to the metadata store
	// without staging the change for the set.
	FeeAroundTheSet   func(slot, fee uint64)
	BlockAroundTheSet func(slot uint64, pool []byte)
	// Counts and Fees are the reward round's reads for an epoch.
	Counts func(epoch uint64) (map[string]uint64, uint64, bool, error)
	Fees   func(epoch uint64) (uint64, error)
	// EpochCounts is the boundary capture's count of the blocks the pools
	// minted in epoch, overlay slots included.
	EpochCounts func(epoch uint64) (map[string]uint64, uint64, bool, error)
	// Inputs and StoreInputs are the reward snapshot inputs of epoch as the
	// ledger's LedgerDB and the metadata store itself answer.
	Inputs      func(epoch uint64) RewardInputsAnswer
	StoreInputs func(epoch uint64) RewardInputsAnswer
	// InputsAroundTheSet changes epoch's pots, pool inputs and stake inputs
	// in the metadata store without staging the change for the set.
	InputsAroundTheSet func(epoch uint64)
	// AddSetStake adds a live output of amount, held by the stake
	// credential key, to the set alone, which the harness created in the
	// metadata store only.
	AddSetStake func(key []byte, amount uint64)
	// PruneBefore runs the epoch-boundary prune of reward state below epoch.
	PruneBefore func(epoch uint64)
	// Outputs and StoreOutputs are the derived reward rows of the snapshot
	// epoch and the credited rounds as the ledger's LedgerDB and the
	// metadata store itself answer.
	Outputs      func(epoch uint64) RewardOutputsAnswer
	StoreOutputs func(epoch uint64) RewardOutputsAnswer
	// OutputsAroundTheSet changes epoch's pool outputs, and its account
	// outputs when accounts is set, in the metadata store without staging
	// the change for the set.
	OutputsAroundTheSet func(epoch uint64, accounts bool)
	// PrecomputeRound runs the background precompute of the round the
	// boundary into epoch 6 applies, to completion.
	PrecomputeRound func() error
	// DeregisterMember deregisters the harness's delegator credential in the
	// metadata store and the set at slot 11, with no certificate row.
	DeregisterMember func()
	// DeregisterMemberInSet deregisters the harness's delegator credential in
	// the set alone, at slot 11 with the deregistration event the set keeps,
	// for a ledger whose metadata store holds no accounts.
	DeregisterMemberInSet func()
	// Balance is a credential's account reward and its pending credits as
	// the ledger's LedgerDB answers.
	Balance func(key []byte) (reward, pending uint64)
	// Tip records the metadata store's tip at slot, which the boundary reads
	// to decide that a block followed the stability window.
	Tip func(slot uint64)
	// ConfigureLeiosValidator sets a certificate validator, as every node
	// does.
	ConfigureLeiosValidator func()
	// StageNoncesInSet gives the set nonces the harness wrote only to the
	// metadata store.
	StageNoncesInSet func(rows []models.BlockNonce)
	// PotsFees is the fee total the boundary stored for epoch.
	PotsFees func(epoch uint64) uint64
	// Boundary applies the rollover into epoch 6.
	Boundary func()
	// RollbackBeforeBoundary rolls the ledger back to slot 10.
	RollbackBeforeBoundary func()
	// HideStoreReads is BoundaryEpochStateScenario.HideStoreReads.
	HideStoreReads func(names ...string) (func() int64, func(bool))
	// DrainFamilies publishes the drained mask to the ledger.
	DrainFamilies func(mask nodesettings.FamilyMask)
	// FailSet makes the ledger treat its set as failed, as an update the
	// set refused does.
	FailSet func()
	// InstallDrainedSources is BoundaryEpochStateScenario.InstallDrainedSources.
	InstallDrainedSources func()
	LedgerDB              func() ledgerdb.LedgerDB
}

// RewardInputsAnswer is what a read of an epoch's reward snapshot inputs
// returned, with the store row IDs left out.
type RewardInputsAnswer struct {
	Pots     *models.RewardAdaPots
	Snapshot *models.RewardSnapshot
	Failure  string
	Pools    []*models.RewardPoolInput
	Stakes   []*models.RewardStakeInput
	Err      error
}

func normalizeInputs(a RewardInputsAnswer) RewardInputsAnswer {
	if a.Pots != nil {
		c := *a.Pots
		c.ID = 0
		a.Pots = &c
	}
	if a.Snapshot != nil {
		c := *a.Snapshot
		c.ID = 0
		a.Snapshot = &c
	}
	for i, p := range a.Pools {
		c := *p
		c.ID = 0
		a.Pools[i] = &c
	}
	for i, st := range a.Stakes {
		c := *st
		c.ID = 0
		a.Stakes[i] = &c
	}
	if len(a.Pools) == 0 {
		a.Pools = nil
	}
	if len(a.Stakes) == 0 {
		a.Stakes = nil
	}
	return a
}

// RewardBlocksOptions selects what the scenario seeds before the set loads.
type RewardBlocksOptions struct {
	// Imported records a Mithril anchor at slot 8, the first slot of epoch
	// 4, with imported block counts for epochs 3 and 4 and imported fees for
	// epoch 4.
	Imported bool
	// Round seeds the reward round the boundary into epoch 6 applies: pool
	// B's inputs in snapshot epoch 3, with the stake of the harness's owner
	// and delegator credentials.
	Round bool
}

// RewardOutputsAnswer is what a read of an epoch's derived reward rows and
// the credited rounds returned, with the store row IDs left out.
type RewardOutputsAnswer struct {
	Pools    []*models.RewardPoolOutput
	Accounts []*models.RewardAccountOutput
	Cursor   string
	Rounds   []models.RewardCreditRound
	Err      error
}

func normalizeOutputs(a RewardOutputsAnswer) RewardOutputsAnswer {
	for i, p := range a.Pools {
		c := *p
		c.ID = 0
		a.Pools[i] = &c
	}
	for i, o := range a.Accounts {
		c := *o
		c.ID = 0
		a.Accounts[i] = &c
	}
	if len(a.Pools) == 0 {
		a.Pools = nil
	}
	if len(a.Accounts) == 0 {
		a.Accounts = nil
	}
	if len(a.Rounds) == 0 {
		a.Rounds = nil
	}
	return a
}

// NewRewardBlocksScenarioForTest builds the scenario over a metadata store
// whose statements pass through guard.
func NewRewardBlocksScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
	guard metadata.StatementGuard,
	opts RewardBlocksOptions,
) RewardBlocksScenario {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	poolA := bytes.Repeat([]byte{0xa1}, 28)
	poolB := bytes.Repeat([]byte{0xb2}, 28)
	poolIdle := bytes.Repeat([]byte{0xc3}, 28)
	h := newBoundaryReplayHarnessSeeded(t, db, false, func() {
		require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 9_000, 0, nil))
		require.NoError(
			t,
			db.Metadata().IterateAccounts(nil, func(a *models.Account) error {
				c := *a
				return db.Metadata().ImportAccount(&c, nil)
			}),
		)
		if opts.Imported {
			require.NoError(t, db.SetSyncState("mithril_ledger_slot", "8", nil))
			meta := db.Metadata()
			require.NoError(t, meta.SaveImportedPoolBlockCounts(
				[]models.ImportedPoolBlockCount{
					{
						PoolKeyHash:    poolB,
						Epoch:          3,
						BlocksProduced: 2,
						CapturedSlot:   8,
					},
					{
						PoolKeyHash:    poolIdle,
						Epoch:          3,
						BlocksProduced: 1,
						CapturedSlot:   8,
					},
					{
						PoolKeyHash:    poolA,
						Epoch:          4,
						BlocksProduced: 3,
						CapturedSlot:   8,
					},
				}, nil))
			require.NoError(t, meta.SaveImportedEpochBlockTotal(3, 3, 8, nil))
			require.NoError(t, meta.SaveImportedEpochBlockTotal(4, 3, 8, nil))
			fees := types.Uint64(1_000)
			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch: 4, ImportedEpochFees: &fees, CapturedSlot: 8,
			}, nil))
		}
		if opts.Round {
			seedRewardRound(t, db, poolB)
		}
		if set != nil {
			AdoptLiveSetForTest(t, db, set)
		}
	})
	var generation uint64
	if tip := h.ls.loadTipSnapshot(); tip != nil {
		generation = tip.generation
	}
	h.ls.consensus.Store(&consensusSnapshot{
		generation:     generation,
		currentEpoch:   h.ls.currentEpoch,
		currentEra:     h.ls.currentEra,
		currentPParams: h.ls.currentPParams,
		epochCache:     h.ls.epochCache,
	})
	if opts.Round {
		window := uint64(0)
		h.ls.rewardUpdateWindowOverride = &window
	}
	write := func(fn func(ledgerdb.LedgerDB, ledgerdb.Txn)) {
		ldb := h.ls.LedgerDB()
		txn := ldb.Begin(t.Context(), true)
		defer txn.Release()
		fn(ldb, txn)
		require.NoError(t, txn.Commit())
	}
	var seq uint64
	var tag byte = 0x80
	txOf := func(body map[uint]any, valid bool) (lcommon.Transaction, [32]byte) {
		raw, err := cbor.Encode([]any{body, map[uint]any{}, valid, nil})
		require.NoError(t, err)
		tx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(t, err)
		var hash [32]byte
		copy(hash[:], tx.Hash().Bytes())
		return tx, hash
	}
	submitVia := func(
		via func() ledgerdb.LedgerDB,
		slot uint64,
		body map[uint]any,
		valid bool,
		stageSet bool,
	) {
		tx, hash := txOf(body, valid)
		offsets := &database.BlockIngestionResult{
			TxOffsets:   map[[32]byte]database.CborOffset{hash: {}},
			UtxoOffsets: map[database.UtxoRef]database.CborOffset{},
		}
		for _, utxo := range tx.Produced() {
			offsets.UtxoOffsets[database.UtxoRef{
				TxId: hash, OutputIdx: utxo.Id.Index(),
			}] = database.CborOffset{BlockSlot: slot, ByteLength: 1}
		}
		ldb := via()
		txn := ldb.Begin(t.Context(), true)
		defer txn.Release()
		func() {
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32)),
				0, 0, nil, nil,
				offsets,
				txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
			))
		}()
		require.NoError(t, txn.Commit())
		if !stageSet || h.ls.config.UtxoSet == nil {
			return
		}
		// The ledger's block path stages the transaction's UTxO change for
		// the set; the harness commits transactions directly, so it does the
		// same, or the set keeps the collateral an invalid transaction
		// consumed and never sees its return.
		point := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		evt, err := utxoEventFor(point, 0, tx)
		require.NoError(t, err)
		require.NoError(t, h.ls.config.UtxoSet.ApplyUtxoChanges(
			&point, []UtxoEvent{evt},
		))
	}
	submit := func(slot uint64, body map[uint]any, valid bool) {
		submitVia(h.ls.LedgerDB, slot, body, valid, true)
	}
	addr := func() []byte {
		a, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
			bytes.Repeat([]byte{0xaa}, 28), bytes.Repeat([]byte{0xab}, 28),
		)
		require.NoError(t, err)
		raw, err := a.Bytes()
		require.NoError(t, err)
		return raw
	}
	input := func() []any {
		tag++
		return []any{bytes.Repeat([]byte{tag}, 32), uint64(0)}
	}
	return RewardBlocksScenario{
		DB: db, PoolA: poolA, PoolB: poolB, PoolIdle: poolIdle,
		LedgerDB: h.ls.LedgerDB,
		Block: func(slot uint64, pool []byte) {
			seq++
			write(func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
				require.NoError(t, ldb.UpdatePoolOpCertSequence(
					t.Context(), lcommon.PoolKeyHash(pool), seq, slot, txn,
				))
			})
		},
		Fee: func(slot, fee uint64) {
			submit(slot, map[uint]any{
				0: []any{input()}, 1: []any{}, 2: fee,
			}, true)
		},
		InvalidTotal: func(slot, total uint64) {
			submit(slot, map[uint]any{
				0: []any{input()}, 1: []any{}, 2: uint64(7),
				13: []any{input()}, 17: total,
			}, false)
		},
		InvalidInputs: func(slot uint64, id byte, ret uint64) {
			submit(slot, map[uint]any{
				0: []any{input()}, 1: []any{}, 2: uint64(7),
				13: []any{[]any{bytes.Repeat([]byte{id}, 32), uint64(0)}},
				16: map[uint]any{0: addr(), 1: ret},
			}, false)
		},
		FeeAroundTheSet: func(slot, fee uint64) {
			submitVia(func() ledgerdb.LedgerDB { return dbadapter.New(db) },
				slot, map[uint]any{0: []any{input()}, 1: []any{}, 2: fee}, true,
				false)
		},
		BlockAroundTheSet: func(slot uint64, pool []byte) {
			seq++
			ldb := dbadapter.New(db)
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.UpdatePoolOpCertSequence(
				t.Context(), lcommon.PoolKeyHash(pool), seq, slot, txn,
			))
			require.NoError(t, txn.Commit())
		},
		Collateral: func(id byte, amount uint64) {
			require.NoError(t, db.CreateUtxo(t.Context(), nil, &models.Utxo{
				TxId: bytes.Repeat([]byte{id}, 32), OutputIdx: 0,
				Amount: types.Uint64(amount), AddedSlot: 5,
				Address:    addr(),
				StakingKey: bytes.Repeat([]byte{0xab}, 28),
			}))
			out, err := cbor.Encode(map[uint]any{0: addr(), 1: amount})
			require.NoError(t, err)
			point := ocommon.NewPoint(5, bytes.Repeat([]byte{0x05}, 32))
			require.NoError(t, h.ls.config.UtxoSet.ApplyUtxoChanges(
				&point, []UtxoEvent{{
					Point: point,
					Added: []UtxoOutput{{
						Ref: models.UtxoId{
							Hash: bytes.Repeat([]byte{id}, 32),
						},
						Cbor: out,
					}},
				}},
			))
		},
		Counts: func(epoch uint64) (map[string]uint64, uint64, bool, error) {
			inputs := []*models.RewardPoolInput{
				{PoolKeyHash: poolA}, {PoolKeyHash: poolB},
				{PoolKeyHash: poolIdle},
			}
			return h.ls.rewardBlockCounts(
				h.ls.LedgerDB(), nil, epoch, inputs,
			)
		},
		Fees: func(epoch uint64) (uint64, error) {
			row, err := db.GetEpoch(epoch, nil)
			require.NoError(t, err)
			require.NotNil(t, row)
			return h.ls.rewardEpochFeesFor(nil, *row)
		},
		EpochCounts: func(epoch uint64) (map[string]uint64, uint64, bool, error) {
			start := epoch * uint64(replayBoundaryLength)
			return h.ls.LedgerDB().CountEpochPoolBlocks(
				epoch,
				[]lcommon.PoolKeyHash{
					lcommon.PoolKeyHash(poolA), lcommon.PoolKeyHash(poolB),
					lcommon.PoolKeyHash(poolIdle),
				},
				start, start+uint64(replayBoundaryLength)-1, nil,
			)
		},
		Inputs: func(epoch uint64) RewardInputsAnswer {
			var a RewardInputsAnswer
			ldb := h.ls.LedgerDB()
			if a.Pots, a.Err = ldb.GetRewardAdaPots(epoch, nil); a.Err != nil {
				return a
			}
			a.Snapshot, a.Err = ldb.GetRewardSnapshot(epoch, "mark", nil)
			if a.Err != nil {
				return a
			}
			a.Failure, a.Err = ldb.GetRewardSeedFailure(epoch, "mark", nil)
			if a.Err != nil {
				return a
			}
			if a.Pools, a.Err = ldb.GetRewardPoolInputs(epoch, nil); a.Err != nil {
				return a
			}
			a.Stakes, a.Err = ldb.GetRewardStakeInputs(epoch, nil)
			return normalizeInputs(a)
		},
		StoreInputs: func(epoch uint64) RewardInputsAnswer {
			var a RewardInputsAnswer
			meta := db.Metadata()
			if a.Pots, a.Err = meta.GetRewardAdaPots(epoch, nil); a.Err != nil {
				return a
			}
			a.Snapshot, a.Err = meta.GetRewardSnapshot(epoch, "mark", nil)
			if a.Err != nil {
				return a
			}
			a.Failure, a.Err = meta.GetRewardSeedFailure(epoch, "mark", nil)
			if a.Err != nil {
				return a
			}
			if a.Pools, a.Err = meta.GetRewardPoolInputs(epoch, nil); a.Err != nil {
				return a
			}
			a.Stakes, a.Err = meta.GetRewardStakeInputs(epoch, nil)
			return normalizeInputs(a)
		},
		InputsAroundTheSet: func(epoch uint64) {
			meta := db.Metadata()
			pots, err := meta.GetRewardAdaPots(epoch, nil)
			require.NoError(t, err)
			require.NotNil(t, pots)
			pots.Treasury++
			require.NoError(t, meta.SaveRewardAdaPots(pots, nil))
			pools, err := meta.GetRewardPoolInputs(epoch, nil)
			require.NoError(t, err)
			require.NotEmpty(t, pools)
			pools[0].Pledge++
			require.NoError(t, meta.SaveRewardPoolInputs(pools[:1], nil))
			stakes, err := meta.GetRewardStakeInputs(epoch, nil)
			require.NoError(t, err)
			require.NotEmpty(t, stakes)
			stakes[0].Stake++
			require.NoError(t, meta.SaveRewardStakeInputs(stakes[:1], nil))
		},
		AddSetStake: func(key []byte, amount uint64) {
			a, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
				bytes.Repeat([]byte{0xaa}, 28), key,
			)
			require.NoError(t, err)
			raw, err := a.Bytes()
			require.NoError(t, err)
			out, err := cbor.Encode(map[uint]any{0: raw, 1: amount})
			require.NoError(t, err)
			tag++
			point := ocommon.NewPoint(4, bytes.Repeat([]byte{0x04}, 32))
			require.NoError(t, h.ls.config.UtxoSet.ApplyUtxoChanges(
				&point, []UtxoEvent{{
					Point: point,
					Added: []UtxoOutput{{
						Ref: models.UtxoId{
							Hash: bytes.Repeat([]byte{tag}, 32),
						},
						Cbor: out,
					}},
				}},
			))
		},
		PruneBefore: func(epoch uint64) {
			require.NoError(t, h.ls.LedgerDB().DeleteRewardStateBeforeEpoch(
				t.Context(), epoch, nil,
			))
		},
		Outputs: func(epoch uint64) RewardOutputsAnswer {
			return readOutputs(h.ls.LedgerDB(), epoch)
		},
		StoreOutputs: func(epoch uint64) RewardOutputsAnswer {
			a := readOutputs(dbadapter.New(db), epoch)
			a.Cursor, a.Err = db.GetSyncState(
				rewardPrecomputeCursorKey(epoch),
				nil,
			)
			return normalizeOutputs(a)
		},
		OutputsAroundTheSet: func(epoch uint64, accounts bool) {
			meta := db.Metadata()
			pools, err := meta.GetRewardPoolOutputs(epoch, nil)
			require.NoError(t, err)
			require.NotEmpty(t, pools)
			pools[0].TotalReward++
			require.NoError(t, meta.SaveRewardPoolOutputs(pools[:1], nil))
			if !accounts {
				return
			}
			rows, err := meta.GetRewardAccountOutputs(epoch, nil)
			require.NoError(t, err)
			if len(rows) > 0 {
				rows[0].Amount++
				require.NoError(t, meta.SaveRewardAccountOutputs(rows[:1], nil))
			}
		},
		PrecomputeRound: func() error {
			_, err := h.ls.runChunkedStakeRewardPrecomputeRound(
				replayBoundaryNewEpoch,
				replayBoundaryEndedEra*uint64(replayBoundaryLength),
				replayBoundaryNewEpoch*uint64(replayBoundaryLength),
			)
			return err
		},
		DeregisterMember: func() {
			ref := models.NewStakeCredentialRef(
				0,
				bytes.Repeat([]byte{0x72}, 28),
			)
			accts, err := h.ls.LedgerDB().GetAccountsByCredential(
				t.Context(), []models.StakeCredentialRef{ref}, true, nil,
			)
			require.NoError(t, err)
			acct := accts[ref.MapKey()]
			require.NotNil(t, acct)
			acct.Active = false
			stored := *acct
			require.NoError(t, db.Metadata().ImportAccount(&stored, nil))
			writer, ok := h.ls.config.UtxoSet.(LedgerChangesWriter)
			require.True(t, ok)
			require.NoError(t, writer.ApplyLedgerChanges(nil, LedgerChanges{
				Accounts: []AccountChange{{Slot: 11, Account: *acct}},
			}))
		},
		DeregisterMemberInSet: func() {
			ref := models.NewStakeCredentialRef(
				0,
				bytes.Repeat([]byte{0x72}, 28),
			)
			accts, err := h.ls.LedgerDB().GetAccountsByCredential(
				t.Context(), []models.StakeCredentialRef{ref}, true, nil,
			)
			require.NoError(t, err)
			acct := accts[ref.MapKey()]
			require.NotNil(t, acct)
			acct.Active = false
			writer, ok := h.ls.config.UtxoSet.(LedgerChangesWriter)
			require.True(t, ok)
			zero := uint64(0)
			require.NoError(t, writer.ApplyLedgerChanges(nil, LedgerChanges{
				Accounts: []AccountChange{{Slot: 11, Account: *acct}},
				StakeEvents: []StakeEventChange{{
					Ref: ref,
					Event: StakeEvent{
						Slot: 11, Position: PoolEventPosition(0, 0),
						Deposit: &zero, Class: StakeClassOther,
					},
				}},
			}))
		},
		Balance: func(key []byte) (uint64, uint64) {
			ldb := h.ls.LedgerDB()
			ref := models.NewStakeCredentialRef(0, key)
			accts, err := ldb.GetAccountsByCredential(
				t.Context(), []models.StakeCredentialRef{ref}, true, nil,
			)
			require.NoError(t, err)
			var reward, pending uint64
			if a := accts[ref.MapKey()]; a != nil {
				reward = uint64(a.Reward)
			}
			rows, err := ldb.GetPendingRewardAccountOutputsForCredential(
				0,
				key,
				nil,
			)
			require.NoError(t, err)
			for _, o := range rows {
				pending += uint64(o.Amount)
			}
			return reward, pending
		},
		Tip: func(slot uint64) {
			require.NoError(t, db.SetTip(ochainsync.Tip{
				Point: ocommon.NewPoint(
					slot,
					bytes.Repeat([]byte{byte(slot)}, 32),
				),
				BlockNumber: slot,
			}, nil))
		},
		ConfigureLeiosValidator: func() {
			h.ls.config.ValidateLeiosCertificate = func(uint64, []byte, []byte, []byte) error {
				return nil
			}
		},
		StageNoncesInSet: func(rows []models.BlockNonce) {
			h.ls.stageBlockNonces(nil, rows, 0)
		},
		PotsFees: func(epoch uint64) uint64 {
			pots, err := db.Metadata().GetRewardAdaPots(epoch, nil)
			require.NoError(t, err)
			require.NotNil(t, pots)
			return uint64(pots.Fees)
		},
		Boundary:               func() { h.applyBoundaryQuiet() },
		RollbackBeforeBoundary: h.rollbackBeforeBoundary,
		HideStoreReads: func(names ...string) (func() int64, func(bool)) {
			blind := h.blind
			require.NotNil(t, blind)
			clear(blind.denied)
			for _, n := range names {
				blind.denied[n] = true
			}
			blind.off.Store(false)
			return blind.hits.Load, func(on bool) { blind.off.Store(!on) }
		},
		DrainFamilies: h.ls.SetDrainedFamilies,
		FailSet:       func() { h.ls.utxoSetFailed.Store(true) },
		InstallDrainedSources: func() {
			h.ls.installNonceSource()
			h.ls.installEpochSource()
			h.ls.installPParamsSource()
			h.ls.installPoolSource()
		},
	}
}

func readOutputs(ldb ledgerdb.LedgerDB, epoch uint64) RewardOutputsAnswer {
	var a RewardOutputsAnswer
	if a.Pools, a.Err = ldb.GetRewardPoolOutputs(epoch, nil); a.Err != nil {
		return a
	}
	if a.Accounts, a.Err = ldb.GetRewardAccountOutputs(epoch, nil); a.Err != nil {
		return a
	}
	if a.Cursor, a.Err = ldb.GetSyncState(rewardPrecomputeCursorKey(epoch), nil); a.Err != nil {
		return a
	}
	a.Rounds, a.Err = ldb.GetPendingRewardCreditRounds(nil)
	return normalizeOutputs(a)
}

// seedRewardRound replaces the empty reward basis of snapshot epoch 3 with
// one pool, owned by the harness's owner credential, and two staked
// credentials, in the metadata store, for a set that loads them.
func seedRewardRound(t *testing.T, db *database.Database, pool []byte) {
	t.Helper()
	meta := db.Metadata()
	owner := bytes.Repeat([]byte{0x71}, 28)
	member := bytes.Repeat([]byte{0x72}, 28)
	const slot = uint64(10)
	// A decentralization of at least 4/5 gives every pool full performance
	// and needs no expected block count, which two-slot epochs have none of.
	rat := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	encoded, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 7, MinFeeA: 44, NOpt: 1,
		A0: rat(1, 2), Rho: rat(1, 2), Tau: rat(1, 2),
		Decentralization: rat(9, 10),
	})
	require.NoError(t, err)
	require.NoError(
		t,
		seedDB(t, db).SetPParams(encoded, 8, 4, eras.ShelleyEraDesc.Id, nil),
	)
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 5, Treasury: 1_000, Reserves: 9_000, CapturedSlot: slot,
	}, nil))
	require.NoError(t, meta.DeleteRewardInputsForEpoch(3, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: 3, SnapshotType: "mark", TotalActiveStake: 20_000_000_000_000_000,
		TotalPoolCount: 1, TotalDelegators: 2, CapturedSlot: slot,
		BoundarySlot: slot, ProtocolVersion: 7, Authoritative: true,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
		Epoch: 3, PoolKeyHash: pool, RewardAccount: owner,
		Margin: &types.Rat{Rat: big.NewRat(1, 100)}, Pledge: 5_000_000,
		DelegatedStake: 20_000_000_000_000_000, OwnerStake: 10_000_000_000_000_000,
		DelegatorCount: 2,
		CapturedSlot:   slot, BoundarySlot: slot,
	}}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch: 3, PoolKeyHash: pool, StakingKey: owner, Stake: 10_000_000_000_000_000,
			Owner: true, Registered: true, CapturedSlot: slot, BoundarySlot: slot,
		},
		{
			Epoch: 3, PoolKeyHash: pool, StakingKey: member, Stake: 10_000_000_000_000_000,
			Registered: true, CapturedSlot: slot, BoundarySlot: slot,
		},
	}, nil))
}
