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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const (
	replayBoundaryLength   = uint(2)
	replayBoundaryEndedEra = uint64(5)
	replayBoundaryNewEpoch = replayBoundaryEndedEra + 1
)

// boundaryReplayEffects is every durable and in-memory effect a normal
// successor NEWEPOCH leaves behind, read back after the rollover commits.
type boundaryReplayEffects struct {
	epoch         models.Epoch
	adaPots       *models.RewardAdaPots
	rewardMark    *models.RewardSnapshot
	stakeInputs   []*models.RewardStakeInput
	summary       *models.EpochSummary
	markRows      []*models.PoolStakeSnapshot
	network       *models.NetworkState
	pparamsRows   int
	ledgerEpoch   uint64
	currentEpoch  uint64
	snapshotCalls int
	pparams       lcommon.ProtocolParameters
}

func normalizeReplayEpoch(epoch models.Epoch) models.Epoch {
	epoch.ID = 0
	return epoch
}

func normalizeReplayMarkRows(
	rows []*models.PoolStakeSnapshot,
) []*models.PoolStakeSnapshot {
	out := make([]*models.PoolStakeSnapshot, 0, len(rows))
	for _, row := range rows {
		cp := *row
		cp.ID = 0
		out = append(out, &cp)
	}
	return out
}

func normalizeReplayStakeInputs(
	rows []*models.RewardStakeInput,
) []*models.RewardStakeInput {
	out := make([]*models.RewardStakeInput, 0, len(rows))
	for _, row := range rows {
		cp := *row
		cp.ID = 0
		out = append(out, &cp)
	}
	return out
}

// boundaryReplayHarness is a real ledger over a real chain and metadata
// store, with the production snapshot manager wired into the rollover hooks.
type boundaryReplayHarness struct {
	t         *testing.T
	fixture   *chainsyncRollbackFixture
	ls        *LedgerState
	poolHash  []byte
	snapCalls int
	next      models.Epoch
	// mgr is the snapshot manager wired to the ledger's hooks.
	mgr *snapshot.Manager
	// blind wraps the metadata store under the ledger's LedgerDB when the
	// set keeps epoch state.
	blind *blindStore
}

func newBoundaryReplayHarness(t *testing.T) *boundaryReplayHarness {
	t.Helper()
	return newBoundaryReplayHarnessWithDB(t, newTestDB(t))
}

func newBoundaryReplayHarnessWithDB(
	t *testing.T,
	db *database.Database,
) *boundaryReplayHarness {
	t.Helper()
	return newBoundaryReplayHarnessSeeded(t, db, true, nil)
}

// newBoundaryReplayHarnessSeeded is newBoundaryReplayHarnessWithDB over a
// database that also held, in SQL, the ledger state seed writes: the LedgerDB
// set takes it over with the harness's own when it opens. ownStake gives the
// harness's two delegators their staked outputs; a scenario that stakes them
// itself passes false.
func newBoundaryReplayHarnessSeeded(
	t *testing.T,
	db *database.Database,
	ownStake bool,
	seed func(),
) *boundaryReplayHarness {
	t.Helper()

	rat := func() *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(1, 2)} }
	pparams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor:    shelley.MinProtocolVersionShelley,
		MinFeeA:          44,
		NOpt:             1,
		A0:               rat(),
		Rho:              rat(),
		Tau:              rat(),
		Decentralization: rat(),
	}
	poolHash := bytes.Repeat([]byte{0xd1}, 28)
	ownerKey := bytes.Repeat([]byte{0x71}, 28)
	delegatorKey := bytes.Repeat([]byte{0x72}, 28)
	// The ledger's current block is the last of the ended epoch, so the
	// boundary the harness applies follows it as on a real chain.
	lastEndedSlot := replayBoundaryNewEpoch*uint64(replayBoundaryLength) - 1
	fixture := newChainsyncRollbackFixtureAt(t, db, lastEndedSlot, func() {
		for epochID := uint64(0); epochID <= replayBoundaryEndedEra; epochID++ {
			require.NoError(t, seedDB(t, db).SetEpoch(
				epochID*uint64(replayBoundaryLength), epochID,
				nil, nil, nil, nil,
				eras.ShelleyEraDesc.Id, 1000, replayBoundaryLength, nil,
			))
		}
		epochs, err := seedDB(t, db).GetEpochs(nil)
		require.NoError(t, err)
		require.Len(t, epochs, int(replayBoundaryNewEpoch))
		seedEmptyRewardBasisForRollover(
			t, db, epochs[len(epochs)-1], pparams,
		)
		margin := &types.Rat{Rat: big.NewRat(1, 100)}
		require.NoError(t, fxImportPool(t, db,
			context.Background(), nil,
			&models.Pool{
				PoolKeyHash:   poolHash,
				VrfKeyHash:    make([]byte, 32),
				Pledge:        5_000_000,
				Cost:          340_000_000,
				Margin:        margin,
				RewardAccount: ownerKey,
			},
			&models.PoolRegistration{
				PoolKeyHash:   poolHash,
				AddedSlot:     0,
				Pledge:        5_000_000,
				Cost:          340_000_000,
				Margin:        margin,
				VrfKeyHash:    make([]byte, 32),
				RewardAccount: ownerKey,
				Owners: []models.PoolRegistrationOwner{
					{KeyHash: append([]byte(nil), ownerKey...)},
				},
			},
		))
		for _, key := range [][]byte{ownerKey, delegatorKey} {
			require.NoError(t, seedCreateAccount(t, db,
				context.Background(), nil, &models.Account{
					StakingKey: key,
					Pool:       poolHash,
					AddedSlot:  0,
					Active:     true,
				},
			))
		}
		if ownStake {
			// Outputs the database held at the ancestor, so a rollback to
			// it keeps them.
			for i, key := range [][]byte{ownerKey, delegatorKey} {
				seedReplayOutput(t, db, byte(0x10+i), key, 20_000_000)
			}
		}
		if seed != nil {
			seed()
		}
	})
	ls := fixture.ls
	h := &boundaryReplayHarness{t: t, fixture: fixture, ls: ls}
	ls.installNonceSource()
	ls.installEpochSource()
	ls.installPoolSource()
	// Every component that reads through the ledger's LedgerDB, the snapshot
	// manager included, gets this one, so a test can later hide the metadata
	// store's reads from all of them.
	h.blind = &blindStore{
		LedgerDB: dbadapter.New(ls.db),
		denied:   map[string]bool{},
	}
	h.blind.off.Store(true)
	ls.ldb = stagingLedgerDB{LedgerDB: h.blind, ls: ls}
	ls.config.CardanoNodeConfig.ShelleyGenesisHash = "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	epochs, err := ls.ledgerDB().GetEpochs(nil)
	require.NoError(t, err)
	require.Len(t, epochs, int(replayBoundaryNewEpoch))
	ls.currentEra = eras.ShelleyEraDesc
	ls.currentPParams = pparams
	ls.epochCache = epochs
	ls.currentEpoch = epochs[len(epochs)-1]
	ls.ledgerEpoch = ls.currentEpoch.EpochId

	h.poolHash = poolHash

	mgr := snapshot.NewManager(ls.LedgerDB(), nil, nil)
	ls.SetEpochBoundarySnapshotStakeHook(
		func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
			return mgr.ComputeEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	ls.SetEpochBoundarySnapshotHook(
		func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
			h.snapCalls++
			return mgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	ls.SetCurrentBoundarySPOStakeHook(
		func(
			txn ledgerdb.Txn,
			evt event.EpochTransitionEvent,
			incomingEraID uint,
		) ([]*models.PoolStakeSnapshot, error) {
			return mgr.CurrentBoundarySPOStakeRows(
				context.Background(), txn, evt, incomingEraID,
			)
		},
	)
	epochBoundaryBenchWireDeferred(ls, mgr)
	h.mgr = mgr

	h.next = models.Epoch{
		EpochId:       replayBoundaryNewEpoch,
		StartSlot:     replayBoundaryNewEpoch * uint64(replayBoundaryLength),
		SlotLength:    1000,
		LengthInSlots: replayBoundaryLength,
		EraId:         eras.ShelleyEraDesc.Id,
	}
	return h
}

// replayStakedOutputCbor encodes an output of amount staked to stakingKey.
func replayStakedOutputCbor(
	t *testing.T,
	stakingKey []byte,
	amount uint64,
) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), stakingKey,
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr, OutputAmount: amount,
	})
	require.NoError(t, err)
	return encoded
}

// seedReplayOutput writes a live output of amount staked to stakingKey to
// db's metadata store, which the LedgerDB set takes over when it opens.
func seedReplayOutput(
	t *testing.T,
	db *database.Database,
	id byte,
	stakingKey []byte,
	amount uint64,
) {
	t.Helper()
	txID := bytes.Repeat([]byte{id}, 32)
	encoded := replayStakedOutputCbor(t, stakingKey, amount)
	fxSeedOutput(t, db, txID, 0, 0, encoded)
}

// addUtxo puts a live output of amount, staked to stakingKey and created at
// slot, into the harness's LedgerDB set.
func (h *boundaryReplayHarness) addUtxo(
	id byte,
	stakingKey []byte,
	amount uint64,
	slot uint64,
) {
	h.t.Helper()
	encoded := replayStakedOutputCbor(h.t, stakingKey, amount)
	require.NoError(h.t, h.ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{
		Point: ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{id}, 32)},
		Added: []UtxoOutput{{
			Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{id}, 32)},
			Cbor: encoded,
		}},
	}}))
}

// applyBoundary runs the successor rollover the way ledgerProcessBlocks does:
// it hands the rollover the ledger's own epoch label, commits, and then
// publishes the result into the in-memory state.
func (h *boundaryReplayHarness) applyBoundary() boundaryReplayEffects {
	h.applyBoundaryQuiet()
	return h.readEffects()
}

// runBoundary runs the successor rollover in its own write transaction and
// returns what it computed, or the error that rolled the transaction back.
func (h *boundaryReplayHarness) runBoundary() (*EpochRolloverResult, error) {
	h.t.Helper()
	ls := h.ls
	h.snapCalls = 0
	var result *EpochRolloverResult
	txn := ls.db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRolloverForTargetWithClassicPParamsContext(
			context.Background(),
			txn,
			ls.currentEpoch,
			h.next,
			h.next,
			ls.ledgerEpoch,
			ls.currentEra,
			ls.currentPParams,
			false,
			false,
			nil,
		)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// applyBoundaryQuiet applies the rollover and publishes its result without
// reading the metadata store back.
func (h *boundaryReplayHarness) applyBoundaryQuiet() {
	h.t.Helper()
	ls := h.ls
	result, err := h.runBoundary()
	require.NoError(h.t, err)
	require.NotNil(h.t, result)
	ls.Lock()
	ls.epochCache = result.NewEpochCache
	ls.currentEpoch = result.NewCurrentEpoch
	ls.currentEra = result.NewCurrentEra
	ls.currentPParams = result.NewCurrentPParams
	ls.ledgerEpoch = result.newLedgerEpoch
	ls.checkpointWrittenForEpoch = result.CheckpointWrittenForEpoch
	ls.Unlock()
}

func (h *boundaryReplayHarness) readEffects() boundaryReplayEffects {
	h.t.Helper()
	meta := h.ls.ledgerDB()
	var e boundaryReplayEffects
	epoch, err := meta.GetEpoch(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	if epoch != nil {
		e.epoch = normalizeReplayEpoch(*epoch)
	}
	e.adaPots, err = meta.GetRewardAdaPots(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	if e.adaPots != nil {
		e.adaPots.ID = 0
	}
	e.rewardMark, err = meta.GetRewardSnapshot(
		replayBoundaryNewEpoch, "mark", nil,
	)
	require.NoError(h.t, err)
	if e.rewardMark != nil {
		e.rewardMark.ID = 0
	}
	inputs, err := meta.GetRewardStakeInputs(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	e.stakeInputs = normalizeReplayStakeInputs(inputs)
	e.summary, err = meta.GetEpochSummary(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	if e.summary != nil {
		e.summary.ID = 0
	}
	marks, err := meta.GetPoolStakeSnapshotsByEpoch(
		replayBoundaryNewEpoch, models.PoolStakeSnapshotTypeMark, nil,
	)
	require.NoError(h.t, err)
	e.markRows = normalizeReplayMarkRows(marks)
	e.network, err = meta.GetNetworkState(nil)
	require.NoError(h.t, err)
	if e.network != nil {
		e.network.ID = 0
	}
	e.pparams = h.ls.currentPParams
	e.ledgerEpoch = h.ls.ledgerEpoch
	e.currentEpoch = h.ls.currentEpoch.EpochId
	e.snapshotCalls = h.snapCalls
	return e
}

func (h *boundaryReplayHarness) rollbackBeforeBoundary() {
	h.t.Helper()
	h.rollbackBeforeBoundaryRepairing(false)
}

// rollbackBeforeBoundaryRepairing is rollbackBeforeBoundary for a ledger whose
// tip already sits at the target, where only a repairing rollback truncates.
func (h *boundaryReplayHarness) rollbackBeforeBoundaryRepairing(repair bool) {
	h.t.Helper()
	// A scenario that writes below the rollback target leaves a production
	// set's tip there; the set rolls back along its own journal, so it is
	// brought up to the target, which the ledger is already past.
	if tipper, ok := h.ls.config.UtxoSet.(interface {
		Tip() (ocommon.Point, bool, error)
	}); ok {
		target := h.fixture.ancestorTip.Point
		tip, _, err := tipper.Tip()
		require.NoError(h.t, err)
		if tip.Slot < target.Slot {
			require.NoError(h.t, h.ls.config.UtxoSet.ApplyUtxoChanges(&target, nil))
		}
	}
	require.NoError(h.t, h.ls.rollbackWithBlocksAndIntent(
		context.Background(),
		h.fixture.ancestorTip.Point,
		nil,
		repair,
		false,
		false,
	))
}

// A rollback across epoch E followed by re-application of E's first block
// must run E's EPOCH effects again. cardano-ledger restores the whole
// NewEpochState snapshot, so the re-applied boundary is a fresh successor
// NEWEPOCH; a skipped effect would split the chain from the Haskell node.
func TestRollbackAcrossEpochBoundaryReplaysEpochEffects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// forkStake is the extra stake the re-applied chain carries at the
		// boundary; the original chain carries origStake.
		forkStake uint64
	}{
		{name: "same chain", forkStake: 7_000_000},
		{name: "fork with different stake", forkStake: 9_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newBoundaryReplayHarness(t)
			delegatorKey := bytes.Repeat([]byte{0x72}, 28)
			boundarySlot := h.next.StartSlot
			require.Greater(t, boundarySlot, h.fixture.ancestorTip.Point.Slot)

			// Stake credited by a block of the epoch being closed, above the
			// rollback point, so the rollback deletes it.
			const origStake = uint64(7_000_000)
			h.addUtxo(0x33, delegatorKey, origStake, boundarySlot-1)
			preBoundaryPParams, err := cbor.Encode(h.ls.currentPParams)
			require.NoError(t, err)
			first := h.applyBoundary()

			require.Equal(t, replayBoundaryNewEpoch, first.currentEpoch)
			require.Equal(t, replayBoundaryNewEpoch, first.ledgerEpoch)
			require.Equal(t, 1, first.snapshotCalls,
				"the first application must run the SNAP capture")
			require.NotNil(t, first.adaPots,
				"the successor rollover persists the ADA pots")
			require.NotNil(t, first.summary)
			require.NotEmpty(t, first.markRows)
			require.NotNil(t, first.rewardMark)
			require.Equal(t, models.PoolStakeSnapshotTypeMark,
				first.markRows[0].SnapshotType)
			firstStake := uint64(first.markRows[0].TotalStake)

			h.rollbackBeforeBoundary()

			epoch, err := seedDB(t, h.ls.db).GetEpoch(replayBoundaryNewEpoch, nil)
			require.NoError(t, err)
			require.Nil(t, epoch, "the rollback must delete epoch E's row")
			pots, err := seedDB(t, h.ls.db).GetRewardAdaPots(
				replayBoundaryNewEpoch, nil,
			)
			require.NoError(t, err)
			require.Nil(t, pots, "the rollback must delete epoch E's pots")
			staleMarks, err := seedDB(t, h.ls.db).GetPoolStakeSnapshotsByEpoch(
				replayBoundaryNewEpoch, models.PoolStakeSnapshotTypeMark, nil,
			)
			require.NoError(t, err)
			require.Empty(t, staleMarks,
				"the rollback must delete epoch E's mark snapshot rows")
			staleSummary, err := seedDB(t, h.ls.db).GetEpochSummary(
				replayBoundaryNewEpoch, nil,
			)
			require.NoError(t, err)
			require.Nil(t, staleSummary,
				"the rollback must delete epoch E's summary")
			require.Equal(t, replayBoundaryEndedEra, h.ls.currentEpoch.EpochId)
			require.True(t, h.ls.checkpointWrittenForEpoch,
				"the rollback target's epoch already has its checkpoint row, "+
					"so replay must not write a second one")
			rewoundPParams, err := cbor.Encode(h.ls.currentPParams)
			require.NoError(t, err)
			require.Equal(t, preBoundaryPParams, rewoundPParams,
				"the rollback must restore the pre-boundary protocol parameters")

			h.addUtxo(0x34, delegatorKey, tc.forkStake, boundarySlot-1)
			second := h.applyBoundary()

			require.Equal(t, 1, second.snapshotCalls,
				"the re-applied boundary must run the SNAP capture again; "+
					"zero means the jump path skipped every EPOCH effect")
			require.Equal(t, first.currentEpoch, second.currentEpoch)
			require.Equal(t, first.ledgerEpoch, second.ledgerEpoch)
			require.NotNil(t, second.adaPots)
			require.NotEmpty(t, second.markRows)
			require.Equal(t, first.epoch, second.epoch)
			require.Equal(t, first.adaPots, second.adaPots)
			require.Equal(t, first.network, second.network)
			require.Equal(t, first.stakeInputs != nil, second.stakeInputs != nil)

			wantStake := firstStake - origStake + tc.forkStake
			require.Equal(t, wantStake, uint64(second.markRows[0].TotalStake),
				"the mark snapshot must reflect the re-applied chain, not "+
					"the rolled-back one")
			require.Len(t, second.markRows, len(first.markRows),
				"the rolled-back boundary's mark rows must not be duplicated")
			if tc.forkStake == origStake {
				require.Equal(t, first.markRows, second.markRows)
				require.Equal(t, first.rewardMark, second.rewardMark)
				require.Equal(t, first.summary, second.summary)
				require.Equal(t, first.stakeInputs, second.stakeInputs)
			}
		})
	}
}

// dumpAllMetadataTables renders every row of every metadata table as text so
// two states can be compared without enumerating what a boundary writes.
func dumpAllMetadataTables(
	t *testing.T,
	raw *sql.DB,
) map[string][]string {
	t.Helper()
	names, err := raw.Query(
		`SELECT name FROM sqlite_master WHERE type = 'table' ` +
			`AND name NOT LIKE 'sqlite_%'`,
	)
	require.NoError(t, err)
	var tables []string
	for names.Next() {
		var name string
		require.NoError(t, names.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, names.Err())
	require.NoError(t, names.Close())
	out := make(map[string][]string, len(tables))
	for _, table := range tables {
		rows, err := raw.Query("SELECT * FROM `" + table + "`")
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		var lines []string
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			parts := make([]string, len(cols))
			for i, v := range vals {
				if b, ok := v.([]byte); ok {
					parts[i] = fmt.Sprintf("%s=%x", cols[i], b)
				} else {
					parts[i] = fmt.Sprintf("%s=%v", cols[i], v)
				}
			}
			lines = append(lines, strings.Join(parts, " "))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sort.Strings(lines)
		out[table] = lines
	}
	return out
}

func diffMetadataTables(before, after map[string][]string) []string {
	var diffs []string
	for table, want := range before {
		got := after[table]
		if slices.Equal(want, got) {
			continue
		}
		for _, line := range want {
			if !slices.Contains(got, line) {
				diffs = append(diffs, table+" lost: "+line)
			}
		}
		for _, line := range got {
			if !slices.Contains(want, line) {
				diffs = append(diffs, table+" added: "+line)
			}
		}
	}
	sort.Strings(diffs)
	return diffs
}

func TestRollbackAcrossEpochBoundaryRestoresEveryTable(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	h := newBoundaryReplayHarnessWithDB(t, db)
	raw, err := dbtest.RawSQLiteMetadata(t, h.ls.db)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	delegatorKey := bytes.Repeat([]byte{0x72}, 28)
	// Rolling back without crossing the boundary first drops everything the
	// blocks above the target contributed, leaving only what the boundary
	// itself could add or alter.
	h.addUtxo(0x33, delegatorKey, 7_000_000, h.next.StartSlot-1)
	h.rollbackBeforeBoundary()
	before := dumpComparableTables(t, raw)
	h.addUtxo(0x33, delegatorKey, 7_000_000, h.next.StartSlot-1)
	h.applyBoundary()
	h.rollbackBeforeBoundaryRepairing(true)
	after := dumpComparableTables(t, raw)
	diffs := diffMetadataTables(before, after)
	for _, d := range diffs {
		t.Log(d)
	}
	require.Empty(t, diffs)
}

// A snapshot whose reward_stake_input rows are marked incomplete keeps the
// mark through a rollback that keeps its boundary and loses it with one that
// rewinds the boundary: the rollback deletes the snapshot, so nothing remains
// for the mark to describe and the incomplete check would report the epoch as
// unfinished.
func TestRollbackDiscardsPendingRewardStakeInputsOfRewoundBoundary(
	t *testing.T,
) {
	h := newBoundaryReplayHarness(t)
	h.applyBoundary()
	snapshot, err := h.ls.ledgerDB().GetRewardSnapshot(
		replayBoundaryNewEpoch, "mark", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.NoError(t, markRewardStakeInputsPending(
		h.ls.ledgerDB(), nil, replayBoundaryNewEpoch, snapshot.BoundarySlot,
	))

	incomplete, err := h.ls.rewardStakeInputsIncompleteCommitted(
		replayBoundaryNewEpoch, replayBoundaryNewEpoch+1,
	)
	require.NoError(t, err)
	require.True(t, incomplete,
		"the snapshot's basis must read as incomplete while marked")

	h.rollbackBeforeBoundary()

	pending, err := epochengine.LoadRewardStakeInputsPending(h.ls.ledgerDB(), nil)
	require.NoError(t, err)
	require.Empty(t, pending.Entries,
		"the rollback deleted the snapshot the entry describes")
}

// The boundary's mark snapshot is captured once, inside the rollover
// transaction. A capture that fails rolls the whole rollover back, and the
// pipeline applies the boundary block again: that attempt must close the
// epoch exactly as a boundary whose capture never failed does, with the
// stake distribution the next epochs elect leaders and pay rewards from.
func TestEpochBoundaryCompletesAfterItsMarkCaptureFailsOnce(t *testing.T) {
	t.Parallel()

	delegatorKey := bytes.Repeat([]byte{0x72}, 28)
	const stake = uint64(7_000_000)

	control := newBoundaryReplayHarness(t)
	control.addUtxo(0x33, delegatorKey, stake, control.next.StartSlot-1)
	want := control.applyBoundary()
	require.NotEmpty(t, want.markRows)
	require.NotNil(t, want.rewardMark)

	h := newBoundaryReplayHarness(t)
	h.addUtxo(0x33, delegatorKey, stake, h.next.StartSlot-1)
	before := h.readEffects()
	errCapture := errors.New("mark capture interrupted")
	failures := 1
	h.ls.SetEpochBoundarySnapshotHook(
		func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
			h.snapCalls++
			err := h.mgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
			if err == nil && failures > 0 {
				failures--
				return errCapture
			}
			return err
		},
	)

	result, err := h.runBoundary()
	require.ErrorIs(t, err, errCapture)
	require.Nil(t, result)
	failed := h.readEffects()
	failed.snapshotCalls = before.snapshotCalls
	require.Equal(t, before, failed,
		"a boundary whose capture failed must leave the ledger where it was")

	got := h.applyBoundary()
	require.Equal(t, want, got)
}
