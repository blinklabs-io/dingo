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
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// UtxoSetWriter keeps a durable copy of the live UTxO set in step with the
// ledger.
//
// ApplyUtxoChanges receives, once per ledger commit, every UTxO change the
// commit made, in order, and must apply them atomically. tip is the ledger
// point the commit left the ledger at; it is nil for a commit that changed
// the set without moving the ledger, such as the Allegra AVVM removal. Every
// block applied reports its point, even one with no transactions. The call
// is made synchronously on the committing goroutine, after the commit and
// before the next block is validated, so the copy's recorded tip is always a
// point at which it equals the ledger's live set. A copy whose tip trails the
// ledger's is caught up by ResumeUtxoSetFrom.
type UtxoSetWriter interface {
	ApplyUtxoChanges(tip *ocommon.Point, changes []UtxoEvent) error
}

// LedgerChanges is one ledger commit's changes to a LedgerDB store: its UTxO
// changes, the nonces of the blocks it applied, and its nonce prune.
type LedgerChanges struct {
	Utxo   []UtxoEvent
	Nonces []models.BlockNonce
	// PruneNoncesBefore, when non-zero, drops the non-checkpoint nonces
	// before that slot.
	PruneNoncesBefore uint64
	// Epochs are the epoch rows the commit wrote, as the metadata store
	// holds them after the write.
	Epochs []models.Epoch
	// Snapshots are the commit's pool stake snapshot changes, in order.
	Snapshots []SnapshotChange
	// Summaries are the epoch summary rows the commit wrote, as the metadata
	// store holds them after the write, in order.
	Summaries []models.EpochSummary
	// EraSwitch is the mid-epoch era switch the commit recorded, if any.
	EraSwitch *database.MidEpochEraSwitch
	// NetworkStates and Donations are the network state and donation rows
	// the commit wrote, in order.
	NetworkStates []models.NetworkState
	Donations     []models.NetworkDonation
	// PParams are the protocol parameter rows the commit wrote, in order.
	PParams []models.PParams
	// Accounts are the account changes the commit made, in order.
	Accounts []AccountChange
	// RewardRecords are the reward credits and withdrawals those changes
	// applied.
	RewardRecords []RewardRecord
	// DReps, DRepLinks and Dormancy are the commit's DRep state changes,
	// each in order.
	DReps     []DRepChange
	DRepLinks []DRepLinkChange
	Dormancy  []DormancyChange
	// DRepHistory are the history rows the commit's DRep changes recorded, in
	// order.
	DRepHistory []DRepHistoryChange
	// Pools are the commit's pool state changes, in order.
	Pools []PoolChange
	// PoolEvents are the pool registration and retirement certificates the
	// commit applied, in order.
	PoolEvents []PoolEventChange
	// StakeEvents are the stake registration and deregistration
	// certificates the commit applied, in order.
	StakeEvents []StakeEventChange
	// DelegationEvents are the pool delegations the commit applied, in
	// order.
	DelegationEvents []DelegationEventChange
	// DRepDeposits are the DRep registrations the commit applied, in order.
	DRepDeposits []DRepDepositChange
	// GenesisDelegations are the genesis key delegation certificates the
	// commit applied, in order.
	GenesisDelegations []models.GenesisDelegation
	// MIRCerts are the MIR certificates the commit applied, in order.
	MIRCerts []MIRCert
	// RewardOutputs are the reward account output writes, in order.
	RewardOutputs []RewardOutputChange
	// RewardRounds are the reward credit rounds applied, in order.
	RewardRounds []RewardRoundChange
	// PostSnapshotCredits are the credits a boundary applied after its SNAP
	// point, in order.
	PostSnapshotCredits []PostSnapshotCredit
	// OpCerts are the commit's op-cert counter changes, in order.
	OpCerts []OpCertChange
	// RewardBlocks are the commit's changes to the per-epoch block counts
	// and fees reward calculation reads, in order.
	RewardBlocks []RewardBlockChange
	// RewardInputs are the commit's changes to the reward snapshot inputs, in
	// order.
	RewardInputs []RewardInputChange
	// RewardDerived are the commit's changes to the derived reward outputs
	// and cursors, in order.
	RewardDerived []RewardDerivedChange
	// Committee are the commit's committee and constitution row puts, in
	// order.
	Committee []CommitteeChange
	// Proposals and Votes are the commit's governance proposal and vote
	// row puts, in order.
	Proposals []ProposalChange
	Votes     []VoteChange
	// PParamUpdates are the commit's protocol parameter update proposals,
	// in order.
	PParamUpdates []PParamUpdateChange
	// SyncMarkers are the commit's protocol parameter sync marker writes,
	// in order.
	SyncMarkers []SyncMarkerChange
	// ImportBaselines are the account import baselines the commit wrote, in
	// order.
	ImportBaselines []ImportBaselineChange
	// Witnesses are the slots the commit's certificates and withdrawals
	// witnessed credentials at, in order.
	Witnesses []WitnessChange
}

// SyncMarkerChange sets the protocol parameter sync marker Name, one of the
// database.SyntheticV2CostModelSyncKey keys, to Value, or deletes it.
type SyncMarkerChange struct {
	Name   string
	Value  string
	Delete bool
}

// PParamUpdateChange is a protocol parameter update proposal stored at
// Update.AddedSlot by the transaction at Index in its block.
type PParamUpdateChange struct {
	Index  uint32
	Update models.PParamUpdate
}

// DRepChange is a DRep as the change at Slot leaves it, or its removal.
type DRepChange struct {
	Slot   uint64
	Drep   models.Drep
	Delete bool
}

// DRepLinkChange opens, or closes, Stake's link in Drep's delegator set at
// Slot.
type DRepLinkChange struct {
	Slot  uint64
	Drep  models.StakeCredentialRef
	Stake models.StakeCredentialRef
	Live  bool
}

// DRepHistoryKind is what a DRepHistoryChange records.
type DRepHistoryKind uint8

// The DRep history kinds, the rows of the metadata store's
// drep_expiry_history, drep_dormancy_history, drep_expiry_epoch_event and
// account_drep_clear tables.
const (
	DRepHistoryExpiry DRepHistoryKind = iota
	DRepHistoryDormancy
	DRepHistoryEvent
	DRepHistoryClear
)

// DRepHistoryChange is a DRep history row recorded at Slot. Tag and Key name
// the DRep of an expiry row and the account of a clear; Activity and Expiry
// are an expiry row's epochs; Previous is the dormancy counter a dormancy
// row records.
type DRepHistoryChange struct {
	Kind     DRepHistoryKind
	Slot     uint64
	Tag      uint8
	Key      []byte
	Activity uint64
	Expiry   uint64
	Previous uint64
}

// DormancyChange sets the DRep dormancy counter at Slot.
type DormancyChange struct {
	Slot   uint64
	Epochs uint64
}

// RewardRecord identifies one reward credit, by its source, or one
// withdrawal, by its transaction, applied to an account at Slot.
type RewardRecord struct {
	Slot       uint64
	Withdrawal bool
	Source     []byte
	Ref        models.StakeCredentialRef
	// Amount is the lovelace credited or withdrawn.
	Amount uint64
	// PostSnapshot marks a credit a boundary applied after its SNAP point.
	PostSnapshot bool
	// Seq orders the records the set holds by when it wrote them. The set
	// assigns it; it is zero on a record being staged.
	Seq uint64
}

// AccountChange is an account as the change at Slot leaves it.
type AccountChange struct {
	Slot    uint64
	Account models.Account
}

// SnapshotChangeKind is what a SnapshotChange does.
type SnapshotChangeKind uint8

const (
	// SnapshotSave stores Snapshot.
	SnapshotSave SnapshotChangeKind = iota
	// SnapshotDeleteForEpoch deletes Epoch's snapshots of Type.
	SnapshotDeleteForEpoch
	// SnapshotDeleteBeforeEpoch deletes the snapshots of epochs before Epoch.
	SnapshotDeleteBeforeEpoch
)

// SnapshotChange is one change to the pool stake snapshots.
type SnapshotChange struct {
	Kind     SnapshotChangeKind
	Snapshot *models.PoolStakeSnapshot
	Epoch    uint64
	Type     string
}

// LedgerChangesWriter is a UtxoSetWriter that also keeps block nonces,
// written in the same store transaction as the commit's UTxO changes.
type LedgerChangesWriter interface {
	UtxoSetWriter
	ApplyLedgerChanges(tip *ocommon.Point, changes LedgerChanges) error
}

// BlockNonceReader serves block nonces from a LedgerChangesWriter.
type BlockNonceReader interface {
	// NoncesLoaded reports whether the reader holds every nonce up to its
	// tip; until it does, nonce reads stay on the metadata store.
	NoncesLoaded() (bool, error)
	BlockNonce(point ocommon.Point) ([]byte, bool, error)
	BlockNoncesInSlotRange(start, end uint64) ([]models.BlockNonce, error)
	LatestBlockNonce() (models.BlockNonce, bool, error)
}

// UtxoSetRollbacker rolls a live UTxO set back from its own history, since
// the metadata store holds no utxo rows to supply a rollback's change. It
// returns the net change as a rollback UtxoEvent.
type UtxoSetRollbacker interface {
	RollbackUtxoSet(point ocommon.Point) (UtxoEvent, error)
	// PreviewUtxoSetRollback returns the change RollbackUtxoSet(point)
	// would make, without making it.
	PreviewUtxoSetRollback(point ocommon.Point) (UtxoEvent, error)
}

// UtxoSetFailureMarker is a live UTxO set that can record durably that the
// ledger failed to apply a commit to it, so the next startup rewinds it to
// before that commit instead of trusting its tip. A zero slot is a failure
// the ledger could not place.
type UtxoSetFailureMarker interface {
	MarkUtxoSetFailed(slot uint64) error
}

// errLiveUtxoSetUnavailable reports a read of a live UTxO set that stopped
// following the ledger when nothing else holds the UTxO set.
var errLiveUtxoSetUnavailable = errors.New(
	"live UTxO set stopped following the ledger; restart to resume it",
)

// utxoSetBatch is one commit's changes for the live UTxO set.
type utxoSetBatch struct {
	// txn is the write transaction staging the batch, nil for a batch
	// applied at once; seq orders batches by when they began staging.
	txn ledgerdb.Txn
	seq uint64
	tip *ocommon.Point
	// rollbackTo rolls the set back to a point from its journal before
	// changes apply.
	rollbackTo *ocommon.Point
	// rollbackRefs are the stake credentials of the outputs the rollback
	// changes, whose live reward aggregate the truncation must refresh.
	rollbackRefs []models.StakeCredentialRef
	changes      []UtxoEvent
	// nonces are the block nonces txn wrote, and pruneNoncesBefore the
	// slot its nonce prune reached, for a set that keeps nonces.
	nonces            []models.BlockNonce
	pruneNoncesBefore uint64
	// epochs and snapshots are the epoch rows and snapshot changes txn
	// wrote, for a set that keeps epoch state.
	epochs    []models.Epoch
	snapshots []SnapshotChange
	summaries []models.EpochSummary
	eraSwitch *database.MidEpochEraSwitch
	// networkStates and donations are the network state and donation rows
	// txn wrote, for a set that keeps them.
	networkStates []models.NetworkState
	donations     []models.NetworkDonation
	pparams       []models.PParams
	accounts      []AccountChange
	rewardRecords []RewardRecord
	dreps         []DRepChange
	drepLinks     []DRepLinkChange
	dormancy      []DormancyChange
	drepHistory   []DRepHistoryChange
	pools         []PoolChange
	poolEvents    []PoolEventChange
	stakeEvents   []StakeEventChange
	delegations   []DelegationEventChange
	drepDeposits  []DRepDepositChange
	genesisDelegs []models.GenesisDelegation
	mirCerts      []MIRCert
	rewardOutputs []RewardOutputChange
	rewardRounds  []RewardRoundChange
	postSnapshot  []PostSnapshotCredit
	opCerts       []OpCertChange
	rewardBlocks  []RewardBlockChange
	rewardInputs  []RewardInputChange
	rewardDerived []RewardDerivedChange
	committee     []CommitteeChange
	proposals     []ProposalChange
	votes         []VoteChange
	pparamUpdates []PParamUpdateChange
	syncMarkers   []SyncMarkerChange
	// importBaselines and witnesses are the account import baselines and
	// witness slots txn wrote, for a set that keeps accounts.
	importBaselines []ImportBaselineChange
	witnesses       []WitnessChange
	// overlay is each changed output's last staged state, and stake each
	// stake sum's staged change, for reads in the same transaction.
	overlay map[utxoref.Key]utxoSetOverlayEntry
	stake   map[string]int64
}

// hasWrites reports whether the batch writes anything to the set besides
// its tip and rollback.
func (b *utxoSetBatch) hasWrites() bool {
	return len(b.changes) > 0 || len(b.nonces) > 0 ||
		b.pruneNoncesBefore != 0 || len(b.epochs) > 0 ||
		len(b.snapshots) > 0 || len(b.summaries) > 0 || b.eraSwitch != nil ||
		len(b.networkStates) > 0 || len(b.donations) > 0 ||
		len(b.pparams) > 0 || len(b.accounts) > 0 ||
		len(b.rewardRecords) > 0 || len(b.dreps) > 0 ||
		len(b.drepLinks) > 0 || len(b.dormancy) > 0 ||
		len(b.drepHistory) > 0 || len(b.pools) > 0 || len(b.poolEvents) > 0 ||
		len(b.stakeEvents) > 0 || len(b.delegations) > 0 ||
		len(b.drepDeposits) > 0 ||
		len(b.genesisDelegs) > 0 || len(b.mirCerts) > 0 ||
		len(b.rewardOutputs) > 0 || len(b.rewardRounds) > 0 ||
		len(b.postSnapshot) > 0 ||
		len(b.opCerts) > 0 || len(b.rewardBlocks) > 0 ||
		len(b.rewardInputs) > 0 || len(b.rewardDerived) > 0 ||
		len(b.committee) > 0 ||
		len(b.proposals) > 0 || len(b.votes) > 0 ||
		len(b.pparamUpdates) > 0 || len(b.syncMarkers) > 0 ||
		len(b.importBaselines) > 0 ||
		len(b.witnesses) > 0
}

// failUtxoSet stops the live UTxO set following the ledger, records the
// failure in the set, and halts the node through FatalErrorFunc, since
// nothing else answers for the set's rows; startup resumes the set. slot is
// the commit the update failed at, zero when unknown.
func (ls *LedgerState) failUtxoSet(
	slot uint64,
	msg string,
	err error,
) {
	first := !ls.utxoSetFailed.Swap(true)
	attrs := []any{"component", "ledger", "error", err}
	if slot != 0 {
		attrs = append(attrs, "slot", slot)
	}
	ls.config.Logger.Error(msg, attrs...)
	if !first {
		return
	}
	if m, ok := ls.config.UtxoSet.(UtxoSetFailureMarker); ok {
		if markErr := m.MarkUtxoSetFailed(slot); markErr != nil {
			ls.config.Logger.Error(
				"live UTxO set failure could not be recorded",
				"component", "ledger",
				"error", markErr,
			)
		}
	}
	if ls.config.FatalErrorFunc != nil {
		ls.config.FatalErrorFunc(fmt.Errorf(
			"live UTxO set stopped following the ledger; the next start resumes it: %w",
			err,
		))
	}
}

// stageUtxoSet adds changes, and tip when it is not nil, to txn's batch for
// the live UTxO set, which is applied once txn commits. Callers stage before
// the commit, in the order the changes apply. Without a txn the batch is
// applied at once.
func (ls *LedgerState) stageUtxoSet(
	txn ledgerdb.Txn,
	tip *ocommon.Point,
	changes ...UtxoEvent,
) {
	if txn == nil {
		ls.applyUtxoSet(&utxoSetBatch{tip: tip, changes: changes})
		return
	}
	batch := ls.utxoSetBatchForTxn(txn, func(b *utxoSetBatch) {
		b.changes = append(b.changes, changes...)
		if tip != nil {
			t := *tip
			b.tip = &t
		}
	})
	if r, readable := ls.config.UtxoSet.(UtxoSetReader); readable {
		if err := ls.stageUtxoOverlay(r, txn, batch, changes); err != nil {
			slot := uint64(0)
			if tip != nil {
				slot = tip.Slot
			}
			ls.failUtxoSet(
				slot,
				"live UTxO set read failed while staging; it stops following the ledger until startup resumes it",
				err,
			)
		}
	}
}

// utxoSetBatchForTxn returns txn's batch, creating and registering it on
// first use, after calling stage on it under utxoSetMu.
func (ls *LedgerState) utxoSetBatchForTxn(
	txn ledgerdb.Txn,
	stage func(*utxoSetBatch),
) *utxoSetBatch {
	ls.utxoSetMu.Lock()
	if ls.utxoSetBatches == nil {
		ls.utxoSetBatches = make(map[ledgerdb.Txn]*utxoSetBatch)
	}
	batch, ok := ls.utxoSetBatches[txn]
	if !ok {
		ls.utxoSetSeq++
		batch = &utxoSetBatch{txn: txn, seq: ls.utxoSetSeq}
		ls.utxoSetBatches[txn] = batch
		var meta types.Txn
		if dbTxn := dbadapter.Unwrap(txn); dbTxn != nil {
			meta = dbTxn.Metadata()
		}
		if meta != nil {
			if ls.utxoSetByMeta == nil {
				ls.utxoSetByMeta = make(map[types.Txn]*utxoSetBatch)
			}
			ls.utxoSetByMeta[meta] = batch
		}
	}
	stage(batch)
	ls.utxoSetMu.Unlock()
	if ok {
		return batch
	}
	// The batch is complete before the commit, so one callback applies it.
	// A committed batch stays in the maps until it is applied: another
	// write can start between the commit and the apply, and must see the
	// set as the commit left it (see utxoSetBatchesSeenBy). A rolled-back
	// one is dropped when the transaction finishes.
	txn.OnFinish(func() {
		if !txn.IsCommitted() {
			ls.forgetUtxoSetBatch(batch)
		}
	})
	txn.AfterCommit(func() { ls.applyUtxoSet(batch) })
	return batch
}

// markUtxoSetBatch records what txn has staged for the live UTxO set and
// returns a function that drops everything staged after the mark. A caller
// that rolls txn back to a savepoint calls it, because the savepoint undoes
// only the metadata writes: without it the staged changes of the abandoned
// work would still reach the set when txn commits.
func (ls *LedgerState) markUtxoSetBatch(txn ledgerdb.Txn) func() {
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	var saved *utxoSetBatch
	if batch, ok := ls.utxoSetBatches[txn]; ok {
		cp := *batch
		cp.overlay = maps.Clone(batch.overlay)
		cp.stake = maps.Clone(batch.stake)
		saved = &cp
	}
	return func() {
		ls.utxoSetMu.Lock()
		defer ls.utxoSetMu.Unlock()
		batch, ok := ls.utxoSetBatches[txn]
		if !ok {
			return
		}
		if saved == nil {
			*batch = utxoSetBatch{txn: batch.txn, seq: batch.seq}
			return
		}
		restored := *saved
		restored.overlay = maps.Clone(saved.overlay)
		restored.stake = maps.Clone(saved.stake)
		*batch = restored
	}
}

// stageBlockNonces stages nonces, for a set that keeps them, to be written
// with txn's UTxO changes after it commits.
func (ls *LedgerState) stageBlockNonces(
	txn ledgerdb.Txn,
	nonces []models.BlockNonce,
	pruneBefore uint64,
) {
	if _, ok := ls.config.UtxoSet.(LedgerChangesWriter); !ok {
		return
	}
	if len(nonces) == 0 && pruneBefore == 0 {
		return
	}
	stage := func(b *utxoSetBatch) {
		b.nonces = append(b.nonces, nonces...)
		if pruneBefore > b.pruneNoncesBefore {
			b.pruneNoncesBefore = pruneBefore
		}
	}
	if txn == nil {
		b := &utxoSetBatch{}
		stage(b)
		ls.applyUtxoSet(b)
		return
	}
	ls.utxoSetBatchForTxn(txn, stage)
}

// forgetUtxoSetBatch removes a finished transaction's batch from the maps.
func (ls *LedgerState) forgetUtxoSetBatch(batch *utxoSetBatch) {
	if batch.txn == nil {
		return
	}
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	if ls.utxoSetBatches[batch.txn] == batch {
		delete(ls.utxoSetBatches, batch.txn)
	}
	for meta, b := range ls.utxoSetByMeta {
		if b == batch {
			delete(ls.utxoSetByMeta, meta)
		}
	}
}

// utxoSetBatchesSeenBy returns the batches a reader sees over the set's
// applied state, oldest first. A pinned reader (pin non-nil) sees the batches
// that had committed at its pin and no others. Otherwise: own, the reader's
// open transaction's batch
// (nil for none), and every batch whose transaction has committed but has
// not reached the set yet. The set is applied after the commit, so a write
// starting in between would otherwise pair metadata that holds the commit
// with a set that does not. The caller holds utxoSetApplyMu for reading.
func (ls *LedgerState) utxoSetBatchesSeenBy(
	pin *utxoSetPin,
	own *utxoSetBatch,
) []*utxoSetBatch {
	return ls.utxoSetBatchesObserved(pin, own, func(txn ledgerdb.Txn) bool {
		return txn.IsCommitted()
	})
}

// The database snapshot anchor holds the commit-phase barrier. Waiting for a
// transaction mutex here would deadlock a writer waiting to enter that phase.
func (ls *LedgerState) utxoSetBatchesAtCommitAnchor() []*utxoSetBatch {
	return ls.utxoSetBatchesObserved(nil, nil, func(txn ledgerdb.Txn) bool {
		return dbadapter.Unwrap(txn).CommitPublished()
	})
}

func (ls *LedgerState) utxoSetBatchesObserved(
	pin *utxoSetPin,
	own *utxoSetBatch,
	committed func(ledgerdb.Txn) bool,
) []*utxoSetBatch {
	if pin != nil {
		return slices.Clone(pin.batches)
	}
	ls.utxoSetMu.Lock()
	others := make([]*utxoSetBatch, 0, len(ls.utxoSetBatches))
	for _, b := range ls.utxoSetBatches {
		if b != own {
			others = append(others, b)
		}
	}
	ls.utxoSetMu.Unlock()
	// Observe commit state without holding the batch map while inspecting
	// the transactions.
	seen := make([]*utxoSetBatch, 0, len(others)+1)
	for _, b := range others {
		if committed(b.txn) {
			seen = append(seen, b)
		}
	}
	if own != nil {
		seen = append(seen, own)
	}
	slices.SortFunc(seen, func(a, b *utxoSetBatch) int {
		return cmp.Compare(a.seq, b.seq)
	})
	return seen
}

// stagedStake sums the stake deltas of batches for the keys keep accepts.
func (ls *LedgerState) stagedStake(
	batches []*utxoSetBatch,
	keep func(key []byte) bool,
) map[string]int64 {
	staged := make(map[string]int64)
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	for _, b := range batches {
		for key, d := range b.stake {
			if keep([]byte(key)) {
				staged[key] += d
			}
		}
	}
	return staged
}

// applyUtxoSet applies one commit's batch to the live UTxO set. After a
// failure it applies nothing more, leaving the set at the last commit it
// took for startup to resume from.
func (ls *LedgerState) applyUtxoSet(batch *utxoSetBatch) {
	if rolledBack := ls.applyAndForgetUtxoSet(batch); rolledBack != nil {
		ls.publishUtxoEvent(ls.lifecycleContext(), *rolledBack) //nolint:contextcheck // the ledger lifecycle bounds an after-commit publish
	}
}

// applyAndForgetUtxoSet applies batch and drops it from the maps as one step
// for readers, which merge committed batches over the set.
func (ls *LedgerState) applyAndForgetUtxoSet(batch *utxoSetBatch) *UtxoEvent {
	ls.utxoSetApplyMu.Lock()
	defer ls.utxoSetApplyMu.Unlock()
	defer ls.forgetUtxoSetBatch(batch)
	return ls.applyUtxoSetLocked(batch)
}

// applyUtxoSetLocked is applyUtxoSet under utxoSetApplyMu. It returns the
// change a rollback made, for publishing once the lock is released.
func (ls *LedgerState) applyUtxoSetLocked(batch *utxoSetBatch) *UtxoEvent {
	w := ls.config.UtxoSet
	if w == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if batch.tip == nil && batch.rollbackTo == nil && !batch.hasWrites() {
		return nil
	}
	var rolledBack *UtxoEvent
	if batch.rollbackTo != nil {
		rb, ok := w.(UtxoSetRollbacker)
		if !ok {
			ls.failUtxoSet(
				batch.rollbackTo.Slot,
				"live UTxO set cannot roll back from its own history",
				errors.New("no rollback from the set's own history"),
			)
			return nil
		}
		evt, err := rb.RollbackUtxoSet(*batch.rollbackTo)
		if err != nil {
			ls.failUtxoSet(
				batch.rollbackTo.Slot,
				"live UTxO set rollback failed; it stops following the ledger until startup resumes it",
				err,
			)
			return nil
		}
		rolledBack = &evt
	}
	if batch.tip == nil && !batch.hasWrites() {
		return rolledBack
	}
	var err error
	if lw, ok := w.(LedgerChangesWriter); ok {
		err = lw.ApplyLedgerChanges(batch.tip, LedgerChanges{
			Utxo:                batch.changes,
			Nonces:              batch.nonces,
			PruneNoncesBefore:   batch.pruneNoncesBefore,
			Epochs:              batch.epochs,
			Snapshots:           batch.snapshots,
			Summaries:           batch.summaries,
			EraSwitch:           batch.eraSwitch,
			NetworkStates:       batch.networkStates,
			Donations:           batch.donations,
			PParams:             batch.pparams,
			Accounts:            batch.accounts,
			RewardRecords:       batch.rewardRecords,
			DReps:               batch.dreps,
			DRepLinks:           batch.drepLinks,
			Dormancy:            batch.dormancy,
			DRepHistory:         batch.drepHistory,
			Pools:               batch.pools,
			PoolEvents:          batch.poolEvents,
			StakeEvents:         batch.stakeEvents,
			DelegationEvents:    batch.delegations,
			DRepDeposits:        batch.drepDeposits,
			GenesisDelegations:  batch.genesisDelegs,
			MIRCerts:            batch.mirCerts,
			RewardOutputs:       batch.rewardOutputs,
			RewardRounds:        batch.rewardRounds,
			PostSnapshotCredits: batch.postSnapshot,
			OpCerts:             batch.opCerts,
			RewardBlocks:        batch.rewardBlocks,
			RewardInputs:        batch.rewardInputs,
			RewardDerived:       batch.rewardDerived,
			Committee:           batch.committee,
			Proposals:           batch.proposals,
			Votes:               batch.votes,
			PParamUpdates:       batch.pparamUpdates,
			SyncMarkers:         batch.syncMarkers,
			ImportBaselines:     batch.importBaselines,
			Witnesses:           batch.witnesses,
		})
	} else {
		err = w.ApplyUtxoChanges(batch.tip, batch.changes)
	}
	if err != nil {
		slot := uint64(0)
		if batch.tip != nil {
			slot = batch.tip.Slot
		}
		ls.failUtxoSet(
			slot,
			"live UTxO set update failed; it stops following the ledger until startup resumes it",
			err,
		)
	}
	return rolledBack
}

type resumeCheckKey struct{}

// WithResumeCheck returns a context under which ResumeUtxoSetFrom only
// reports whether it would roll back to the point, changing nothing. The
// rollback reads the live set, so a caller that has to keep its current set
// until the ledger agrees asks this way first.
func WithResumeCheck(ctx context.Context) context.Context {
	return context.WithValue(ctx, resumeCheckKey{}, true)
}

// ResumeCheckOnly reports whether ctx came from WithResumeCheck.
func ResumeCheckOnly(ctx context.Context) bool {
	v, _ := ctx.Value(resumeCheckKey{}).(bool)
	return v
}

// ResumeUtxoSetFrom rolls the ledger back to point, the tip of a live UTxO
// set that trails the ledger, so every block after it is applied again and
// reaches the set. The set equals the ledger's live set as of point, and the
// rollback's own change, which turns the ledger's set at its tip back into
// the set at point, leaves a set already at point unchanged.
//
// It reports false, changing nothing, when point is not an applied block on
// the primary chain below the ledger tip, or when the rollback would cross the
// Mithril trust boundary or the consumed-UTxO prune floor; the caller then
// rebuilds the set instead. Call it before the ledger starts.
func (ls *LedgerState) ResumeUtxoSetFrom(
	ctx context.Context,
	point ocommon.Point,
) (bool, error) {
	if len(point.Hash) == 0 {
		return false, nil
	}
	tip, err := ls.ledgerDB().GetTip(nil)
	if err != nil {
		return false, fmt.Errorf("read ledger tip: %w", err)
	}
	if point.Slot >= tip.Point.Slot {
		return false, nil
	}
	onChain, err := ls.primaryChainContainsPoint(ctx, point)
	if err != nil {
		return false, fmt.Errorf(
			"check live UTxO set tip on the chain: %w",
			err,
		)
	}
	if !onChain {
		return false, nil
	}
	ls.RLock()
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	if mithrilLedgerSlot > 0 && point.Slot < mithrilLedgerSlot {
		return false, nil
	}
	below, _, err := ls.rollbackBelowConsumedUtxoPruneFloor(point)
	if err != nil {
		return false, fmt.Errorf("determine consumed UTxO prune floor: %w", err)
	}
	if below {
		return false, nil
	}
	if ResumeCheckOnly(ctx) {
		return true, nil
	}
	ls.resumingUtxoSet.Store(true)
	defer ls.resumingUtxoSet.Store(false)
	if err := ls.rollback(ctx, point); err != nil {
		// A committed rollback has moved the ledger and the set together.
		if _, ok := errors.AsType[*rollbackCommittedError](err); ok {
			return true, err
		}
		return false, err
	}
	return true, nil
}

// UtxoSetSnapshotter is a live UTxO set that can fix a read-only view of its
// applied state. The view answers every read interface the set does, as of
// the call, and fails writes. release ends it.
type UtxoSetSnapshotter interface {
	SnapshotUtxoSet() (view UtxoSetWriter, release func(), err error)
}

// UtxoSetReader reads the live UTxO set. A UtxoSetWriter that also
// implements it can serve validation's reads.
type UtxoSetReader interface {
	// Get returns the CBOR of the live output txID#index.
	Get(txID []byte, index uint32) ([]byte, bool, error)
	// Sum returns the committed stake sum for a key built by
	// CredentialStakeKey or PointerStakeKey, as UtxoStakeOf classifies
	// live outputs.
	Sum(key []byte) (uint64, error)
}

// utxoSetOverlayEntry is the last staged state of one output in an open
// transaction's batch.
type utxoSetOverlayEntry struct {
	cbor []byte
	live bool
}

// stageUtxoOverlay records changes in the batch's overlay and stake deltas,
// which reads in the same transaction see before the set does. Each change's
// stake effect is taken against the output's value as txn sees it, so an
// output created and spent in one transaction nets to nothing.
func (ls *LedgerState) stageUtxoOverlay(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	b *utxoSetBatch,
	changes []UtxoEvent,
) error {
	unstake := func(hash []byte, idx uint32) error {
		cbor, live, err := ls.utxoSetGet(r, txn, hash, idx)
		if err != nil || !live {
			return err
		}
		ls.utxoSetMu.Lock()
		b.addStake(cbor, -1)
		ls.utxoSetMu.Unlock()
		return nil
	}
	for _, c := range changes {
		for _, ref := range c.Removed {
			if err := unstake(ref.Hash, ref.Idx); err != nil {
				return err
			}
			ls.utxoSetMu.Lock()
			b.setOverlay(utxoSetKey(ref.Hash, ref.Idx), utxoSetOverlayEntry{})
			ls.utxoSetMu.Unlock()
		}
		for _, o := range c.Added {
			if err := unstake(o.Ref.Hash, o.Ref.Idx); err != nil {
				return err
			}
			ls.utxoSetMu.Lock()
			b.addStake(o.Cbor, 1)
			b.setOverlay(
				utxoSetKey(o.Ref.Hash, o.Ref.Idx),
				utxoSetOverlayEntry{cbor: o.Cbor, live: true},
			)
			ls.utxoSetMu.Unlock()
		}
	}
	return nil
}

func (b *utxoSetBatch) setOverlay(k utxoref.Key, e utxoSetOverlayEntry) {
	if b.overlay == nil {
		b.overlay = make(map[utxoref.Key]utxoSetOverlayEntry)
	}
	b.overlay[k] = e
}

// addStake adds sign times an output's lovelace to its stake sum's delta.
// The caller holds utxoSetMu.
func (b *utxoSetBatch) addStake(cbor []byte, sign int64) {
	key, lovelace, ok := UtxoStakeOf(cbor)
	if !ok {
		return
	}
	if b.stake == nil {
		b.stake = make(map[string]int64)
	}
	b.stake[string(key)] += sign * int64(lovelace) //nolint:gosec // lovelace fits in int64
}

// CredentialUtxoStake returns the lovelace in live outputs naming the stake
// credential (tag, hash), as txn sees the live UTxO set. It needs a live UTxO
// set that is a UtxoSetReader.
func (ls *LedgerState) CredentialUtxoStake(
	txn ledgerdb.Txn,
	tag uint8,
	hash []byte,
) (uint64, error) {
	r, ok := ls.readSet(txn).(UtxoSetReader)
	if !ok {
		return 0, ErrUtxoSetUnserved
	}
	return ls.utxoStakeSeenBy(r, txn, CredentialStakeKey(tag, hash))
}

// utxoStakeSeenBy returns a stake sum (see CredentialStakeKey) as txn sees
// the live UTxO set: the set's applied sum plus the changes of txn and of
// committed transactions not yet applied (see utxoSetBatchesSeenBy).
func (ls *LedgerState) utxoStakeSeenBy(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	key []byte,
) (uint64, error) {
	ls.utxoSetMu.Lock()
	var b *utxoSetBatch
	if txn != nil {
		b = ls.utxoSetBatches[txn]
	}
	ls.utxoSetMu.Unlock()
	return ls.stakeWithBatch(r, ls.pinOf(txn), b, key)
}

// stakeWithBatch is a stake sum from the set's applied state plus the staged
// change of b, which may be nil, and of every committed batch not yet
// applied.
func (ls *LedgerState) stakeWithBatch(
	r UtxoSetReader,
	pin *utxoSetPin,
	b *utxoSetBatch,
	key []byte,
) (uint64, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	committed, err := r.Sum(key)
	if err != nil {
		return 0, err
	}
	seen := ls.utxoSetBatchesSeenBy(pin, b)
	delta := ls.stagedStake(seen, func(k []byte) bool {
		return bytes.Equal(k, key)
	})[string(key)]
	total := int64(committed) + delta //nolint:gosec // lovelace fits in int64
	if total < 0 {
		return 0, fmt.Errorf("stake sum %x is negative as staged", key)
	}
	return uint64(total), nil
}

// utxoSetReader returns the live UTxO set's reader when one is configured.
func (ls *LedgerState) utxoSetReader() (UtxoSetReader, bool) {
	return ls.utxoSetReaderOf(ls.config.UtxoSet)
}

// utxoSetReaderFor is utxoSetReader for the set txn reads, which is its
// pinned snapshot for a pinned transaction.
func (ls *LedgerState) utxoSetReaderFor(
	txn ledgerdb.Txn,
) (UtxoSetReader, bool) {
	return ls.utxoSetReaderOf(ls.readSet(txn))
}

func (ls *LedgerState) utxoSetReaderOf(w UtxoSetWriter) (UtxoSetReader, bool) {
	if w == nil {
		return nil, false
	}
	r, ok := w.(UtxoSetReader)
	if !ok {
		return nil, false
	}
	return r, true
}

// utxoSetGet reads txID#index as txn sees the live UTxO set: the changes txn
// and committed transactions not yet applied have staged over the set.
func (ls *LedgerState) utxoSetGet(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	// r is nil only when reads are off, which callers handle before this.
	if r == nil || ls.utxoSetFailed.Load() {
		return nil, false, errLiveUtxoSetUnavailable
	}
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	return ls.getThroughBatches(r, ls.pinOf(txn), own, txID, index)
}

// getThroughBatches reads txID#index from the newest batch reader own sees
// that changed it, else from the set's applied state.
func (ls *LedgerState) getThroughBatches(
	r UtxoSetReader,
	pin *utxoSetPin,
	own *utxoSetBatch,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	batches := ls.utxoSetBatchesSeenBy(pin, own)
	k := utxoSetKey(txID, index)
	ls.utxoSetMu.Lock()
	for _, b := range slices.Backward(batches) {
		if entry, hit := b.overlay[k]; hit {
			ls.utxoSetMu.Unlock()
			return entry.cbor, entry.live, nil
		}
	}
	ls.utxoSetMu.Unlock()
	return r.Get(txID, index)
}

// utxoByRefForValidation resolves txID#index for validation from the live UTxO set.
func (ls *LedgerState) utxoByRefForValidation(
	txID []byte,
	index uint32,
	txn ledgerdb.Txn,
) (*models.Utxo, error) {
	r, mode := ls.utxoSetReaderFor(txn)
	if !mode {
		return nil, ErrUtxoSetUnserved
	}
	setCbor, setFound, setErr := ls.utxoSetGet(r, txn, txID, index)
	if setErr != nil {
		return nil, setErr
	}
	if !setFound {
		return nil, database.ErrUtxoNotFound
	}
	return &models.Utxo{TxId: txID, OutputIdx: index, Cbor: setCbor}, nil
}

// utxosByRefsForQuery resolves client query references from the live UTxO set.
func (ls *LedgerState) utxosByRefsForQuery(
	refs []models.UtxoId,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return ls.utxosByRefsForValidation(refs, txn)
}

// utxosByRefsForValidation resolves refs for validation from the live UTxO set,
// leaving out refs that are not live.
func (ls *LedgerState) utxosByRefsForValidation(
	refs []models.UtxoId,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	if len(refs) == 0 {
		return []models.Utxo{}, nil
	}
	r, mode := ls.utxoSetReaderFor(txn)
	if !mode {
		return nil, ErrUtxoSetUnserved
	}
	fromSet := make([]models.Utxo, 0, len(refs))
	for _, ref := range refs {
		c, ok, err := ls.utxoSetGet(r, txn, ref.Hash, ref.Idx)
		if err != nil {
			return nil, err
		}
		if ok {
			fromSet = append(fromSet, models.Utxo{
				TxId:      ref.Hash,
				OutputIdx: ref.Idx,
				Cbor:      c,
			})
		}
	}
	return fromSet, nil
}

func utxoSetKey(txID []byte, index uint32) utxoref.Key {
	return utxoref.Key{TxId: lcommon.NewBlake2b256(txID), Index: index}
}

// LiveUtxoCbor implements types.LiveUtxoSource for the metadata store: it
// reads txID#index as the ledger transaction writing in meta sees the live
// UTxO set.
func (ls *LedgerState) LiveUtxoCbor(
	meta types.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	r, ok := ls.readSetMeta(meta).(UtxoSetReader)
	if !ok {
		return nil, false, ErrUtxoSetUnserved
	}
	if ls.utxoSetFailed.Load() {
		return nil, false, errLiveUtxoSetUnavailable
	}
	return ls.getThroughBatches(
		r, ls.pinOfMeta(meta), ls.utxoSetBatchFor(meta), txID, index,
	)
}

// utxoSetBatchFor returns the batch of the write transaction metadata
// transaction meta belongs to, nil for none.
func (ls *LedgerState) utxoSetBatchFor(meta types.Txn) *utxoSetBatch {
	if meta == nil {
		return nil
	}
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	return ls.utxoSetByMeta[meta]
}

// stageUtxoSetRollback makes txn's batch roll the live UTxO set back to point
// from its journal once txn commits, and record point as its tip. A preview
// the journal cannot serve stops the set following the ledger.
func (ls *LedgerState) stageUtxoSetRollback(
	txn ledgerdb.Txn,
	point ocommon.Point,
) {
	if txn == nil {
		ls.applyUtxoSet(&utxoSetBatch{rollbackTo: &point, tip: &point})
		return
	}
	if err := ls.stageUtxoSetRollbackChecked(txn, point); err != nil {
		ls.failUtxoSet(
			point.Slot,
			"live UTxO set rollback preview failed; it stops following the ledger until startup resumes it",
			err,
		)
	}
}

// stageUtxoSetRollbackChecked is stageUtxoSetRollback for a write
// transaction, returning the error of a preview the journal cannot serve
// instead of failing the set. A caller that returns the error must roll txn
// back so the staged batch is dropped.
func (ls *LedgerState) stageUtxoSetRollbackChecked(
	txn ledgerdb.Txn,
	point ocommon.Point,
) error {
	// The batch's tip is applied after the rollback, so its journal record
	// lands after the records the rollback undoes.
	ls.stageUtxoSet(txn, &point)
	ls.utxoSetMu.Lock()
	b := ls.utxoSetBatches[txn]
	if b != nil {
		p := point
		b.rollbackTo = &p
	}
	ls.utxoSetMu.Unlock()
	// The metadata truncation that follows re-reads stake, so the
	// rolled-back set is staged in the overlay, though only the journal
	// rollback after the commit changes the set.
	rb, ok := ls.config.UtxoSet.(UtxoSetRollbacker)
	r, readable := ls.config.UtxoSet.(UtxoSetReader)
	if b == nil || !ok || !readable {
		return nil
	}
	preview, err := rb.PreviewUtxoSetRollback(point)
	if err == nil {
		var refs []models.StakeCredentialRef
		refs, err = ls.rollbackStakeRefs(r, txn, preview)
		ls.utxoSetMu.Lock()
		b.rollbackRefs = refs
		ls.utxoSetMu.Unlock()
	}
	if err == nil {
		err = ls.stageUtxoOverlay(r, txn, b, []UtxoEvent{preview})
	}
	return err
}

// LiveUtxoStake implements types.LiveUtxoSource: the lovelace in live
// outputs naming the stake credential (tag, hash), as the ledger transaction
// writing in meta sees the live UTxO set.
func (ls *LedgerState) LiveUtxoStake(
	meta types.Txn,
	tag uint8,
	hash []byte,
) (uint64, error) {
	r, ok := ls.readSetMeta(meta).(UtxoSetReader)
	if !ok {
		return 0, ErrUtxoSetUnserved
	}
	if ls.utxoSetFailed.Load() {
		return 0, errLiveUtxoSetUnavailable
	}
	return ls.stakeWithBatch(
		r,
		ls.pinOfMeta(meta),
		ls.utxoSetBatchFor(meta),
		CredentialStakeKey(tag, hash),
	)
}

// EachLiveUtxoStake implements types.LiveUtxoSource: every stake
// credential's lovelace in live outputs as the ledger transaction writing in
// meta sees the live UTxO set.
func (ls *LedgerState) EachLiveUtxoStake(
	meta types.Txn,
	fn func(tag uint8, hash []byte, lovelace uint64) error,
) error {
	sums, ok := ls.readSetMeta(meta).(UtxoSetSums)
	if !ok {
		return fmt.Errorf("%w: the live UTxO set keeps no stake sums", ErrUtxoSetUnserved)
	}
	if ls.utxoSetFailed.Load() {
		return errLiveUtxoSetUnavailable
	}
	isCredential := func(key []byte) bool {
		_, _, ok := ParseCredentialStakeKey(key)
		return ok
	}
	type stakeSum struct {
		key []byte
		n   int64
	}
	var totals []stakeSum
	// The sums are collected under the lock and fn runs after it, since fn
	// may read the set again.
	err := func() error {
		ls.utxoSetApplyMu.RLock()
		defer ls.utxoSetApplyMu.RUnlock()
		staged := ls.stagedStake(
			ls.utxoSetBatchesSeenBy(
				ls.pinOfMeta(meta), ls.utxoSetBatchFor(meta),
			),
			isCredential,
		)
		if err := sums.EachSum(func(key []byte, n uint64) error {
			if !isCredential(key) {
				return nil
			}
			d := staged[string(key)]
			delete(staged, string(key))
			totals = append(totals, stakeSum{
				key: bytes.Clone(key),
				n:   int64(n) + d, //nolint:gosec // lovelace fits in int64
			})
			return nil
		}); err != nil {
			return err
		}
		for key, d := range staged {
			totals = append(totals, stakeSum{key: []byte(key), n: d})
		}
		return nil
	}()
	if err != nil {
		return err
	}
	for _, t := range totals {
		if t.n < 0 {
			return fmt.Errorf("stake sum %x is negative as staged", t.key)
		}
		if t.n == 0 {
			continue
		}
		tag, hash, _ := ParseCredentialStakeKey(t.key)
		if err := fn(tag, hash, uint64(t.n)); err != nil {
			return err
		}
	}
	return nil
}

// rollbackStakeRefs returns the stake credentials of the outputs a rollback
// change adds and removes, reading removed outputs before the change.
func (ls *LedgerState) rollbackStakeRefs(
	r UtxoSetReader,
	txn ledgerdb.Txn,
	change UtxoEvent,
) ([]models.StakeCredentialRef, error) {
	var refs []models.StakeCredentialRef
	add := func(cbor []byte) {
		key, _, ok := UtxoStakeOf(cbor)
		if !ok {
			return
		}
		if tag, hash, ok := ParseCredentialStakeKey(key); ok {
			refs = append(refs, models.NewStakeCredentialRef(tag, hash))
		}
	}
	for _, o := range change.Added {
		add(o.Cbor)
	}
	for _, ref := range change.Removed {
		cbor, live, err := ls.utxoSetGet(r, txn, ref.Hash, ref.Idx)
		if err != nil {
			return nil, err
		}
		if live {
			add(cbor)
		}
	}
	return refs, nil
}

// refreshUtxoSetRollbackStake refreshes, after the metadata truncation in
// txn, the live reward aggregate of the credentials txn's rollback changes,
// which the metadata store finds through utxo rows when it has them.
func (ls *LedgerState) refreshUtxoSetRollbackStake(
	txn ledgerdb.Txn,
	slot uint64,
) error {
	if txn == nil {
		return nil
	}
	if ls.resumingUtxoSet.Load() {
		// The set never applied the blocks this rollback removes from the
		// ledger, so its journal names none of the credentials whose
		// aggregate those blocks changed: recompute every one from the set.
		return ls.ledgerDB().RebuildRewardLiveStake(slot, txn)
	}
	ls.utxoSetMu.Lock()
	var refs []models.StakeCredentialRef
	if b := ls.utxoSetBatches[txn]; b != nil {
		refs = b.rollbackRefs
	}
	ls.utxoSetMu.Unlock()
	if len(refs) == 0 {
		return nil
	}
	return ls.ledgerDB().RefreshRewardLiveStakeRefs(refs, slot, txn)
}

// UtxoSetIterator walks a live UTxO set's committed outputs.
type UtxoSetIterator interface {
	EachUtxo(fn func(txID []byte, index uint32, cbor []byte) error) error
}

// iterateLiveUtxos calls fn for every live output as txn sees the live UTxO
// set, with txn's staged changes over it. Rows passed to fn carry only the
// reference and the output CBOR.
func (ls *LedgerState) iterateLiveUtxos(
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	it, ok := ls.readSet(txn).(UtxoSetIterator)
	if !ok {
		return fmt.Errorf("%w: the live UTxO set cannot be iterated", ErrUtxoSetUnserved)
	}
	if ls.utxoSetFailed.Load() {
		return errLiveUtxoSetUnavailable
	}
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	// Overlay entries replace the set's by key, so a batch applied while the
	// set is walked is still counted once; the lock is only needed to pick
	// the batches.
	ls.utxoSetApplyMu.RLock()
	batches := ls.utxoSetBatchesSeenBy(ls.pinOf(txn), own)
	overlay := make(map[utxoref.Key]utxoSetOverlayEntry)
	ls.utxoSetMu.Lock()
	for _, b := range batches {
		maps.Copy(overlay, b.overlay)
	}
	ls.utxoSetMu.Unlock()
	ls.utxoSetApplyMu.RUnlock()
	if err := it.EachUtxo(func(txID []byte, index uint32, cbor []byte) error {
		if _, staged := overlay[utxoSetKey(txID, index)]; staged {
			return nil
		}
		return fn(&models.Utxo{TxId: txID, OutputIdx: index, Cbor: cbor})
	}); err != nil {
		return err
	}
	for k, e := range overlay {
		if !e.live {
			continue
		}
		if err := fn(&models.Utxo{
			TxId:      k.TxId.Bytes(),
			OutputIdx: k.Index,
			Cbor:      e.cbor,
		}); err != nil {
			return err
		}
	}
	return nil
}

// liveUtxoLovelace is the lovelace in every live output as txn sees the UTxO
// set.
func (ls *LedgerState) liveUtxoLovelace(
	txn ledgerdb.Txn,
) (uint64, error) {
	var total uint64
	err := ls.iterateLiveUtxos(txn, func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf("decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err)
		}
		amount, err := models.CheckedUint64FromBigInt(out.Amount())
		if err != nil {
			return err
		}
		if total > ^uint64(0)-amount {
			return errors.New("live UTxO lovelace overflows uint64")
		}
		total += amount
		return nil
	})
	return total, err
}

// utxoIncludingSpent resolves txID#index whether or not it is still live,
// returning nil for an output that never existed. A spent output is read from the body of the transaction that produced it: a valid
// transaction's outputs, or an invalid one's collateral return at the index
// after its outputs, as the metadata store records them.
func (ls *LedgerState) utxoIncludingSpent(
	ctx context.Context,
	txn ledgerdb.Txn,
	txID []byte,
	index uint32,
) (*models.Utxo, error) {
	live, err := ls.utxoByRefForValidation(txID, index, txn)
	if err == nil {
		return live, nil
	}
	if !errors.Is(err, database.ErrUtxoNotFound) {
		return nil, err
	}
	rows, err := ls.ledgerDB().GetTransactionsByHashes(ctx, [][]byte{txID}, txn)
	if err != nil {
		return nil, fmt.Errorf("find producer of %x#%d: %w", txID, index, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	bodyCbor, err := ls.ledgerDB().ResolveTxCbor(txn, txID)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve producer of %x#%d: %w",
			txID,
			index,
			err,
		)
	}
	body, err := gledger.NewTransactionBodyFromCbor(
		uint(rows[0].Type), //nolint:gosec // era ID fits in uint
		bodyCbor,
	)
	if err != nil {
		return nil, fmt.Errorf("decode producer of %x#%d: %w", txID, index, err)
	}
	outputs := body.Outputs()
	var out lcommon.TransactionOutput
	switch {
	case rows[0].Valid && int(index) < len(outputs):
		out = outputs[index]
	case !rows[0].Valid && int(index) == len(outputs):
		out = body.CollateralReturn()
	}
	if out == nil {
		return nil, nil
	}
	c, err := outputCbor(out)
	if err != nil {
		return nil, fmt.Errorf("encode output %x#%d: %w", txID, index, err)
	}
	return &models.Utxo{
		TxId:      txID,
		OutputIdx: index,
		Cbor:      c,
	}, nil
}

// utxosByAddress returns the live outputs paid to any of addrs, at most limit
// of them when limit is positive. It scans the live UTxO set and matches each
// output's exact address.
func (ls *LedgerState) utxosByAddress(
	addrs []gledger.Address,
	limit int,
	txn ledgerdb.Txn,
) ([]models.Utxo, error) {
	return utxosByAddressFrom(
		addrs,
		limit,
		func(fn func(*models.Utxo) error) error {
			return ls.iterateLiveUtxos(txn, fn)
		},
	)
}

// utxosByAddressFrom returns up to limit outputs each yields that pay to one
// of addrs.
func utxosByAddressFrom(
	addrs []gledger.Address,
	limit int,
	each func(func(*models.Utxo) error) error,
) ([]models.Utxo, error) {
	want := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		raw, err := a.Bytes()
		if err != nil {
			return nil, err
		}
		want[string(raw)] = struct{}{}
	}
	var ret []models.Utxo
	errLimit := errors.New("limit reached")
	err := each(func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf("decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err)
		}
		raw, err := out.Address().Bytes()
		if err != nil {
			return err
		}
		if _, ok := want[string(raw)]; !ok {
			return nil
		}
		ret = append(ret, *u)
		if limit > 0 && len(ret) >= limit {
			return errLimit
		}
		return nil
	})
	if errors.Is(err, errLimit) {
		err = nil
	}
	return ret, err
}

// UtxoSetSums walks a live UTxO set's committed stake sums.
type UtxoSetSums interface {
	EachSum(fn func(key []byte, lovelace uint64) error) error
}

// LivePointerStakes implements types.LiveUtxoSource: the lovelace at each
// pointer position as the ledger transaction writing in meta sees the live
// UTxO set.
func (ls *LedgerState) LivePointerStakes(
	meta types.Txn,
) ([]types.PointerStake, error) {
	sums, ok := ls.readSetMeta(meta).(UtxoSetSums)
	if !ok {
		return nil, fmt.Errorf("%w: the live UTxO set keeps no stake sums", ErrUtxoSetUnserved)
	}
	if ls.utxoSetFailed.Load() {
		return nil, errLiveUtxoSetUnavailable
	}
	isPointer := func(key []byte) bool {
		_, _, _, ok := ParsePointerStakeKey(key)
		return ok
	}
	ls.utxoSetApplyMu.RLock()
	totals := ls.stagedStake(
		ls.utxoSetBatchesSeenBy(ls.pinOfMeta(meta), ls.utxoSetBatchFor(meta)),
		isPointer,
	)
	err := sums.EachSum(func(key []byte, n uint64) error {
		if isPointer(key) {
			totals[string(key)] += int64(n) //nolint:gosec // lovelace fits in int64
		}
		return nil
	})
	ls.utxoSetApplyMu.RUnlock()
	if err != nil {
		return nil, err
	}
	ret := make([]types.PointerStake, 0, len(totals))
	for key, n := range totals {
		if n < 0 {
			return nil, fmt.Errorf(
				"pointer stake %x is negative as staged",
				key,
			)
		}
		if n == 0 {
			continue
		}
		slot, txIndex, certIndex, _ := ParsePointerStakeKey([]byte(key))
		ret = append(ret, types.PointerStake{
			Slot: slot, TxIndex: txIndex, CertIndex: certIndex,
			Lovelace: uint64(n),
		})
	}
	return ret, nil
}

// ErrUtxoAsOfUnavailable reports a UTxO query pinned to a point the live
// UTxO set's history no longer reaches, when the set is the only copy.
var ErrUtxoAsOfUnavailable = errors.New(
	"UTxO queries before the live UTxO set's history are not available when the set is the only copy",
)

// UtxoSetHistory opens views of a live UTxO set as it was at an earlier
// point. UtxoSetAt returns an error
// wrapping ErrUtxoAsOfUnavailable when its history does not reach point, and
// ErrPointNotOnChain when point is not in its retained full-point lineage.
type UtxoSetHistory interface {
	UtxoSetAt(point ocommon.Point) (UtxoSetView, error)
}

// UtxoSetView is a read-only view of a live UTxO set at one point, read from
// one snapshot. Close releases it.
type UtxoSetView interface {
	Get(txID []byte, index uint32) ([]byte, bool, error)
	EachUtxo(fn func(txID []byte, index uint32, cbor []byte) error) error
	Close()
}

// liveUtxoView opens the transaction's live set snapshot as of at. An unpinned
// query returns nil and reads the transaction's current set instead.
func (ls *LedgerState) liveUtxoView(at QueryPoint, txn ledgerdb.Txn) (UtxoSetView, error) {
	if !at.pinned() {
		return nil, nil
	}
	if ls.utxoSetFailed.Load() {
		return nil, errLiveUtxoSetUnavailable
	}
	h, ok := ls.readSet(txn).(UtxoSetHistory)
	if !ok {
		return nil, ErrUtxoAsOfUnavailable
	}
	return h.UtxoSetAt(ocommon.Point{Slot: at.Slot, Hash: at.Hash})
}

// eachInView adapts v to the iterator utxosByAddressFrom takes.
func eachInView(v UtxoSetView) func(func(*models.Utxo) error) error {
	return func(fn func(*models.Utxo) error) error {
		return v.EachUtxo(func(txID []byte, index uint32, cbor []byte) error {
			return fn(&models.Utxo{TxId: txID, OutputIdx: index, Cbor: cbor})
		})
	}
}

// utxoExists reports whether txID#index is live, without reading its output.
func (ls *LedgerState) utxoExists(
	txID []byte,
	index uint32,
) (bool, error) {
	r, ok := ls.utxoSetReader()
	if !ok {
		return false, ErrUtxoSetUnserved
	}
	_, live, err := ls.utxoSetGet(r, nil, txID, index)
	return live, err
}

// ChainContainsPoint reports whether point is a block on the primary chain, as
// opposed to a fork the blob store retains. The point at origin is on every
// chain.
func (ls *LedgerState) ChainContainsPoint(
	ctx context.Context,
	point ocommon.Point,
) (bool, error) {
	return ls.primaryChainContainsPoint(ctx, point)
}
