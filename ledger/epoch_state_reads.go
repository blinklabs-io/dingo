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
	"errors"
	"maps"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// EpochStateReader serves epoch and pool stake snapshot rows from a live
// UTxO set that keeps epoch state.
type EpochStateReader interface {
	// EpochStateLoaded reports whether the reader holds every epoch and
	// snapshot row; until it does, the set does not serve the reads.
	EpochStateLoaded() (bool, error)
	Epochs() ([]models.Epoch, error)
	// EraSwitch returns the recorded mid-epoch era switch, or nil.
	EraSwitch() (*database.MidEpochEraSwitch, error)
	// PoolStakeSnapshots returns epoch's snapshot rows of typ in pool
	// order. A positive limit stops the read after that many rows.
	PoolStakeSnapshots(
		epoch uint64,
		typ string,
		limit int,
	) ([]*models.PoolStakeSnapshot, error)
	// PoolStakeSnapshotsCapturedIn returns the snapshot rows of every epoch
	// and type captured in [from, through], from one read.
	PoolStakeSnapshotsCapturedIn(
		from, through uint64,
	) ([]*models.PoolStakeSnapshot, error)
}

// epochStateOps is what one staged batch does to the epoch state, in the
// order the set applies it.
type epochStateOps struct {
	rollbackTo *uint64
	epochs     []models.Epoch
	snapshots  []SnapshotChange
	summaries  []models.EpochSummary
	eraSwitch  *database.MidEpochEraSwitch
	// networkStates and donations are written after the rollback.
	networkStates []models.NetworkState
	donations     []models.NetworkDonation
	pparams       []models.PParams
	accounts      []AccountChange
	rewardRecords []RewardRecord
	dreps         []DRepChange
	drepLinks     []DRepLinkChange
	dormancy      []DormancyChange
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
	stateMarks    []StateMarkChange

	importBaselines []ImportBaselineChange
	witnesses       []WitnessChange
}

// epochStateOpsOf copies batch's epoch state changes. The caller holds
// utxoSetMu.
func epochStateOpsOf(batch *utxoSetBatch) epochStateOps {
	n := nonceOpsOf(batch)
	return epochStateOps{
		rollbackTo: n.rollbackTo,
		epochs:     slices.Clone(batch.epochs),
		snapshots:  slices.Clone(batch.snapshots),
		summaries:  slices.Clone(batch.summaries),
		eraSwitch:  batch.eraSwitch,

		networkStates: slices.Clone(batch.networkStates),
		donations:     slices.Clone(batch.donations),
		pparams:       slices.Clone(batch.pparams),
		accounts:      slices.Clone(batch.accounts),
		rewardRecords: slices.Clone(batch.rewardRecords),
		dreps:         slices.Clone(batch.dreps),
		drepLinks:     slices.Clone(batch.drepLinks),
		dormancy:      slices.Clone(batch.dormancy),
		pools:         slices.Clone(batch.pools),
		poolEvents:    slices.Clone(batch.poolEvents),
		stakeEvents:   slices.Clone(batch.stakeEvents),
		delegations:   slices.Clone(batch.delegations),
		drepDeposits:  slices.Clone(batch.drepDeposits),
		genesisDelegs: slices.Clone(batch.genesisDelegs),
		mirCerts:      slices.Clone(batch.mirCerts),
		rewardOutputs: slices.Clone(batch.rewardOutputs),
		rewardRounds:  slices.Clone(batch.rewardRounds),
		postSnapshot:  slices.Clone(batch.postSnapshot),
		opCerts:       slices.Clone(batch.opCerts),
		rewardBlocks:  slices.Clone(batch.rewardBlocks),
		rewardInputs:  slices.Clone(batch.rewardInputs),
		rewardDerived: slices.Clone(batch.rewardDerived),
		committee:     slices.Clone(batch.committee),
		proposals:     slices.Clone(batch.proposals),
		votes:         slices.Clone(batch.votes),
		pparamUpdates: slices.Clone(batch.pparamUpdates),
		syncMarkers:   slices.Clone(batch.syncMarkers),
		stateMarks:    slices.Clone(batch.stateMarks),

		importBaselines: slices.Clone(batch.importBaselines),
		witnesses:       slices.Clone(batch.witnesses),
	}
}

// stagedEpochStateOps returns the epoch state changes of the batches txn
// sees, in staging order. The caller holds utxoSetApplyMu for reading.
func (ls *LedgerState) stagedEpochStateOps(txn ledgerdb.Txn) []epochStateOps {
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	batches := ls.utxoSetBatchesSeenBy(ls.pinOf(txn), own)
	ls.utxoSetMu.Lock()
	defer ls.utxoSetMu.Unlock()
	ops := make([]epochStateOps, 0, len(batches))
	for _, b := range batches {
		ops = append(ops, epochStateOpsOf(b))
	}
	return ops
}

// epochStateView is the set's epoch rows with staged batches applied.
func (ls *LedgerState) epochStateView(
	r EpochStateReader,
	txn ledgerdb.Txn,
) (map[uint64]models.Epoch, error) {
	view, _, err := ls.epochStateViewWithSwitch(r, txn)
	return view, err
}

// epochStateViewWithSwitch is epochStateView and the era switch record as txn
// sees it: the set's, replaced by a staged switch and dropped by a staged
// rollback past its block.
func (ls *LedgerState) epochStateViewWithSwitch(
	r EpochStateReader,
	txn ledgerdb.Txn,
) (map[uint64]models.Epoch, *database.MidEpochEraSwitch, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	rows, err := r.Epochs()
	if err != nil {
		return nil, nil, err
	}
	sw, err := r.EraSwitch()
	if err != nil {
		return nil, nil, err
	}
	view := make(map[uint64]models.Epoch, len(rows))
	for _, e := range rows {
		view[e.EpochId] = e
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			for id, e := range view {
				if e.StartSlot > *o.rollbackTo {
					delete(view, id)
				}
			}
			// As the store does, a rollback past the switch's block gives
			// the epoch it switched its source era back.
			if sw != nil && sw.Slot > *o.rollbackTo {
				if e, ok := view[sw.Epoch]; ok && e.EraId == sw.ToEra {
					e.EraId = sw.FromEra
					view[sw.Epoch] = e
				}
				sw = nil
			}
		}
		for _, e := range o.epochs {
			view[e.EpochId] = e
		}
		if o.eraSwitch != nil {
			sw = o.eraSwitch
		}
	}
	return view, sw, nil
}

func sortedEpochs(view map[uint64]models.Epoch, keep func(models.Epoch) bool) []models.Epoch {
	out := make([]models.Epoch, 0, len(view))
	for _, e := range view {
		if keep == nil || keep(e) {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b models.Epoch) int {
		return cmp.Compare(a.EpochId, b.EpochId)
	})
	return out
}

// rollbackEpochStarts drops the epochs that start after slot from starts and
// returns the newest epoch left. Snapshots and summaries of later epochs go
// with the rollback, and are kept when no epoch row is left.
func rollbackEpochStarts(
	starts map[uint64]uint64,
	slot uint64,
) (uint64, bool) {
	var newest uint64
	found := false
	for id, start := range starts {
		if start > slot {
			delete(starts, id)
		} else if !found || id > newest {
			newest, found = id, true
		}
	}
	return newest, found
}

// stagedRollbackEpochStarts maps each epoch the set holds to its start slot
// when ops stage a rollback, and is empty otherwise. Only a rollback reads the
// starts, and Epochs decodes every epoch row, so a read with none staged must
// not pay for it once per epoch it is asked about.
func stagedRollbackEpochStarts(
	r EpochStateReader,
	ops []epochStateOps,
) (map[uint64]uint64, error) {
	if !slices.ContainsFunc(ops, func(o epochStateOps) bool {
		return o.rollbackTo != nil
	}) {
		return map[uint64]uint64{}, nil
	}
	epochs, err := r.Epochs()
	if err != nil {
		return nil, err
	}
	starts := make(map[uint64]uint64, len(epochs))
	for _, e := range epochs {
		starts[e.EpochId] = e.StartSlot
	}
	return starts, nil
}

// snapshotView is epoch's snapshot rows of typ as txn sees the set, in pool
// order. A positive limit returns only the first limit rows and reads no more
// than that many from the set: a row among the first limit of the view is
// either staged or among the first limit the set holds.
func (ls *LedgerState) snapshotView(
	r EpochStateReader,
	txn ledgerdb.Txn,
	epoch uint64,
	typ string,
	limit int,
) ([]*models.PoolStakeSnapshot, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	return ls.snapshotViewLocked(r, txn, epoch, typ, limit)
}

// snapshotViewLocked is snapshotView for a caller holding utxoSetApplyMu for
// reading.
func (ls *LedgerState) snapshotViewLocked(
	r EpochStateReader,
	txn ledgerdb.Txn,
	epoch uint64,
	typ string,
	limit int,
) ([]*models.PoolStakeSnapshot, error) {
	ops := ls.stagedEpochStateOps(txn)
	rows, err := r.PoolStakeSnapshots(epoch, typ, limit)
	if err != nil {
		return nil, err
	}
	starts, err := stagedRollbackEpochStarts(r, ops)
	if err != nil {
		return nil, err
	}
	view := make(map[string]*models.PoolStakeSnapshot, len(rows))
	for _, s := range rows {
		view[string(s.PoolKeyHash)] = s
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			// The rollback leaves the epochs that start at or before it;
			// snapshots of later epochs than the newest of those go.
			newest, found := rollbackEpochStarts(starts, *o.rollbackTo)
			if found && epoch > newest {
				clear(view)
			}
		}
		for _, e := range o.epochs {
			starts[e.EpochId] = e.StartSlot
		}
		for _, c := range o.snapshots {
			switch c.Kind {
			case SnapshotSave:
				if c.Snapshot.Epoch == epoch && c.Snapshot.SnapshotType == typ {
					s := *c.Snapshot
					view[string(s.PoolKeyHash)] = &s
				}
			case SnapshotDeleteForEpoch:
				if c.Epoch == epoch && c.Type == typ {
					clear(view)
				}
			case SnapshotDeleteBeforeEpoch:
				if epoch < c.Epoch {
					clear(view)
				}
			}
		}
	}
	out := make([]*models.PoolStakeSnapshot, 0, len(view))
	for _, s := range view {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b *models.PoolStakeSnapshot) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// snapshotsCapturedInView is every snapshot row captured in [from, through]
// as txn sees the set, for every epoch and type, sorted by epoch, type and
// pool. It applies the staged batches as snapshotViewLocked does for one epoch
// and type. Rows outside the range are dropped before the batches apply, which
// cannot change the result: a staged save replaces a row by its key whatever
// the stored row held, and a staged delete only removes rows.
func (ls *LedgerState) snapshotsCapturedInView(
	r EpochStateReader,
	txn ledgerdb.Txn,
	from, through uint64,
) ([]*models.PoolStakeSnapshot, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	type rowKey struct {
		epoch uint64
		typ   string
		pool  string
	}
	ops := ls.stagedEpochStateOps(txn)
	rows, err := r.PoolStakeSnapshotsCapturedIn(from, through)
	if err != nil {
		return nil, err
	}
	starts, err := stagedRollbackEpochStarts(r, ops)
	if err != nil {
		return nil, err
	}
	view := make(map[rowKey]*models.PoolStakeSnapshot, len(rows))
	for _, s := range rows {
		view[rowKey{s.Epoch, s.SnapshotType, string(s.PoolKeyHash)}] = s
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			newest, found := rollbackEpochStarts(starts, *o.rollbackTo)
			if found {
				maps.DeleteFunc(view, func(k rowKey, _ *models.PoolStakeSnapshot) bool {
					return k.epoch > newest
				})
			}
		}
		for _, e := range o.epochs {
			starts[e.EpochId] = e.StartSlot
		}
		for _, c := range o.snapshots {
			switch c.Kind {
			case SnapshotSave:
				s := *c.Snapshot
				view[rowKey{s.Epoch, s.SnapshotType, string(s.PoolKeyHash)}] = &s
			case SnapshotDeleteForEpoch:
				maps.DeleteFunc(view, func(k rowKey, _ *models.PoolStakeSnapshot) bool {
					return k.epoch == c.Epoch && k.typ == c.Type
				})
			case SnapshotDeleteBeforeEpoch:
				maps.DeleteFunc(view, func(k rowKey, _ *models.PoolStakeSnapshot) bool {
					return k.epoch < c.Epoch
				})
			}
		}
	}
	out := make([]*models.PoolStakeSnapshot, 0, len(view))
	for _, s := range view {
		if s.CapturedSlot >= from && s.CapturedSlot <= through {
			out = append(out, s)
		}
	}
	slices.SortFunc(out, func(a, b *models.PoolStakeSnapshot) int {
		return cmp.Or(
			cmp.Compare(a.Epoch, b.Epoch),
			cmp.Compare(a.SnapshotType, b.SnapshotType),
			bytes.Compare(a.PoolKeyHash, b.PoolKeyHash),
		)
	})
	return out, nil
}

// epochStateReader returns the set's epoch state reader and the read mode,
// or false when the set does not serve them.
func (ls *LedgerState) epochStateReader(
	txn ledgerdb.Txn,
) (EpochStateReader, bool) {
	_, mode := ls.utxoSetReader()
	if !mode {
		return nil, false
	}
	r, ok := ls.readSet(txn).(EpochStateReader)
	if !ok {
		return nil, false
	}
	if !ls.epochStateSetLoaded.Load() {
		loaded, err := r.EpochStateLoaded()
		if err != nil || !loaded {
			return nil, false
		}
		ls.epochStateSetLoaded.Store(true)
	}
	return r, mode
}

func (s stagingLedgerDB) GetEpochs(txn ledgerdb.Txn) ([]models.Epoch, error) {
	return s.epochsWhere(txn, nil)
}

func (s stagingLedgerDB) GetEpochsByEra(
	eraId uint,
	txn ledgerdb.Txn,
) ([]models.Epoch, error) {
	return s.epochsWhere(txn, func(e models.Epoch) bool { return e.EraId == eraId })
}

func (s stagingLedgerDB) epochsWhere(
	txn ledgerdb.Txn,
	keep func(models.Epoch) bool,
) ([]models.Epoch, error) {
	r, mode := s.ls.epochStateReader(txn)
	if !mode || r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyEpochs)
	}
	view, err := s.ls.epochStateView(r, txn)
	return sortedEpochs(view, keep), err
}

func (s stagingLedgerDB) GetEpoch(
	epochId uint64,
	txn ledgerdb.Txn,
) (*models.Epoch, error) {
	return s.epochOne(txn, func(view map[uint64]models.Epoch) *models.Epoch {
		if e, ok := view[epochId]; ok {
			return &e
		}
		return nil
	})
}

func (s stagingLedgerDB) GetEpochBySlot(
	slot uint64,
	txn ledgerdb.Txn,
) (*models.Epoch, error) {
	return s.epochOne(txn, func(view map[uint64]models.Epoch) *models.Epoch {
		var best *models.Epoch
		for _, e := range view {
			if e.StartSlot <= slot && slot < e.StartSlot+uint64(e.LengthInSlots) &&
				(best == nil || e.StartSlot > best.StartSlot) {
				c := e
				best = &c
			}
		}
		return best
	})
}

func (s stagingLedgerDB) epochOne(
	txn ledgerdb.Txn,
	pick func(map[uint64]models.Epoch) *models.Epoch,
) (*models.Epoch, error) {
	r, mode := s.ls.epochStateReader(txn)
	if !mode || r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyEpochs)
	}
	view, err := s.ls.epochStateView(r, txn)
	if err != nil {
		return nil, err
	}
	return pick(view), nil
}

func (s stagingLedgerDB) GetPoolStakeSnapshotsByEpoch(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	return s.snapshotsWhere(txn, epoch, snapshotType, nil)
}

// GetPoolStakeSnapshotsByEpochBounded returns at most maxRows of the
// snapshot rows of epoch, so a caller that asks for one row more than it
// accepts learns the snapshot is larger without receiving all of it.
func (s stagingLedgerDB) GetPoolStakeSnapshotsByEpochBounded(
	epoch uint64,
	snapshotType string,
	maxRows int,
	txn ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	if maxRows <= 0 {
		return nil, errors.New("pool stake snapshot row limit must be positive")
	}
	r, mode := s.ls.epochStateReader(txn)
	if !mode || r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyEpochs)
	}
	return s.ls.snapshotView(r, txn, epoch, snapshotType, maxRows)
}

func (s stagingLedgerDB) GetPoolStakeSnapshotsForPools(
	epoch uint64,
	snapshotType string,
	poolKeyHashes [][]byte,
	txn ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	want := make(map[string]struct{}, len(poolKeyHashes))
	for _, p := range poolKeyHashes {
		want[string(p)] = struct{}{}
	}
	return s.snapshotsWhere(txn, epoch, snapshotType, func(sn *models.PoolStakeSnapshot) bool {
		_, ok := want[string(sn.PoolKeyHash)]
		return ok
	})
}

func (s stagingLedgerDB) GetPoolStakeSnapshot(
	epoch uint64,
	snapshotType string,
	poolKeyHash []byte,
	txn ledgerdb.Txn,
) (*models.PoolStakeSnapshot, error) {
	got, err := s.snapshotsWhere(txn, epoch, snapshotType, func(sn *models.PoolStakeSnapshot) bool {
		return bytes.Equal(sn.PoolKeyHash, poolKeyHash)
	})
	if err != nil || len(got) == 0 {
		return nil, err
	}
	return got[0], nil
}

func (s stagingLedgerDB) snapshotsWhere(
	txn ledgerdb.Txn,
	epoch uint64,
	typ string,
	keep func(*models.PoolStakeSnapshot) bool,
) ([]*models.PoolStakeSnapshot, error) {
	r, mode := s.ls.epochStateReader(txn)
	if !mode || r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyEpochs)
	}
	all, err := s.ls.snapshotView(r, txn, epoch, typ, 0)
	if err != nil || keep == nil {
		return all, err
	}
	fromSet := make([]*models.PoolStakeSnapshot, 0, len(all))
	for _, sn := range all {
		if keep(sn) {
			fromSet = append(fromSet, sn)
		}
	}
	return fromSet, nil
}
