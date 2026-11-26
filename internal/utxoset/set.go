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

// Package utxoset keeps the live UTxO set in a LedgerDB Badger store, updated
// synchronously by the ledger after each commit.
package utxoset

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Config configures a Set.
type Config struct {
	// Dir is the Badger directory.
	Dir        string
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	// HistorySlots is how far behind the tip the rollback journal reaches:
	// the stability window, past which no rollback goes. Zero keeps the
	// whole journal.
	HistorySlots uint64
	// CheckpointDir is where WriteCheckpoint writes; empty keeps no
	// checkpoints. CheckpointKeep is how many it retains, DefaultCheckpointKeep
	// when below one.
	CheckpointDir  string
	CheckpointKeep int
	// MemTableSize is Badger's memtable size in bytes; zero uses
	// DefaultMemTableSize.
	MemTableSize int64
}

// DefaultMemTableSize is the memtable size of a set whose Config leaves
// MemTableSize zero; zero keeps Badger's default. A test binary that opens
// many sets sets it once from TestMain, before any set is opened.
var DefaultMemTableSize int64

// DefaultBlockCacheSize is the Badger block cache size of every set; zero
// keeps Badger's default. A test binary that opens many sets sets it once
// from TestMain, before any set is opened.
var DefaultBlockCacheSize int64

// pruneEvery is how many tip-moving commits pass between journal prunes.
const pruneEvery = 1000

// Set is the live UTxO set in Badger. It implements ledger.UtxoSetWriter.
type Set struct {
	store  *badgerutxo.Store
	logger *slog.Logger
	m      *metrics
	// unregister releases the metrics Open registered. A snapshot view
	// shares them and has none.
	unregister func()
	// resuming is set while the ledger rolls back to the set's tip. The
	// rollback's change is computed against the ledger's set at its tip,
	// so against a set already at the rollback point every output it
	// touches reads as missing or duplicate, and none is a disagreement.
	resuming atomic.Bool
	history  uint64
	commits  uint64
	// migrating is set while the v0.94.2 migration moves a database's ledger
	// state into the set: only then does a metadata store hold the state the
	// set can be rebuilt or loaded from.
	migrating atomic.Bool
	// ckptDir, ckptKeep and ckptMu describe where checkpoints are written;
	// ckptWrite serializes writers.
	ckptMu    sync.Mutex
	ckptDir   string
	ckptKeep  int
	ckptWrite sync.Mutex
	// availableDisk reports the free bytes of the filesystem holding a path;
	// nil means AvailableDiskBytes. Only tests replace it.
	availableDisk func(path string) (uint64, error)
	// afterSwapCheckpoint runs once a re-import's checkpoint is written and
	// before it is restored. Only tests set it.
	afterSwapCheckpoint func()
	// ckptBarrier holds off ledger commits while a checkpoint fixes its
	// version; see SetCheckpointBarrier.
	ckptBarrier atomic.Pointer[CheckpointBarrier]
	// reimport, when set, loads ledger state from a Mithril snapshot when no
	// checkpoint can bring the set to the ledger; see SetReimporter.
	reimport atomic.Pointer[ReimportFunc]
	// scratchReimport, when set, replaces reimport with a re-import that
	// loads into a copy of the set; see SetScratchReimporter.
	scratchReimport atomic.Pointer[ScratchReimportFunc]
	// dir is the set's directory.
	dir string
	// pruneJournal trims the rollback journal; a field so a test can fail
	// it, which a healthy store never does right after a commit.
	pruneJournal func(beforeSlot uint64) error
	// holdMu guards holds and closing; holdIdle is signalled when holds
	// reaches zero. See HoldOpen.
	holdMu   sync.Mutex
	holdIdle *sync.Cond
	holds    int
	closing  bool
}

// UpdateAnchor records the chain's immutable tip, the point k blocks behind
// the ledger tip, which the journal prune never passes. It only stores the
// point and calls nothing else, so the ledger's commit path, which applies
// this set's changes while the ledger holds its own locks, can run beside it.
// A stale anchor only makes the prune more conservative; until one is
// recorded the journal is not pruned.
func (s *Set) UpdateAnchor(point ocommon.Point) {
	s.store.SetAnchor(point)
}

// AnchorView opens a read-only view of the set as of the last recorded
// immutable tip. See badgerutxo.Store.AnchorView. It reads the recorded
// anchor rather than asking the ledger for a fresh one, so it can be called
// under any ledger lock.
func (s *Set) AnchorView() (*badgerutxo.AsOf, error) {
	return s.store.AnchorView()
}

var (
	_ ledger.UtxoSetWriter        = (*Set)(nil)
	_ ledger.UtxoSetSnapshotter   = (*Set)(nil)
	_ ledger.UtxoSetFailureMarker = (*Set)(nil)
)

// SnapshotUtxoSet returns a read-only view of the set as of now and the
// function that releases it. Every read of the view, in every family, sees
// the state of the changes applied before the call and none after; a write
// through it fails.
func (s *Set) SnapshotUtxoSet() (ledger.UtxoSetWriter, func(), error) {
	store, err := s.store.Snapshot()
	if err != nil {
		return nil, nil, err
	}
	view := &Set{
		store:   store,
		logger:  s.logger,
		m:       s.m,
		history: s.history,
	}
	return view, func() { _ = store.Close() }, nil
}

type metrics struct {
	applied   prometheus.Counter
	missing   prometheus.Counter
	duplicate prometheus.Counter
	rebuilds  prometheus.Counter
	resumes   prometheus.Counter
	rollbacks prometheus.Counter
	// checkpointsSkipped counts checkpoints not written for lack of disk.
	checkpointsSkipped prometheus.Counter
}

// newMetrics returns the set's metrics and the function that unregisters
// them from reg.
func newMetrics(reg prometheus.Registerer) (*metrics, func()) {
	var collectors []prometheus.Collector
	c := func(name, help string) prometheus.Counter {
		ctr := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dingo",
			Subsystem: "utxo_set",
			Name:      name,
			Help:      help,
		})
		if reg != nil {
			reg.MustRegister(ctr)
			collectors = append(collectors, ctr)
		}
		return ctr
	}
	unregister := func() {
		for _, ctr := range collectors {
			reg.Unregister(ctr)
		}
	}
	return &metrics{
		applied: c(
			"changes_applied_total",
			"UTxO changes applied to the live UTxO set.",
		),
		missing: c(
			"missing_inputs_total",
			"Removed outputs that were not live in the live UTxO set.",
		),
		duplicate: c(
			"duplicate_outputs_total",
			"Added outputs that were already live in the live UTxO set.",
		),
		rebuilds: c(
			"rebuilds_total",
			"Rebuilds of the live UTxO set from the metadata store.",
		),
		resumes: c(
			"resumes_total",
			"Startups that rolled the ledger back to the live UTxO set's tip.",
		),
		rollbacks: c(
			"startup_rollbacks_total",
			"Startups that rolled the live UTxO set back to the ledger's tip.",
		),
		checkpointsSkipped: c(
			"checkpoints_skipped_total",
			"Live UTxO set checkpoints not written because the disk lacked the space for the copy.",
		),
	}, unregister
}

// anchored reports whether an immutable tip has been recorded.
func (s *Set) anchored() bool {
	_, ok := s.store.Anchor()
	return ok
}

// Open opens or creates the set in cfg.Dir.
func Open(cfg Config) (*Set, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	memTable := cfg.MemTableSize
	if memTable == 0 {
		memTable = DefaultMemTableSize
	}
	store, err := badgerutxo.Open(
		cfg.Dir,
		badgerutxo.WithAggregator(ledger.UtxoStakeOf),
		badgerutxo.WithMemTableSize(memTable),
		badgerutxo.WithBlockCacheSize(DefaultBlockCacheSize),
		badgerutxo.WithRetention(badgerutxo.Retention{
			RewardEpochs:   uint64(ledger.RewardInputEpochWindow),
			SnapshotEpochs: ledgerdb.PoolSnapshotRetentionBackstopEpochs,
		}),
	)
	if err != nil {
		return nil, err
	}
	m, unregister := newMetrics(cfg.Registerer)
	set := &Set{
		store:        store,
		dir:          cfg.Dir,
		logger:       logger.With("component", "utxoset"),
		m:            m,
		unregister:   unregister,
		history:      cfg.HistorySlots,
		pruneJournal: store.PruneJournal,
	}
	set.SetCheckpointDir(cfg.CheckpointDir, cfg.CheckpointKeep)
	return set, nil
}

// HoldOpen keeps the set open for a reader that runs outside the ledger's
// commit path, such as a background job: Close waits for release before it
// closes the store. ok is false once Close has begun, and the caller must
// then not read the set, whose store panics on a read after it is closed.
// The ledger's own Close stops its jobs first; the hold covers a set closed
// under a ledger that was not. A hold is short and never taken while waiting
// on something Close's caller holds.
func (s *Set) HoldOpen() (release func(), ok bool) {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()
	if s.closing {
		return nil, false
	}
	s.holds++
	var once sync.Once
	return func() {
		once.Do(func() {
			s.holdMu.Lock()
			defer s.holdMu.Unlock()
			s.holds--
			if s.holds == 0 && s.holdIdle != nil {
				s.holdIdle.Broadcast()
			}
		})
	}, true
}

// refuseHolds makes HoldOpen refuse and waits for every hold to be released.
func (s *Set) refuseHolds() {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()
	s.closing = true
	if s.holdIdle == nil {
		s.holdIdle = sync.NewCond(&s.holdMu)
	}
	for s.holds > 0 {
		s.holdIdle.Wait()
	}
}

// Close closes the set and unregisters its metrics, so the set can be
// opened again on the same registry. It first waits for every HoldOpen to be
// released.
func (s *Set) Close() error {
	s.refuseHolds()
	if s.unregister != nil {
		s.unregister()
		s.unregister = nil
	}
	return s.store.Close()
}

// ApplyUtxoChanges applies one ledger commit's changes atomically.
func (s *Set) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	return s.ApplyLedgerChanges(tip, ledger.LedgerChanges{Utxo: changes})
}

// ApplyLedgerChanges applies one ledger commit's UTxO changes, block nonces
// and nonce prune atomically.
func (s *Set) ApplyLedgerChanges(
	tip *ocommon.Point,
	lc ledger.LedgerChanges,
) error {
	changes := lc.Utxo
	nonces := make([]badgerutxo.Nonce, 0, len(lc.Nonces))
	for _, n := range lc.Nonces {
		nonces = append(nonces, toStoreNonce(n))
	}
	batch := make([]badgerutxo.Change, 0, len(changes))
	var rollbackTo *uint64
	for _, evt := range changes {
		if evt.Cause == ledger.UtxoChangeRollback &&
			(rollbackTo == nil || evt.Point.Slot < *rollbackTo) {
			slot := evt.Point.Slot
			rollbackTo = &slot
		}
		c := badgerutxo.Change{
			Point:   evt.Point,
			Added:   make([]badgerutxo.Output, 0, len(evt.Added)),
			Removed: make([]badgerutxo.Ref, 0, len(evt.Removed)),
		}
		for _, o := range evt.Added {
			c.Added = append(c.Added, badgerutxo.Output{
				Ref:  badgerutxo.NewRef(o.Ref.Hash, o.Ref.Idx),
				Cbor: o.Cbor,
			})
		}
		for _, r := range evt.Removed {
			c.Removed = append(c.Removed, badgerutxo.NewRef(r.Hash, r.Idx))
		}
		batch = append(batch, c)
	}
	epochs, snapOps, err := epochStateBatch(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	summaries, err := summaryBatch(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	states, donations := networkStateBatch(lc)
	pools, err := poolRows(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	poolEvents, err := poolEventRows(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	registrations, err := registrationHistoryRows(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	genesisMIR, err := genesisMIRRows(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	accountWitnesses, err := accountWitnessRows(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	closureRows, err := s.closureUtxoRows(lc)
	if err != nil {
		return err
	}
	delegations, err := delegationRows(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	rewards, err := rewardRows(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	inputRows, err := s.rewardInputPuts(lc)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	derivedRows, err := rewardDerivedPuts(lc, s)
	if err != nil {
		return fmt.Errorf("utxoset: %w", err)
	}
	boundaryRows, err := governanceBoundaryRows(lc)
	if err != nil {
		return err
	}
	// A store transaction holds a bounded number of writes, so a large
	// reward write goes in after the commit's first chunk of it.
	var moreRewards []badgerutxo.RowPut
	if len(rewards) > rewardRowChunk {
		rewards, moreRewards = rewards[:rewardRowChunk], rewards[rewardRowChunk:]
	}
	pparams := make([]badgerutxo.PParamsRow, 0, len(lc.PParams))
	for _, p := range lc.PParams {
		pparams = append(pparams, pparamsRow(p))
	}
	if tip != nil && rollbackTo == nil {
		batch, err = s.withBlockPoints(batch, nonces, *tip)
		if err != nil {
			return err
		}
	}
	leading, batch := splitTipless(tip, changes, batch)
	var res badgerutxo.ApplyResult
	for _, group := range leading {
		r, err := s.store.ApplyBatch(nil, group)
		res.Missing = append(res.Missing, r.Missing...)
		res.Duplicate = append(res.Duplicate, r.Duplicate...)
		if err != nil {
			return err
		}
	}
	txIndex, err := txIndexRows(lc)
	if err != nil {
		return err
	}
	last, err := s.store.ApplyLedgerBatch(tip, badgerutxo.LedgerBatch{
		Changes:           batch,
		Nonces:            nonces,
		PruneNoncesBefore: lc.PruneNoncesBefore,
		DeleteNoncesAfter: rollbackTo,
		Epochs:            epochs,
		SnapshotOps:       snapOps,
		Summaries:         summaries,
		EraSwitch:         eraSwitchRecord(lc.EraSwitch),
		NetworkStates:     states,
		Donations:         donations,
		PParams:           pparams,
		Markers:           syncMarkerWrites(lc),
		Accounts:          accountPuts(lc.Accounts),
		RewardRecords:     rewardRecords(lc.RewardRecords),
		GovernanceIDs:     governanceIDs(lc),
		RewardBlocks:      rewardBlockOps(lc),
		Rows: slices.Concat(
			drepRows(lc), pools, poolEvents, registrations, genesisMIR, opCertRows(lc),
			committeeRows(lc),
			governanceRows(lc),
			pparamUpdateRows(lc),
			rewards,
			accountWitnesses,
			closureRows,
			delegations,
			inputRows,
			derivedRows,
			boundaryRows,
			stateMarkRows(lc),
			txIndex,
		),
	})
	if err != nil {
		return err
	}
	res.Missing = append(res.Missing, last.Missing...)
	res.Duplicate = append(res.Duplicate, last.Duplicate...)
	for len(moreRewards) > 0 {
		n := min(rewardRowChunk, len(moreRewards))
		if _, err := s.store.ApplyLedgerBatch(nil, badgerutxo.LedgerBatch{
			Rows: moreRewards[:n],
		}); err != nil {
			return err
		}
		moreRewards = moreRewards[n:]
	}
	s.m.applied.Add(float64(len(changes)))
	if tip != nil && s.history > 0 && tip.Slot > s.history {
		// Only the committing goroutine calls this, so commits needs no lock.
		s.commits++
		if s.commits%pruneEvery == 0 && s.anchored() {
			// The batch and tip are already committed, so a failure here
			// is not a failed commit: the next prune retries it.
			if err := s.pruneJournal(tip.Slot - s.history); err != nil {
				s.logger.Warn(
					"pruning the UTxO rollback journal failed",
					"slot", tip.Slot,
					"error", err,
				)
			}
		}
	}
	if s.resuming.Load() {
		return nil
	}
	if len(res.Missing) > 0 || len(res.Duplicate) > 0 {
		s.m.missing.Add(float64(len(res.Missing)))
		s.m.duplicate.Add(float64(len(res.Duplicate)))
		attrs := []any{
			"changes", len(changes),
			"missing_outputs", len(res.Missing),
			"duplicate_outputs", len(res.Duplicate),
		}
		if tip != nil {
			attrs = append(attrs, "slot", tip.Slot)
		}
		s.logger.Warn("UTxO changes disagree with the live UTxO set", attrs...)
	}
	return nil
}

// MarkUtxoSetFailed implements ledger.UtxoSetFailureMarker. A zero slot
// stands for a failure the ledger could not place, which the set's own tip
// bounds.
func (s *Set) MarkUtxoSetFailed(slot uint64) error {
	if slot == 0 {
		tip, ok, err := s.store.Tip()
		if err != nil {
			return err
		}
		if ok {
			slot = tip.Slot
		}
	}
	return s.store.MarkFailed(slot)
}

// JournalFloor returns the oldest point Rollback reaches; ok is false for a
// set that has recorded no tip.
func (s *Set) JournalFloor() (ocommon.Point, bool, error) {
	return s.store.JournalFloor()
}

// CanRollbackTo reports whether the set can be rolled back to exactly point:
// it is the set's tip or the tip before a journaled commit.
func (s *Set) CanRollbackTo(point ocommon.Point) (bool, error) {
	return s.store.HasJournalPoint(point)
}

// JournalReaches implements ledger.UtxoSetRollbacker.
func (s *Set) JournalReaches(point ocommon.Point) (bool, error) {
	return s.store.RollbackReaches(point)
}

// RollbackTo rolls the set back to point from its journal.
func (s *Set) RollbackTo(point ocommon.Point) error {
	_, err := s.Rollback(point)
	return err
}

// Backup streams a consistent copy of the whole set to w.
func (s *Set) Backup(ctx context.Context, w io.Writer) error {
	return s.store.Backup(ctx, w)
}

// FailedAt returns the slot a failure mark records; ok is false when none is
// recorded.
func (s *Set) FailedAt() (uint64, bool, error) {
	return s.store.Failed()
}

// splitTipless cuts a batch without a tip into the groups that each stay
// within the ledger's event bounds, and returns the leading groups apart from
// the last. Changes without a tip, such as the genesis outputs, can together
// exceed what one Badger transaction takes; the caller writes the leading
// groups on their own and the last with the rest of the commit. A change with
// a tip is one block's and is never split, which keeps the tip and its
// changes atomic.
func splitTipless(
	tip *ocommon.Point,
	events []ledger.UtxoEvent,
	batch []badgerutxo.Change,
) (leading [][]badgerutxo.Change, last []badgerutxo.Change) {
	if tip != nil {
		return nil, batch
	}
	start, entries, bytes := 0, 0, 0
	for i, evt := range events {
		e, b := ledger.UtxoEventCost(evt)
		if i > start && (entries+e > ledger.MaxUtxoEventEntries ||
			bytes+b > ledger.MaxUtxoEventBytes) {
			leading = append(leading, batch[start:i])
			start, entries, bytes = i, 0, 0
		}
		entries += e
		bytes += b
	}
	return leading, batch[start:]
}

// RemoveZeroCoinOutputs removes the live outputs among refs whose coin is
// zero and returns how many it removed. An output with coin, or one that does
// not decode, stays. The removal is not a change of the chain: it leaves the
// tip alone and writes no journal record, which would otherwise be undone
// first by the next rollback of any depth. It is for outputs the ledger's
// genesis never holds, found in a set built before it dropped them.
func (s *Set) RemoveZeroCoinOutputs(refs []models.UtxoId) (int, error) {
	out := make([]badgerutxo.Ref, len(refs))
	for i, r := range refs {
		out[i] = badgerutxo.NewRef(r.Hash, r.Idx)
	}
	return s.store.RemoveOutputs(out, isZeroCoinOutput)
}

func isZeroCoinOutput(cbor []byte) bool {
	out, err := gledger.NewTransactionOutputFromCbor(cbor)
	if err != nil || out == nil {
		return false
	}
	amount := out.Amount()
	return amount != nil && amount.Sign() == 0
}

// Get returns the CBOR of the live output txID#index.
func (s *Set) Get(txID []byte, index uint32) ([]byte, bool, error) {
	return s.store.Get(badgerutxo.NewRef(txID, index))
}

// RollbackPreview returns the change Rollback(point) would make, without
// making it.
func (s *Set) RollbackPreview(point ocommon.Point) (ledger.UtxoEvent, error) {
	res, err := s.store.RollbackPreview(point)
	if err != nil {
		return ledger.UtxoEvent{}, err
	}
	return rollbackEvent(point, res), nil
}

// PreviewUtxoSetRollback implements ledger.UtxoSetRollbacker. A point the
// journal does not reach, or does not hold, is also
// ledger.ErrUtxoAsOfUnavailable, which a replay of a past boundary takes for a
// boundary it cannot restore.
func (s *Set) PreviewUtxoSetRollback(
	point ocommon.Point,
) (ledger.UtxoEvent, error) {
	evt, err := s.RollbackPreview(point)
	if errors.Is(err, badgerutxo.ErrRollbackBeyondJournal) ||
		errors.Is(err, badgerutxo.ErrPointNotInJournal) {
		return evt, fmt.Errorf("%w: %w", ledger.ErrUtxoAsOfUnavailable, err)
	}
	return evt, err
}

// Rollback rolls the set back to point from its journal and returns the net
// change as a ledger.utxo rollback change. It returns
// badgerutxo.ErrRollbackBeyondJournal when the journal does not reach point.
func (s *Set) Rollback(point ocommon.Point) (ledger.UtxoEvent, error) {
	res, err := s.store.Rollback(point)
	if err != nil {
		return ledger.UtxoEvent{}, err
	}
	return rollbackEvent(point, res), nil
}

func rollbackEvent(
	point ocommon.Point,
	res badgerutxo.RollbackResult,
) ledger.UtxoEvent {
	evt := ledger.UtxoEvent{
		Point:   point,
		Cause:   ledger.UtxoChangeRollback,
		Added:   make([]ledger.UtxoOutput, 0, len(res.Added)),
		Removed: make([]models.UtxoId, 0, len(res.Removed)),
	}
	for _, o := range res.Added {
		evt.Added = append(evt.Added, ledger.UtxoOutput{
			Ref:  refID(o.Ref),
			Cbor: o.Cbor,
		})
	}
	for _, r := range res.Removed {
		evt.Removed = append(evt.Removed, refID(r))
	}
	return evt
}

// PruneHistory drops the journal of changes before slot, which no rollback
// will reach.
func (s *Set) PruneHistory(slot uint64) error {
	return s.store.PruneJournal(slot)
}

func refID(r badgerutxo.Ref) models.UtxoId {
	return models.UtxoId{
		Hash: append([]byte(nil), r[:32]...),
		Idx:  binary.BigEndian.Uint32(r[32:]),
	}
}

// Tip returns the ledger point the set last recorded; ok is false for a set
// that has recorded none.
func (s *Set) Tip() (ocommon.Point, bool, error) {
	return s.store.Tip()
}

// Count returns the number of live outputs.
func (s *Set) Count() (uint64, error) {
	return s.store.Count()
}

// ResumeFunc rolls the ledger back to point, the set's tip, so the blocks
// after it are applied again. It reports whether it did.
type ResumeFunc func(ctx context.Context, point ocommon.Point) (bool, error)

// Reconcile makes the set equal to the ledger's live set before the ledger
// applies anything. The set's tip is always a point at which it equalled the
// ledger, so a set at the ledger tip is already equal. A set behind it is
// handed to resume, which rolls the ledger back to the set's tip; when resume
// is nil or declines, the set recovers through its journal. The set is the
// only copy of the ledger state, so ldb supplies only the ledger's tip. While
// the ledger stands at origin the set holds no state: it starts empty, with
// every family loaded empty.
//
// Outside the v0.94.2 migration no store holds the rows a family load would
// replace the set's with, so Reconcile refuses family loads from then on.
func (s *Set) Reconcile(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	resume ResumeFunc,
) error {
	s.store.RefuseLoads()
	if err := s.beginReconcile(ctx); err != nil {
		return err
	}
	if err := s.reconcileOutputs(ctx, ldb, resume); err != nil {
		return err
	}
	return s.loadEmptyFamilies()
}

// ReconcileReleased brings the set to the ledger state of ldb, a v0.94.2
// database whose metadata store holds it, for the migration that moves that
// state into the set: a set that cannot be caught up to ldb's tip is rebuilt
// from ldb's live outputs, and every family the set lacks is loaded from ldb.
func (s *Set) ReconcileReleased(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	s.migrating.Store(true)
	defer s.migrating.Store(false)
	if err := s.beginReconcile(ctx); err != nil {
		return err
	}
	if err := s.reconcileOutputs(ctx, ldb, nil); err != nil {
		return err
	}
	return s.loadFamilies(ctx, ldb)
}

// beginReconcile finishes what a stopped checkpoint restore left and refuses
// a set an import has not finished.
func (s *Set) beginReconcile(ctx context.Context) error {
	if dir, _ := s.checkpointDir(); dir != "" {
		if err := removeTmpEntries(dir); err != nil {
			return err
		}
	}
	if err := s.finishPendingRestore(ctx); err != nil {
		return err
	}
	if _, pending, err := s.store.ImportPending(); err != nil {
		return fmt.Errorf("utxoset: read the import mark: %w", err)
	} else if pending {
		return badgerutxo.ErrImportPending
	}
	return nil
}

// LoadLedgerState loads into the set the ledger-state families it lacks, from
// the metadata store of a v0.94.2 database that kept them there. It is the
// family load ReconcileReleased runs once the outputs agree with that
// database, without the output reconciliation.
func (s *Set) LoadLedgerState(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	return s.loadFamilies(ctx, ldb)
}

// loadEmptyFamilies marks every family the set has not loaded as loaded
// empty while the set stands at origin, before the ledger has written any
// state. Past origin nothing outside the set holds the ledger state, so a
// family the set lacks there is an error rather than something to load.
func (s *Set) loadEmptyFamilies() error {
	tip, ok, err := s.store.Tip()
	if err != nil {
		return fmt.Errorf("utxoset: read tip: %w", err)
	}
	if !ok || isOrigin(tip) {
		if err := s.store.LoadMissingFamiliesEmpty(); err != nil {
			return fmt.Errorf("utxoset: load empty ledger state: %w", err)
		}
		return nil
	}
	for _, family := range nodesettings.Families() {
		loaded, err := ledger.FamilyLoaded(family, s)
		if err != nil {
			return fmt.Errorf("utxoset: read the %s markers: %w", family, err)
		}
		if !loaded {
			return fmt.Errorf(
				"utxoset: the live UTxO set at slot %d does not hold the %s ledger state",
				tip.Slot, family,
			)
		}
	}
	return nil
}

func (s *Set) loadFamilies(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	if err := s.ensureNonces(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureEpochState(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureSummaries(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureNetworkState(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensurePParams(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureSyncMarkers(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureAccounts(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureInactivity(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureAccountInactivityActivation(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureDrepState(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensurePools(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensurePoolHistory(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureRegistrationHistory(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureImportBaselines(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureWitnessSlots(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureClosureUtxoEffects(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureDelegations(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureTxIndex(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureGenesisMIR(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureRewardOutputs(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureOpCerts(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureRewardBlocks(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureImportedBlocks(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureRewardRecords(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureRewardInputs(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureRewardDerived(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureCommittee(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureGovernance(ctx, ldb); err != nil {
		return err
	}
	if err := s.ensureRatifyPending(ctx, ldb); err != nil {
		return err
	}
	return s.ensurePParamUpdates(ctx, ldb)
}

// ensureNonces loads the metadata store's block nonces up to the set's tip
// into a set bootstrapped or written before it kept them. The metadata store
// still records every nonce, so this never needs a rebuild.
func (s *Set) ensureNonces(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.NoncesLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the nonce marker: %w", err)
	}
	if loaded {
		return nil
	}
	tip, ok, err := s.store.Tip()
	if err != nil {
		return fmt.Errorf("utxoset: read tip: %w", err)
	}
	var nonces []badgerutxo.Nonce
	if ok {
		txn := ldb.Begin(ctx, false)
		rows, err := ldb.GetBlockNoncesInSlotRange(0, tip.Slot+1, txn)
		txn.Release()
		if err != nil {
			return fmt.Errorf("utxoset: read block nonces: %w", err)
		}
		nonces = make([]badgerutxo.Nonce, 0, len(rows))
		for _, n := range rows {
			nonces = append(nonces, toStoreNonce(n))
		}
	}
	if err := s.store.LoadNonces(nonces); err != nil {
		return fmt.Errorf("utxoset: load block nonces: %w", err)
	}
	s.logger.Info("loaded block nonces into the live UTxO set",
		"nonces", len(nonces), "slot", tip.Slot)
	return nil
}

func toStoreNonce(n models.BlockNonce) badgerutxo.Nonce {
	return badgerutxo.Nonce{
		Point:      ocommon.Point{Slot: n.Slot, Hash: n.Hash},
		Nonce:      n.Nonce,
		Checkpoint: n.IsCheckpoint,
	}
}

func fromStoreNonce(n badgerutxo.Nonce) models.BlockNonce {
	return models.BlockNonce{
		Hash:         n.Point.Hash,
		Slot:         n.Point.Slot,
		Nonce:        n.Nonce,
		IsCheckpoint: n.Checkpoint,
	}
}

// NoncesLoaded reports whether the set holds every block nonce up to its tip.
func (s *Set) NoncesLoaded() (bool, error) {
	return s.store.NoncesLoaded()
}

// BlockNonce returns the nonce of the block at point; ok is false when the
// set holds none.
func (s *Set) BlockNonce(point ocommon.Point) ([]byte, bool, error) {
	return s.store.Nonce(point)
}

// BlockNoncesInSlotRange returns the nonces of the blocks in [start, end), in
// slot order.
func (s *Set) BlockNoncesInSlotRange(
	start, end uint64,
) ([]models.BlockNonce, error) {
	ns, err := s.store.NoncesInSlotRange(start, end)
	if err != nil {
		return nil, err
	}
	out := make([]models.BlockNonce, 0, len(ns))
	for _, n := range ns {
		out = append(out, fromStoreNonce(n))
	}
	return out, nil
}

// LatestBlockNonce returns the highest-slot block nonce the set holds.
func (s *Set) LatestBlockNonce() (models.BlockNonce, bool, error) {
	n, ok, err := s.store.LatestNonce()
	if err != nil || !ok {
		return models.BlockNonce{}, ok, err
	}
	return fromStoreNonce(n), true, nil
}

func (s *Set) reconcileOutputs(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	resume ResumeFunc,
) error {
	ledgerTip, err := readLedgerTip(ctx, ldb)
	if err != nil {
		return err
	}
	incomplete, err := s.store.BootstrapIncomplete()
	if err != nil {
		return fmt.Errorf("utxoset: read bootstrap state: %w", err)
	}
	var setTip ocommon.Point
	var ok bool
	if !incomplete {
		if setTip, ok, err = s.store.Tip(); err != nil {
			return fmt.Errorf("utxoset: read tip: %w", err)
		}
	}
	stale, err := s.store.Stale()
	if err != nil {
		return fmt.Errorf("utxoset: read format: %w", err)
	}
	if stale {
		if !s.migrating.Load() {
			return s.recoverUnreachable(ctx, resume, ledgerTip, s.layoutError())
		}
		return s.rebuild(ctx, ldb, resume, ledgerTip, ok, setTip)
	}
	if ok {
		// A rollback that stopped between its transactions leaves rows from
		// two states, which nothing may read or rewind from until it ends.
		resumed, err := s.store.ResumeRollback()
		if err != nil {
			return s.unrecoverable(
				ctx, ldb, resume, ledgerTip, ok, setTip, err,
			)
		}
		if resumed {
			s.logger.Warn("finished a rollback that stopped part way")
			if setTip, _, err = s.store.Tip(); err != nil {
				return fmt.Errorf("utxoset: read tip: %w", err)
			}
		}
		if setTip, err = s.rewindFailed(setTip); err != nil {
			return s.unrecoverable(
				ctx, ldb, resume, ledgerTip, ok, setTip, err,
			)
		}
	}
	if err := s.reconcileTip(ctx, ldb, resume, ledgerTip, setTip, ok); err != nil {
		return err
	}
	if err := s.store.ClearFailed(); err != nil {
		return fmt.Errorf("utxoset: clear the failure mark: %w", err)
	}
	return nil
}

// rewindFailed undoes, from the journal, every commit at or after the slot
// the ledger failed to apply, and returns the resulting tip. A failed commit
// can have left rows behind that its tip does not account for, such as a
// reward write chunk that landed after the commit's first chunk or the
// outputs of a chunked batch that never reached its last transaction, so the
// ledger reapplies from before it.
func (s *Set) rewindFailed(setTip ocommon.Point) (ocommon.Point, error) {
	slot, failed, err := s.store.Failed()
	if err != nil {
		return setTip, fmt.Errorf("utxoset: read the failure mark: %w", err)
	}
	if !failed {
		return setTip, nil
	}
	before, found, err := s.store.PointBefore(slot)
	if err != nil {
		return setTip, fmt.Errorf(
			"utxoset: the ledger failed to apply a commit at slot %d and the journal does not reach back before it: %w",
			slot,
			err,
		)
	}
	// A chunked batch that stopped before its tip moved leaves records past
	// the tip, so a point at the tip is still a rewind.
	if !found {
		return setTip, nil
	}
	s.logger.Warn(
		"rolling the live UTxO set back before a commit the ledger failed to apply",
		"failed_slot",
		slot,
		"set_slot",
		setTip.Slot,
		"rolled_back_to",
		before.Slot,
	)
	if _, err := s.Rollback(before); err != nil {
		return setTip, fmt.Errorf(
			"utxoset: roll back to slot %d: %w",
			before.Slot,
			err,
		)
	}
	return before, nil
}

func (s *Set) reconcileTip(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	resume ResumeFunc,
	ledgerTip, setTip ocommon.Point,
	ok bool,
) error {
	if ok && samePoint(setTip, ledgerTip) {
		return nil
	}
	if ok && setTip.Slot > ledgerTip.Slot {
		restorable, err := s.store.HasJournalPoint(ledgerTip)
		if err != nil {
			return fmt.Errorf("utxoset: search the journal: %w", err)
		}
		if restorable {
			if _, err := s.Rollback(ledgerTip); err != nil {
				return fmt.Errorf(
					"utxoset: roll back to the ledger tip: %w",
					err,
				)
			}
			s.m.rollbacks.Inc()
			s.logger.Info("rolled the live UTxO set back to the ledger's tip",
				"set_slot", setTip.Slot, "ledger_slot", ledgerTip.Slot)
			return nil
		}
	}
	if ok && resume != nil {
		s.resuming.Store(true)
		resumed, err := resume(ctx, setTip)
		s.resuming.Store(false)
		if err != nil {
			s.logger.Warn("rolling the ledger back to the live UTxO set failed",
				"error", err, "set_slot", setTip.Slot,
				"ledger_slot", ledgerTip.Slot)
		}
		if resumed {
			s.m.resumes.Inc()
			s.logger.Info("rolled the ledger back to the live UTxO set's tip",
				"slot", setTip.Slot, "ledger_slot", ledgerTip.Slot)
			return nil
		}
		// A failed rollback may still have moved the ledger.
		if ledgerTip, err = readLedgerTip(ctx, ldb); err != nil {
			return err
		}
	}
	return s.rebuild(ctx, ldb, resume, ledgerTip, ok, setTip)
}

// unrecoverable handles a set whose failure mark cannot be rewound. A set the
// metadata store can rebuild is rebuilt. A set that is the only copy keeps its
// mark and stops the node: rows a failed commit left outside the journal
// cannot be told apart from the rest of the set, and the metadata store holds
// nothing to rebuild it from.
func (s *Set) unrecoverable(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	resume ResumeFunc,
	ledgerTip ocommon.Point,
	hadTip bool,
	setTip ocommon.Point,
	cause error,
) error {
	if !s.migrating.Load() {
		return s.recoverUnreachable(ctx, resume, ledgerTip, fmt.Errorf(
			"utxoset: the live UTxO set cannot be rewound before the commit the ledger failed to apply, and no copy of the ledger state from before that commit is retained: %w",
			cause,
		))
	}
	s.logger.Warn("rebuilding the live UTxO set after a failed update",
		"error", cause)
	return s.rebuild(ctx, ldb, resume, ledgerTip, hadTip, setTip)
}

func readLedgerTip(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) (ocommon.Point, error) {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	tip, err := ldb.GetTip(txn)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf("utxoset: read ledger tip: %w", err)
	}
	return ocommon.Point{Slot: tip.Point.Slot, Hash: tip.Point.Hash}, nil
}

func (s *Set) rebuild(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	resume ResumeFunc,
	ledgerTip ocommon.Point,
	hadTip bool,
	setTip ocommon.Point,
) error {
	// Outside the migration the set is the only copy of every family, so
	// only a ledger past origin needs a base.
	if !s.migrating.Load() && !isOrigin(ledgerTip) {
		recovered, err := s.recoverFromJournal(ctx, resume, ledgerTip)
		if err != nil {
			return err
		}
		if recovered {
			return nil
		}
		return s.recoverUnreachable(
			ctx, resume, ledgerTip,
			s.noBaseError(hadTip, setTip, ledgerTip),
		)
	}
	if !s.migrating.Load() {
		// At origin the ledger has applied nothing, so the set starts empty.
		if err := s.store.Bootstrap(
			ledgerTip,
			func(func(badgerutxo.Output) error) error { return nil },
		); err != nil {
			return fmt.Errorf("utxoset: start empty: %w", err)
		}
		return nil
	}
	s.logger.Info("rebuilding the live UTxO set from the metadata store",
		"had_tip", hadTip,
		"set_slot", setTip.Slot,
		"ledger_slot", ledgerTip.Slot)
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	err := s.store.Bootstrap(
		ledgerTip,
		func(put func(badgerutxo.Output) error) error {
			return ldb.IterateLiveUtxos(ctx, txn, func(u *models.Utxo) error {
				return put(badgerutxo.Output{
					Ref:  badgerutxo.NewRef(u.TxId, u.OutputIdx),
					Cbor: u.Cbor,
				})
			})
		},
	)
	if err != nil {
		return fmt.Errorf("utxoset: rebuild: %w", err)
	}
	s.m.rebuilds.Inc()
	n, _ := s.store.Count()
	s.logger.Info("rebuilt the live UTxO set", "live_outputs", n)
	return nil
}

// recoverFromJournal brings a set the metadata store cannot rebuild into step
// with the ledger without a rebuild. It walks the set's journal from the
// newest restorable point down: the set rolls back to the point, and the
// ledger is asked to roll back to the same point and apply the blocks after
// it again. The first point the ledger accepts ends the walk; a point it
// declines, such as a block of a fork the chain left, is passed over, and the
// set stays at the earlier point it rolled back to. A point equal to the
// ledger tip needs no resume. It reports false, leaving the set at the oldest
// point the journal reaches, when no point is accepted.
func (s *Set) recoverFromJournal(
	ctx context.Context,
	resume ResumeFunc,
	ledgerTip ocommon.Point,
) (bool, error) {
	points, err := s.store.JournalPoints()
	if err != nil {
		return false, fmt.Errorf("utxoset: read the journal points: %w", err)
	}
	if len(points) == 0 {
		return false, nil
	}
	s.logger.Warn(
		"recovering the live UTxO set from its rollback journal",
		"ledger_slot", ledgerTip.Slot,
		"candidate_points", len(points),
	)
	for i, p := range points {
		if p.Slot > ledgerTip.Slot ||
			(p.Slot == ledgerTip.Slot && !samePoint(p, ledgerTip)) {
			continue
		}
		if _, err := s.Rollback(p); err != nil {
			return false, fmt.Errorf(
				"utxoset: roll back to slot %d: %w", p.Slot, err,
			)
		}
		if err := s.store.ClearFailed(); err != nil {
			return false, fmt.Errorf(
				"utxoset: clear the failure mark: %w", err,
			)
		}
		if samePoint(p, ledgerTip) {
			s.m.rollbacks.Inc()
			s.logger.Info(
				"rolled the live UTxO set back to the ledger's tip",
				"slot", p.Slot,
			)
			return true, nil
		}
		if resume == nil {
			continue
		}
		s.resuming.Store(true)
		resumed, err := resume(ctx, p)
		s.resuming.Store(false)
		if err != nil && !resumed {
			// Stop here rather than roll the set further back for an
			// error that may be transient.
			return false, fmt.Errorf(
				"utxoset: roll the ledger back to slot %d: %w", p.Slot, err,
			)
		}
		if resumed {
			if err != nil {
				s.logger.Warn(
					"the ledger rolled back to a live UTxO set point but reported an error",
					"slot", p.Slot, "error", err,
				)
			}
			s.m.resumes.Inc()
			s.logger.Info(
				"rolled the live UTxO set and the ledger back to a point both hold",
				"slot", p.Slot,
				"ledger_slot", ledgerTip.Slot,
				"points_tried", i+1,
			)
			return true, nil
		}
	}
	return false, nil
}

func isOrigin(p ocommon.Point) bool {
	return p.Slot == 0 && len(p.Hash) == 0
}

// layoutError reports a set written in a layout this release does not read,
// when the metadata store cannot rebuild it. Rebuilding it from the metadata
// store would lose the families only the set holds, so the rows have to be
// migrated from the layout they are in to the one this release writes, and
// this release carries no such migration.
func (s *Set) layoutError() error {
	if incomplete, err := s.store.BootstrapIncomplete(); err == nil && incomplete {
		return errors.New(
			"utxoset: a load of the live UTxO set did not finish, so its contents are partial, and the metadata store no longer holds the ledger state to rebuild them from",
		)
	}
	stored, found, current, err := s.store.FormatVersion()
	if err != nil {
		return fmt.Errorf("utxoset: read format: %w", err)
	}
	from := "no layout version"
	if found {
		from = fmt.Sprintf("layout version %d", stored)
	}
	return fmt.Errorf(
		"utxoset: the live UTxO set was written with %s and this release reads layout version %d; it holds ledger state the metadata store no longer has, and this release has no migration of the set's rows from that layout, so start the release that wrote it",
		from, current,
	)
}

// noBaseError reports a set that no retained state can bring to the ledger.
func (s *Set) noBaseError(
	hadTip bool,
	setTip, ledgerTip ocommon.Point,
) error {
	floor := "its tip"
	if p, ok, err := s.store.JournalFloor(); err == nil && ok {
		floor = fmt.Sprintf("slot %d", p.Slot)
	}
	relation := "cannot be caught up to"
	if hadTip && setTip.Slot > ledgerTip.Slot {
		relation = "is ahead of"
	}
	return fmt.Errorf(
		"utxoset: the live UTxO set at slot %d %s the ledger at slot %d: its rollback journal reaches back only to %s and the ledger cannot resume from any point it holds, and no metadata store holds an earlier copy of the ledger state",
		setTip.Slot, relation, ledgerTip.Slot, floor,
	)
}

func samePoint(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

// withBlockPoints returns batch with an empty change at every block of the
// commit that changed no output. The store journals one record per change
// point and a rollback can only target a record's point, so without them a
// block with no transactions that commits with later blocks is not a point
// the set can roll back to. The commit's blocks are its nonce points, which
// the ledger stages for every block it applies.
func (s *Set) withBlockPoints(
	batch []badgerutxo.Change,
	nonces []badgerutxo.Nonce,
	tip ocommon.Point,
) ([]badgerutxo.Change, error) {
	if len(nonces) == 0 {
		return batch, nil
	}
	cur, _, err := s.store.Tip()
	if err != nil {
		return nil, fmt.Errorf("utxoset: read tip: %w", err)
	}
	last := cur.Slot
	for _, n := range nonces {
		// Points out of order or outside the commit are not its blocks.
		if len(n.Point.Hash) == 0 || n.Point.Slot < last ||
			n.Point.Slot > tip.Slot || samePoint(n.Point, cur) {
			return batch, nil
		}
		last = n.Point.Slot
	}
	out := make([]badgerutxo.Change, 0, len(batch)+len(nonces))
	i := 0
	for _, n := range nonces {
		for i < len(batch) && batch[i].Point.Slot < n.Point.Slot {
			out = append(out, batch[i])
			i++
		}
		if i == len(batch) || !samePoint(batch[i].Point, n.Point) {
			if len(out) == 0 || !samePoint(out[len(out)-1].Point, n.Point) {
				out = append(out, badgerutxo.Change{Point: n.Point})
			}
			continue
		}
		for i < len(batch) && samePoint(batch[i].Point, n.Point) {
			out = append(out, batch[i])
			i++
		}
	}
	return append(out, batch[i:]...), nil
}

// Sum returns the stake sum for a key built by ledger.CredentialStakeKey or
// ledger.PointerStakeKey.
func (s *Set) Sum(key []byte) (uint64, error) {
	return s.store.Sum(key)
}

// CredentialStake returns the lovelace in live outputs whose address names
// the stake credential (tag, hash).
func (s *Set) CredentialStake(tag uint8, hash []byte) (uint64, error) {
	return s.store.Sum(ledger.CredentialStakeKey(tag, hash))
}

// EachCredentialStake calls fn with every stake credential that live outputs
// name and their lovelace.
func (s *Set) EachCredentialStake(
	fn func(tag uint8, hash []byte, lovelace uint64) error,
) error {
	return s.store.EachSum(func(key []byte, n uint64) error {
		tag, hash, ok := ledger.ParseCredentialStakeKey(key)
		if !ok {
			return nil
		}
		return fn(tag, hash, n)
	})
}

// RollbackUtxoSet implements ledger.UtxoSetRollbacker.
func (s *Set) RollbackUtxoSet(point ocommon.Point) (ledger.UtxoEvent, error) {
	return s.Rollback(point)
}

// EachUtxo implements ledger.UtxoSetIterator.
func (s *Set) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	return s.store.Each(func(o badgerutxo.Output) error {
		id := refID(o.Ref)
		return fn(id.Hash, id.Idx, o.Cbor)
	})
}

// UtxoSetAt implements ledger.UtxoSetHistory from the set's journal, which
// reaches back one stability window.
func (s *Set) UtxoSetAt(point ocommon.Point) (ledger.UtxoSetView, error) {
	v, err := s.store.AsOf(point)
	if errors.Is(err, badgerutxo.ErrRollbackBeyondJournal) {
		return nil, fmt.Errorf("%w: %w", ledger.ErrUtxoAsOfUnavailable, err)
	}
	if errors.Is(err, badgerutxo.ErrPointNotInJournal) {
		return nil, fmt.Errorf("%w: %w", ledger.ErrPointNotOnChain, err)
	}
	if err != nil {
		return nil, err
	}
	return asOfView{v}, nil
}

type asOfView struct{ v *badgerutxo.AsOf }

func (a asOfView) Get(txID []byte, index uint32) ([]byte, bool, error) {
	return a.v.Get(badgerutxo.NewRef(txID, index))
}

func (a asOfView) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	return a.v.Each(func(o badgerutxo.Output) error {
		id := refID(o.Ref)
		return fn(id.Hash, id.Idx, o.Cbor)
	})
}

func (a asOfView) Close() { a.v.Close() }

// EachSum implements ledger.UtxoSetSums.
func (s *Set) EachSum(fn func(key []byte, lovelace uint64) error) error {
	return s.store.EachSum(fn)
}

// Sync makes every change applied so far durable. Badger syncs its log only
// on close otherwise, so a write elsewhere that records the set's contents
// syncs the set first.
func (s *Set) Sync() error {
	return s.store.Sync()
}
