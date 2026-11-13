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
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Config configures a Set.
type Config struct {
	// Dir is the Badger directory.
	Dir        string
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	// Only marks the set as the only copy of the UTxO set: the metadata
	// store has none to rebuild it from, so Reconcile rebuilds only at
	// origin, where the set is empty.
	Only bool
	// HistorySlots is how far behind the tip the rollback journal reaches:
	// the stability window, past which no rollback goes. Zero keeps the
	// whole journal.
	HistorySlots uint64
}

// pruneEvery is how many tip-moving commits pass between journal prunes.
const pruneEvery = 1000

// Set is the live UTxO set in Badger. It implements ledger.UtxoSetWriter.
type Set struct {
	store  *badgerutxo.Store
	logger *slog.Logger
	m      *metrics
	// resuming is set while the ledger rolls back to the set's tip. The
	// rollback's change is computed against the ledger's set at its tip,
	// so against a set already at the rollback point every output it
	// touches reads as missing or duplicate, and none is a disagreement.
	resuming atomic.Bool
	history  uint64
	commits  uint64
	only     bool
	// pruneJournal trims the rollback journal; a field so a test can fail
	// it, which a healthy store never does right after a commit.
	pruneJournal func(beforeSlot uint64) error
}

var _ ledger.UtxoSetWriter = (*Set)(nil)

type metrics struct {
	applied   prometheus.Counter
	missing   prometheus.Counter
	duplicate prometheus.Counter
	rebuilds  prometheus.Counter
	resumes   prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	c := func(name, help string) prometheus.Counter {
		ctr := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dingo",
			Subsystem: "utxo_set",
			Name:      name,
			Help:      help,
		})
		if reg != nil {
			reg.MustRegister(ctr)
		}
		return ctr
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
	}
}

// Open opens or creates the set in cfg.Dir.
func Open(cfg Config) (*Set, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	store, err := badgerutxo.Open(
		cfg.Dir,
		badgerutxo.WithAggregator(ledger.UtxoStakeOf),
	)
	if err != nil {
		return nil, err
	}
	return &Set{
		store:        store,
		logger:       logger.With("component", "utxoset"),
		m:            newMetrics(cfg.Registerer),
		history:      cfg.HistorySlots,
		only:         cfg.Only,
		pruneJournal: store.PruneJournal,
	}, nil
}

// Close closes the set.
func (s *Set) Close() error {
	return s.store.Close()
}

// ApplyUtxoChanges applies one ledger commit's changes atomically.
func (s *Set) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	batch := make([]badgerutxo.Change, 0, len(changes))
	for _, evt := range changes {
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
	res, err := s.store.ApplyBatch(tip, batch)
	if err != nil {
		return err
	}
	s.m.applied.Add(float64(len(changes)))
	if tip != nil && s.history > 0 && tip.Slot > s.history {
		// Only the committing goroutine calls this, so commits needs no lock.
		s.commits++
		if s.commits%pruneEvery == 0 {
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

// PreviewUtxoSetRollback implements ledger.UtxoSetRollbacker.
func (s *Set) PreviewUtxoSetRollback(
	point ocommon.Point,
) (ledger.UtxoEvent, error) {
	return s.RollbackPreview(point)
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
// is nil or declines, the set is rebuilt from the metadata store.
func (s *Set) Reconcile(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	resume ResumeFunc,
) error {
	ledgerTip, err := readLedgerTip(ctx, ldb)
	if err != nil {
		return err
	}
	setTip, ok, err := s.store.Tip()
	if err != nil {
		return fmt.Errorf("utxoset: read tip: %w", err)
	}
	stale, err := s.store.Stale()
	if err != nil {
		return fmt.Errorf("utxoset: read format: %w", err)
	}
	if stale {
		return s.rebuild(ctx, ldb, ledgerTip, ok, setTip)
	}
	if ok && samePoint(setTip, ledgerTip) {
		return nil
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
	return s.rebuild(ctx, ldb, ledgerTip, ok, setTip)
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
	ledgerTip ocommon.Point,
	hadTip bool,
	setTip ocommon.Point,
) error {
	if s.only && (ledgerTip.Slot != 0 || len(ledgerTip.Hash) != 0) {
		return fmt.Errorf(
			"utxoset: the live UTxO set at slot %d cannot be caught up to the ledger at slot %d and the metadata store holds no UTxO set to rebuild it from; resync",
			setTip.Slot,
			ledgerTip.Slot,
		)
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

func samePoint(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
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
