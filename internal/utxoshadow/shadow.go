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

// Package utxoshadow maintains a Badger copy of the live UTxO set from the
// ledger's ledger.utxo events and checks it against the metadata store. It is
// a diagnostic for the LedgerDB's UTxO set and does not affect validation.
//
// Every ledger.utxo event carries the outputs it adds and removes, a
// rollback's event included, so the copy follows the stream alone, the way a
// stateless consumer would. It is bootstrapped from the metadata store at the
// first event. Every change is self-checked: an output it removes must be
// live and an output it adds must not be. When a block's first transaction
// shows the previous block is complete, a sample of live outputs is compared
// with the metadata store as of that block's slot, which stays correct while
// the metadata store is ahead. A forward change out of order triggers a fresh
// bootstrap.
package utxoshadow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	defaultDiffWindowSlots = 129600
	defaultSampleEvery     = 500
	defaultSampleSize      = 64
	pruneEvery             = 100
	maxLoggedRefs          = 8
)

var (
	errShadowStopped         = errors.New("utxoshadow: stopped")
	errShadowNotBootstrapped = errors.New(
		"utxoshadow: not bootstrapped yet",
	)
)

// Config configures a Shadow.
type Config struct {
	// Dir is the directory the Badger copy lives in.
	Dir string
	// LedgerDB is the store the copy is bootstrapped from and compared with.
	LedgerDB ledgerdb.LedgerDB
	// EventBus delivers the ledger's block events.
	EventBus *event.EventBus
	// Logger receives mismatch reports. A nil Logger discards.
	Logger *slog.Logger
	// Registerer receives the shadow's metrics. Nil registers none.
	Registerer prometheus.Registerer
	// DiffWindowSlots is how far behind the tip block diffs are kept.
	// Zero uses the mainnet stability window.
	DiffWindowSlots uint64
	// SampleEvery is the number of completed blocks between comparisons.
	SampleEvery uint64
	// SampleSize is the number of outputs compared each time.
	SampleSize int
}

type metrics struct {
	reg                         prometheus.Registerer
	collectors                  []prometheus.Collector
	applied, undone, bootstraps prometheus.Counter
	missing, duplicate          prometheus.Counter
	sampled, sampleMismatch     prometheus.Counter
	samplesSkipped              prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	var collectors []prometheus.Collector
	c := func(name, help string) prometheus.Counter {
		ctr := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dingo",
			Subsystem: "utxo_shadow",
			Name:      name,
			Help:      help,
		})
		if reg != nil {
			reg.MustRegister(ctr)
			collectors = append(collectors, ctr)
		}
		return ctr
	}
	m := &metrics{
		applied: c(
			"transactions_applied_total",
			"Transactions applied to the shadow UTxO set.",
		),
		undone: c(
			"transactions_undone_total",
			"Transactions undone in the shadow UTxO set.",
		),
		bootstraps: c(
			"bootstraps_total",
			"Bootstraps of the shadow UTxO set from the metadata store.",
		),
		missing: c(
			"missing_inputs_total",
			"Spent inputs that were not live in the shadow set.",
		),
		duplicate: c(
			"duplicate_outputs_total",
			"Created outputs that were already live in the shadow set.",
		),
		sampled: c(
			"sampled_outputs_total",
			"Shadow outputs compared with the metadata store.",
		),
		sampleMismatch: c(
			"sample_mismatches_total",
			"Sampled outputs that differ from the metadata store.",
		),
		samplesSkipped: c(
			"samples_skipped_total",
			"Comparisons skipped because the metadata store could not be read.",
		),
	}
	m.reg = reg
	m.collectors = collectors
	return m
}

// unregister removes the collectors newMetrics registered, so a later Start
// on the same registry can register them again.
func (m *metrics) unregister() {
	for _, c := range m.collectors {
		m.reg.Unregister(c)
	}
}

// Shadow follows the ledger's UTxO events into a Badger UTxO set.
type Shadow struct {
	cfg     Config
	store   *badgerutxo.Store
	logger  *slog.Logger
	m       *metrics
	subID   event.EventSubscriberId
	rng     *rand.Rand
	mu      sync.Mutex
	stopped bool
	synced  bool
	anchor  ocommon.Point
	forward ocommon.Point
	block   ocommon.Point
	blocks  uint64
}

// Start opens the Badger copy and subscribes to the ledger's UTxO events.
func Start(cfg Config) (*Shadow, error) {
	if cfg.LedgerDB == nil || cfg.EventBus == nil || cfg.Dir == "" {
		return nil, errors.New(
			"utxoshadow: LedgerDB, EventBus and Dir are required",
		)
	}
	if cfg.DiffWindowSlots == 0 {
		cfg.DiffWindowSlots = defaultDiffWindowSlots
	}
	if cfg.SampleEvery == 0 {
		cfg.SampleEvery = defaultSampleEvery
	}
	if cfg.SampleSize == 0 {
		cfg.SampleSize = defaultSampleSize
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	store, err := badgerutxo.Open(cfg.Dir)
	if err != nil {
		return nil, err
	}
	s := &Shadow{
		cfg:    cfg,
		store:  store,
		logger: logger.With("component", "utxo_shadow"),
		m:      newMetrics(cfg.Registerer),
		rng:    sampleRand(),
	}
	// A detached subscription would silently skip changes and leave the copy
	// stale with no signal to rebuild, so a slow Badger write must stall
	// delivery instead.
	s.subID = cfg.EventBus.SubscribeFuncWithBufferPolicy(
		ledger.UtxoEventType,
		event.EventQueueSize,
		event.SubscriberBackpressureBlock,
		s.handle,
	)
	return s, nil
}

// Stop unsubscribes, closes the Badger copy and unregisters the metrics.
func (s *Shadow) Stop() error {
	s.cfg.EventBus.Unsubscribe(ledger.UtxoEventType, s.subID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	// Unsubscribe does not wait for a handler already dispatched; it sees
	// stopped and leaves the closed store alone.
	s.stopped = true
	s.m.unregister()
	return s.store.Close()
}

func (s *Shadow) handle(evt event.Event) {
	ue, ok := evt.Data.(ledger.UtxoEvent)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	if err := s.process(ue); err != nil {
		s.logger.Warn("shadow UTxO set out of sync, will bootstrap again",
			"slot", ue.Point.Slot, "cause", ue.Cause, "error", err)
		s.synced = false
	}
}

// Bootstrap rebuilds the copy from the metadata store now, instead of at the
// next event.
func (s *Shadow) Bootstrap() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bootstrap(); err != nil {
		return err
	}
	s.synced = true
	s.block = ocommon.Point{}
	return nil
}

// Applied returns the number of forward changes applied to the copy.
func (s *Shadow) Applied() float64 {
	return testutilValue(s.m.applied)
}

// Disagreements returns the number of removed outputs that were not live and
// added outputs that already were, across every applied change.
func (s *Shadow) Disagreements() float64 {
	return testutilValue(s.m.missing) + testutilValue(s.m.duplicate)
}

// Rollbacks returns the number of rollbacks applied to the copy.
func (s *Shadow) Rollbacks() float64 {
	return testutilValue(s.m.undone)
}

// sampleRand returns the generator that picks comparison samples. Sampling
// needs no cryptographic strength.
func sampleRand() *rand.Rand {
	//nolint:gosec // sampling only
	return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
}

func samePoint(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

func (s *Shadow) process(ue ledger.UtxoEvent) error {
	if !s.synced {
		if err := s.bootstrap(); err != nil {
			return err
		}
		s.synced = true
		s.block = ocommon.Point{}
	}
	if ue.Cause == ledger.UtxoChangeRollback {
		// The event carries the outputs the rollback restores, so it applies
		// even below the bootstrap point. Changes after the rollback point
		// are new from here on and must not be skipped.
		if ue.Point.Slot < s.anchor.Slot {
			s.anchor = ue.Point
		}
		s.block = ocommon.Point{}
		s.forward = ue.Point
		return s.apply(ue, s.m.undone)
	}
	if ue.Point.Slot <= s.anchor.Slot {
		return nil
	}
	if ue.Point.Slot < s.forward.Slot {
		return fmt.Errorf(
			"change at slot %d precedes the last change at slot %d",
			ue.Point.Slot, s.forward.Slot,
		)
	}
	s.forward = ue.Point
	if ue.Cause == ledger.UtxoChangeTransaction &&
		!samePoint(ue.Point, s.block) {
		if s.block.Slot != 0 {
			s.blocks++
			if s.blocks%s.cfg.SampleEvery == 0 {
				s.compare(s.block.Slot)
			}
			if s.blocks%pruneEvery == 0 {
				s.prune(s.block.Slot)
			}
		}
		s.block = ue.Point
	}
	return s.apply(ue, s.m.applied)
}

// prune drops journal records older than the diff window behind slot. A
// failure only delays pruning; the copy itself is unaffected.
func (s *Shadow) prune(slot uint64) {
	if slot <= s.cfg.DiffWindowSlots {
		return
	}
	if err := s.store.PruneJournal(slot - s.cfg.DiffWindowSlots); err != nil {
		s.logger.Warn("pruning the shadow UTxO set's journal failed",
			"slot", slot, "error", err)
	}
}

func (s *Shadow) bootstrap() error {
	ctx := context.Background()
	ldb := s.cfg.LedgerDB
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	tip, err := ldb.GetTip(txn)
	if err != nil {
		return fmt.Errorf("read tip for bootstrap: %w", err)
	}
	point := ocommon.Point{Slot: tip.Point.Slot, Hash: tip.Point.Hash}
	err = s.store.Bootstrap(
		point,
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
		return err
	}
	s.anchor = point
	s.forward = point
	s.m.bootstraps.Inc()
	n, _ := s.store.Count()
	s.logger.Info("shadow UTxO set bootstrapped from the metadata store",
		"slot", point.Slot, "live_outputs", n)
	return nil
}

func (s *Shadow) apply(ue ledger.UtxoEvent, counter prometheus.Counter) error {
	added := make([]badgerutxo.Output, 0, len(ue.Added))
	for _, o := range ue.Added {
		added = append(added, badgerutxo.Output{
			Ref:  badgerutxo.NewRef(o.Ref.Hash, o.Ref.Idx),
			Cbor: o.Cbor,
		})
	}
	removed := make([]badgerutxo.Ref, 0, len(ue.Removed))
	for _, r := range ue.Removed {
		removed = append(removed, badgerutxo.NewRef(r.Hash, r.Idx))
	}
	res, err := s.store.Apply(ue.Point, added, removed)
	if err != nil {
		return err
	}
	counter.Inc()
	if len(res.Missing) > 0 || len(res.Duplicate) > 0 {
		s.m.missing.Add(float64(len(res.Missing)))
		s.m.duplicate.Add(float64(len(res.Duplicate)))
		s.logger.Warn("change disagrees with the shadow UTxO set",
			"slot", ue.Point.Slot,
			"cause", ue.Cause,
			"tx_index", ue.TxIndex,
			"missing_outputs", refStrings(res.Missing),
			"duplicate_outputs", refStrings(res.Duplicate))
	}
	return nil
}

// compare checks a sample of shadow outputs against the metadata store as of
// slot, the last block the shadow has fully applied. Reading as of that slot
// keeps blocks the metadata store has applied since from showing up as
// mismatches.
func (s *Shadow) compare(slot uint64) {
	sample, err := s.store.Sample(s.cfg.SampleSize, s.rng)
	if err != nil {
		s.logger.Warn("sample shadow UTxO set", "error", err)
		return
	}
	if len(sample) == 0 {
		return
	}
	ids := make([]models.UtxoId, 0, len(sample))
	for _, o := range sample {
		ids = append(ids, models.UtxoId{Hash: o.Ref[:32], Idx: refIndex(o.Ref)})
	}
	got, err := s.cfg.LedgerDB.UtxosByRefsAsOf(
		context.Background(),
		ids,
		slot,
		nil,
	)
	if err != nil {
		s.m.samplesSkipped.Inc()
		s.logger.Warn("read metadata store for comparison", "error", err)
		return
	}
	byRef := make(map[badgerutxo.Ref][]byte, len(got))
	for _, u := range got {
		byRef[badgerutxo.NewRef(u.TxId, u.OutputIdx)] = u.Cbor
	}
	var mismatched []string
	for _, o := range sample {
		c, ok := byRef[o.Ref]
		switch {
		case !ok:
			mismatched = append(
				mismatched,
				refString(o.Ref)+" not live in metadata store",
			)
		case !bytes.Equal(c, o.Cbor):
			mismatched = append(mismatched, refString(o.Ref)+" CBOR differs")
		}
	}
	s.m.sampled.Add(float64(len(sample)))
	if len(mismatched) > 0 {
		s.m.sampleMismatch.Add(float64(len(mismatched)))
		if len(mismatched) > maxLoggedRefs {
			mismatched = mismatched[:maxLoggedRefs]
		}
		s.logger.Warn("shadow UTxO set differs from the metadata store",
			"slot", slot, "outputs", mismatched)
	}
}

// VerifyReport is the result of comparing the whole shadow set with the
// metadata store.
type VerifyReport struct {
	// Checked is the number of live outputs in the metadata store.
	Checked int
	// MissingInShadow are live in the metadata store but not in the shadow.
	MissingInShadow []string
	// ExtraInShadow are live in the shadow but not in the metadata store.
	ExtraInShadow []string
	// CborDiffers are live in both with different CBOR.
	CborDiffers []string
}

// Clean reports whether the two sets are identical.
func (r VerifyReport) Clean() bool {
	return len(r.MissingInShadow) == 0 && len(r.ExtraInShadow) == 0 &&
		len(r.CborDiffers) == 0
}

// VerifyAll compares the whole shadow set with the metadata store's current
// live set. It is meaningful only while no block is being applied, since the
// two are read at different moments.
func (s *Shadow) VerifyAll(ctx context.Context) (VerifyReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return VerifyReport{}, errShadowStopped
	}
	if !s.synced {
		return VerifyReport{}, errShadowNotBootstrapped
	}
	shadow := map[badgerutxo.Ref][]byte{}
	if err := s.store.Each(func(o badgerutxo.Output) error {
		shadow[o.Ref] = o.Cbor
		return nil
	}); err != nil {
		return VerifyReport{}, err
	}
	var rep VerifyReport
	ldb := s.cfg.LedgerDB
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	err := ldb.IterateLiveUtxos(ctx, txn, func(u *models.Utxo) error {
		rep.Checked++
		r := badgerutxo.NewRef(u.TxId, u.OutputIdx)
		c, ok := shadow[r]
		switch {
		case !ok:
			rep.MissingInShadow = append(rep.MissingInShadow, refString(r))
		case !bytes.Equal(c, u.Cbor):
			rep.CborDiffers = append(rep.CborDiffers, refString(r))
		}
		delete(shadow, r)
		return nil
	})
	if err != nil {
		return VerifyReport{}, err
	}
	for r := range shadow {
		rep.ExtraInShadow = append(rep.ExtraInShadow, refString(r))
	}
	return rep, nil
}

func testutilValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func refIndex(r badgerutxo.Ref) uint32 {
	return uint32(
		r[32],
	)<<24 | uint32(
		r[33],
	)<<16 | uint32(
		r[34],
	)<<8 | uint32(
		r[35],
	)
}

func refString(r badgerutxo.Ref) string {
	return fmt.Sprintf("%x#%d", r[:32], refIndex(r))
}

func refStrings(refs []badgerutxo.Ref) []string {
	out := make([]string, 0, min(len(refs), maxLoggedRefs))
	for i, r := range refs {
		if i == maxLoggedRefs {
			break
		}
		out = append(out, refString(r))
	}
	return out
}
