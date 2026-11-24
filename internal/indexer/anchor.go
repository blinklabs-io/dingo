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

package indexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrAnchorMismatch is returned when the IndexerDB's rows at the Mithril
// anchor disagree with the ledger state the snapshot imported.
var ErrAnchorMismatch = errors.New(
	"indexer: rows at the anchor do not match the imported ledger state",
)

// ErrAnchorUnserved is returned when the ledger no longer holds the state a
// snapshot import established at the anchor in its metadata store, so an empty
// IndexerDB cannot take it over.
var ErrAnchorUnserved = errors.New(
	"indexer: the ledger has drained the imported account state the " +
		"IndexerDB takes over at the Mithril anchor",
)

// AnchorChain is an optional ChainSource capability: the Mithril anchor the
// ledger's state was imported at, when there is one.
type AnchorChain interface {
	// Anchor returns the anchor block. found is false for a chain that was
	// not bootstrapped from a snapshot.
	Anchor(ctx context.Context) (anchor indexerdb.Tip, found bool, err error)
}

// Anchor returns the Mithril anchor the ledger database recorded.
func (s *DatabaseSource) Anchor(
	ctx context.Context,
) (indexerdb.Tip, bool, error) {
	slot, err := s.db.ImportedLedgerStateBoundarySlotStrict(nil)
	if err != nil {
		return indexerdb.Tip{}, false, err
	}
	if slot == 0 {
		return indexerdb.Tip{}, false, nil
	}
	raw, err := s.db.ImportedLedgerStateBoundaryHash()
	if err != nil {
		return indexerdb.Tip{}, false, fmt.Errorf(
			"read Mithril anchor hash: %w",
			err,
		)
	}
	hash, err := hex.DecodeString(raw)
	if err != nil || len(hash) == 0 {
		return indexerdb.Tip{}, false, fmt.Errorf(
			"the Mithril anchor at slot %d has no usable block hash %q",
			slot,
			raw,
		)
	}
	blk, err := database.BlockByHash(ctx, s.db, hash)
	if err != nil {
		return indexerdb.Tip{}, false, fmt.Errorf(
			"load Mithril anchor block at slot %d: %w", slot, err,
		)
	}
	if blk.Slot != slot {
		return indexerdb.Tip{}, false, fmt.Errorf(
			"the Mithril anchor hash is at slot %d, recorded slot is %d",
			blk.Slot,
			slot,
		)
	}
	return indexerdb.Tip{
		Slot:        blk.Slot,
		BlockNumber: blk.Number,
		Hash:        blk.Hash,
	}, true, nil
}

// AnchorFacts is an optional FactsSource capability: taking over the account
// state a snapshot import established, and checking the replayed history
// against it.
type AnchorFacts interface {
	// CopyAnchorState records, inside dst, the import baseline and the
	// anchor-time reward balance of every account the snapshot imported.
	CopyAnchorState(ctx context.Context, dst *database.Txn, anchor uint64) error
	// RequireAnchorState reports ErrAnchorUnserved when the ledger cannot
	// serve the imported accounts and UTxO set the anchor takes over.
	RequireAnchorState(ctx context.Context) error
	// CopyAnchorOpCerts records, inside dst, the op-cert counter of each pool
	// the snapshot import certified at the anchor, dated at the anchor slot as
	// the import dates them.
	CopyAnchorOpCerts(ctx context.Context, dst *database.Txn, anchor uint64) error
	// VerifyAnchor checks the IndexerDB's rows at the anchor against the
	// ledger's imported state. replayedTransactions is how many transactions
	// the replay indexed.
	VerifyAnchor(
		ctx context.Context,
		anchor uint64,
		replayedTransactions uint64,
		captured *metadata.UtxoSetDigest,
	) error
	// CaptureAnchorDigest digests the ledger's live UTxO set at the anchor,
	// outputs included, for VerifyAnchor to compare against once the replay
	// reaches the anchor.
	CaptureAnchorDigest(
		ctx context.Context,
		anchor uint64,
	) (metadata.UtxoSetDigest, error)
}

// outputResolver reads an output's stored CBOR through db.
func outputResolver(
	ctx context.Context,
	db *database.Database,
) metadata.UtxoOutputResolver {
	return func(txHash []byte, idx uint32) ([]byte, error) {
		return db.ResolveUtxoCborWithRecovery(ctx, txHash, idx, nil)
	}
}

// CaptureAnchorDigest implements AnchorFacts. The ledger can rebuild its set
// at the anchor only while its journal reaches back that far, so the digest
// is taken when the anchor is recorded and kept with it.
func (f *MetadataFacts) CaptureAnchorDigest(
	ctx context.Context,
	anchor uint64,
) (metadata.UtxoSetDigest, error) {
	digest, err := f.ledger.AnchorUtxoDigest(ctx, anchor)
	if err != nil {
		return metadata.UtxoSetDigest{}, fmt.Errorf(
			"digest ledger UTxO set: %w", err,
		)
	}
	return digest, nil
}

var _ AnchorFacts = (*MetadataFacts)(nil)

type credential struct {
	key string
	tag uint8
}

// CopyAnchorState implements AnchorFacts. The balance an imported account had
// at the anchor is not recorded anywhere but the account row, which has moved
// on by the time the replay reaches the anchor. It is the balance now less the
// credits journaled after the anchor plus the withdrawals journaled after it.
func (f *MetadataFacts) CopyAnchorState(
	ctx context.Context,
	dst *database.Txn,
	anchor uint64,
) error {
	dstStore, ok := f.dst.Metadata().(metadata.AnchorStateStore)
	if !ok {
		return errors.New(
			"indexer: indexer metadata store cannot import anchor state",
		)
	}
	// The journal and the balances are read at one point of the ledger, which
	// keeps moving while the writer runs.
	source := metadata.AnchorStateSource(f.ledger)
	if viewer, ok := source.(metadata.AnchorStateViewer); ok {
		view, release, err := viewer.AnchorStateView(ctx)
		if err != nil {
			return err
		}
		defer release()
		source = view
	}
	// Signed: credits add and withdrawals subtract, and a total that would
	// take a balance below zero means the journal and the balance disagree.
	moved := make(map[credential]int64)
	note := func(e metadata.RewardJournalEntry) error {
		if e.Amount > uint64(1<<62) {
			return fmt.Errorf(
				"indexer: reward journal amount %d out of range",
				e.Amount,
			)
		}
		c := credential{key: string(e.StakingKey), tag: e.CredentialTag}
		if e.Withdrawal {
			moved[c] -= int64(e.Amount) //nolint:gosec // bounded above
		} else {
			moved[c] += int64(e.Amount) //nolint:gosec // bounded above
		}
		return nil
	}
	if err := source.EachAnchorRewardJournal(ctx, anchor, note); err != nil {
		return err
	}
	var cursor []byte
	for {
		accounts, next, err := source.AnchorImportedAccounts(
			ctx, anchor, cursor, factPageRows,
		)
		if err != nil {
			return err
		}
		for i := range accounts {
			a := &accounts[i]
			delta := moved[credential{key: string(a.StakingKey), tag: a.CredentialTag}]
			if a.Reward > math.MaxInt64 {
				return fmt.Errorf(
					"indexer: account %x reward %d exceeds the int64 range",
					a.StakingKey,
					a.Reward,
				)
			}
			bal := int64(a.Reward)
			if bal-delta < 0 {
				return fmt.Errorf(
					"indexer: account %x reward %d cannot undo journal movement %d",
					a.StakingKey,
					a.Reward,
					delta,
				)
			}
			a.Reward = types.Uint64(bal - delta) //nolint:gosec // non-negative
		}
		if err := dstStore.WriteImportedAccounts(accounts, dst.Metadata()); err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		cursor = next
	}
}

// RequireAnchorState implements AnchorFacts.
func (f *MetadataFacts) RequireAnchorState(ctx context.Context) error {
	if err := f.ledger.AnchorStateReady(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrAnchorUnserved, err)
	}
	return nil
}

// CopyAnchorOpCerts implements AnchorFacts. Block replay writes one row per
// block a pool issued; the counters a snapshot import certified are not
// blocks and are written at the anchor slot, as the import writes them.
func (f *MetadataFacts) CopyAnchorOpCerts(
	ctx context.Context,
	dst *database.Txn,
	anchor uint64,
) error {
	counters, err := f.ledger.AnchorOpCertCounters(ctx, anchor)
	if err != nil {
		return err
	}
	for _, c := range counters {
		if len(c.PoolKeyHash) != len(lcommon.PoolKeyHash{}) {
			return fmt.Errorf(
				"indexer: certified op-cert pool key has length %d, expected 28",
				len(c.PoolKeyHash),
			)
		}
		if err := f.dst.Metadata().UpdatePoolOpCertSequence(
			lcommon.PoolKeyHash(c.PoolKeyHash), c.Sequence, anchor,
			dst.Metadata(),
		); err != nil {
			return fmt.Errorf("store certified op-cert counter: %w", err)
		}
	}
	return nil
}

// VerifyAnchor implements AnchorFacts.
func (f *MetadataFacts) VerifyAnchor(
	ctx context.Context,
	anchor uint64,
	replayedTransactions uint64,
	captured *metadata.UtxoSetDigest,
) error {
	dstStore, ok := f.dst.Metadata().(metadata.AnchorStateStore)
	if !ok {
		return errors.New(
			"indexer: indexer metadata store cannot digest its UTxO set",
		)
	}
	applied, err := dstStore.AppliedTransactionCount(ctx, nil)
	if err != nil {
		return err
	}
	if applied != replayedTransactions {
		return fmt.Errorf(
			"%w: %d applied transactions, replay indexed %d",
			ErrAnchorMismatch, applied, replayedTransactions,
		)
	}
	if captured == nil {
		// No digest was captured when the anchor was recorded, and the
		// ledger's journal may no longer reach back to it.
		slog.Warn(
			"skipping the anchor UTxO set comparison: no digest of the "+
				"ledger's UTxO set at the anchor was captured",
			"component", "indexer", "anchor_slot", anchor,
		)
		return nil
	}
	want := *captured
	got, err := dstStore.LiveUtxoDigestWithOutputs(
		ctx, anchor, outputResolver(ctx, f.dst), nil,
	)
	if err != nil {
		return fmt.Errorf("digest indexer UTxO set: %w", err)
	}
	if want.Count != got.Count || !bytes.Equal(want.Digest[:], got.Digest[:]) {
		return fmt.Errorf(
			"%w: ledger UTxO set has %d outputs (%x), indexer has %d (%x)",
			ErrAnchorMismatch,
			want.Count,
			want.Digest[:8],
			got.Count,
			got.Digest[:8],
		)
	}
	return nil
}

// TransactionCounter is an optional BlockIndexer capability: how many
// transactions the last IndexBlocks call applied.
type TransactionCounter interface {
	LastIndexedTransactions() uint64
}

// AnchorRestorer is an optional BlockIndexer capability: applying the account
// state a snapshot import established, which replaying certificates cannot
// reproduce, once the replay has reached the anchor.
type AnchorRestorer interface {
	RestoreAnchor(
		ctx context.Context,
		txn *database.Txn,
		anchor indexerdb.Tip,
	) error
}

func (w *Writer) countReplayed(txn *database.Txn) error {
	counter, ok := w.idx.(TransactionCounter)
	if !ok {
		return errors.New(
			"indexer: block indexer cannot count the transactions it applied",
		)
	}
	return w.store.AddReplayTransactions(counter.LastIndexedTransactions(), txn)
}

// countSeeded adds the transactions the genesis seed wrote to the replay's
// count: the first batch's transaction holds nothing else yet.
func (w *Writer) countSeeded(ctx context.Context, txn *database.Txn) error {
	store, ok := w.store.Database().Metadata().(metadata.AnchorStateStore)
	if !ok {
		return errors.New(
			"indexer: indexer metadata store cannot count transactions",
		)
	}
	n, err := store.AppliedTransactionCount(ctx, txn.Metadata())
	if err != nil {
		return err
	}
	return w.store.AddReplayTransactions(n, txn)
}

// bootstrap brings the IndexerDB to the Mithril anchor of a chain imported
// from a snapshot, then returns so ordinary catch-up follows the ledger from
// there. It is a no-op for a chain from genesis and once the anchor is done.
//
// The anchor is recorded first, so readiness refuses to answer until the
// phases are finished. Blocks from the origin to the anchor are indexed with
// the deferred indexes dropped; the indexes are then built, the rows are
// checked against the imported ledger state, and the imported account state is
// applied together with the final phase marker. Each phase commits its own
// marker, so a stop in any of them resumes in that phase.
func (w *Writer) bootstrap(ctx context.Context) error {
	phase, anchor, err := w.enter(ctx)
	if err != nil || phase == indexerdb.PhaseNone ||
		phase == indexerdb.PhaseDone {
		return err
	}
	if phase == indexerdb.PhaseReplay {
		if err := w.replay(ctx, anchor); err != nil {
			return err
		}
		if err := w.setPhase(ctx, indexerdb.PhaseIndex); err != nil {
			return err
		}
	}
	return w.finishAnchor(ctx, anchor)
}

// Prepare records the Mithril anchor of a chain imported from a snapshot, so
// readiness refuses to answer before the writer's first pass, and does nothing
// else. It is called once the stores are open and before anything serves from
// the IndexerDB.
func (w *Writer) Prepare(ctx context.Context) error {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	_, _, err := w.enter(ctx)
	return err
}

// enter returns the phase the bootstrap stands in and its anchor, recording
// both when the chain has an anchor the store has not met yet.
func (w *Writer) enter(
	ctx context.Context,
) (indexerdb.BootstrapPhase, indexerdb.Tip, error) {
	ac, ok := w.src.(AnchorChain)
	if !ok {
		return indexerdb.PhaseNone, indexerdb.Tip{}, nil
	}
	phase, err := w.store.Bootstrap(nil)
	if err != nil {
		return phase, indexerdb.Tip{}, err
	}
	if phase == indexerdb.PhaseDone {
		return phase, indexerdb.Tip{}, nil
	}
	if phase != indexerdb.PhaseNone {
		anchor, err := w.store.Anchor(nil)
		return phase, anchor, err
	}
	anchor, found, err := ac.Anchor(ctx)
	if err != nil {
		return phase, anchor, fmt.Errorf("read Mithril anchor: %w", err)
	}
	if !found {
		return indexerdb.PhaseNone, indexerdb.Tip{}, nil
	}
	phase, err = w.recordAnchor(ctx, anchor)
	return phase, anchor, err
}

// recordAnchor records the anchor and the phase it starts in: a replay for an
// empty IndexerDB, or done for a store adopted with its history already
// indexed, which then only gates readiness on the anchor.
func (w *Writer) recordAnchor(
	ctx context.Context,
	anchor indexerdb.Tip,
) (indexerdb.BootstrapPhase, error) {
	tip, err := w.store.Tip(nil)
	if err != nil {
		return indexerdb.PhaseNone, err
	}
	phase := indexerdb.PhaseReplay
	if !tip.IsZero() {
		phase = indexerdb.PhaseDone
	}
	facts, hasFacts := w.facts.(AnchorFacts)
	if hasFacts && phase == indexerdb.PhaseReplay {
		if err := facts.RequireAnchorState(ctx); err != nil {
			return indexerdb.PhaseNone, err
		}
	}
	var captured *metadata.UtxoSetDigest
	if hasFacts && phase == indexerdb.PhaseReplay {
		var err error
		captured, err = w.captureAnchorDigest(ctx, facts, anchor.Slot)
		if err != nil {
			return indexerdb.PhaseNone, err
		}
	}
	txn := w.store.Database().Transaction(ctx, true)
	err = txn.Do(func(txn *database.Txn) error {
		if err := w.store.SetAnchor(anchor, txn); err != nil {
			return err
		}
		// The ledger's counters are read now, not once the replay reaches the
		// anchor: its live set keeps them only for a bounded window, and the
		// ledger moves on while the replay runs.
		if hasFacts && phase == indexerdb.PhaseReplay {
			if err := facts.CopyAnchorOpCerts(ctx, txn, anchor.Slot); err != nil {
				return fmt.Errorf("copy certified op-cert counters: %w", err)
			}
		}
		// A replay seeds genesis state while no boundary is recorded: the
		// seed reads a recorded boundary as a snapshot import. finishAnchor
		// records it once the replay is done.
		if phase != indexerdb.PhaseReplay {
			if err := w.store.SetTrustBoundary(anchor.Slot, txn); err != nil {
				return err
			}
		}
		if captured != nil {
			if err := w.store.SetAnchorDigest(
				captured.Count, captured.Digest, txn,
			); err != nil {
				return err
			}
		}
		return w.store.SetBootstrap(phase, txn)
	})
	if err != nil {
		return indexerdb.PhaseNone, fmt.Errorf("record Mithril anchor: %w", err)
	}
	return phase, nil
}

// captureAnchorDigest digests the ledger's UTxO set at the anchor. It returns
// nil when the ledger can no longer rebuild the anchor, which leaves the replay
// uncompared with it.
func (w *Writer) captureAnchorDigest(
	ctx context.Context,
	facts AnchorFacts,
	anchor uint64,
) (*metadata.UtxoSetDigest, error) {
	digest, err := facts.CaptureAnchorDigest(ctx, anchor)
	switch {
	case errors.Is(err, metadata.ErrAnchorDigestUnavailable):
		w.logger.Warn(
			"the anchor UTxO set comparison is skipped",
			"component", "indexer", "anchor_slot", anchor, "reason", err,
		)
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf(
			"capture the ledger UTxO digest at the anchor: %w", err,
		)
	}
	return &digest, nil
}

func (w *Writer) setPhase(
	ctx context.Context,
	phase indexerdb.BootstrapPhase,
) error {
	txn := w.store.Database().Transaction(ctx, true)
	return txn.Do(func(txn *database.Txn) error {
		return w.store.SetBootstrap(phase, txn)
	})
}

// replay indexes from the origin to the anchor block.
func (w *Writer) replay(ctx context.Context, anchor indexerdb.Tip) error {
	if w.dropIndexes != nil {
		if err := w.dropIndexes(ctx); err != nil {
			return fmt.Errorf("drop deferred indexes: %w", err)
		}
	}
	tip, err := w.store.Tip(nil)
	if err != nil {
		return err
	}
	if tip.IsZero() {
		if err := w.requireOrigin(ctx, anchor); err != nil {
			return err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := w.step(ctx, &anchor, false)
		if errors.Is(err, errForked) || errors.Is(err, errBatchShrunk) {
			continue
		}
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	tip, err = w.store.Tip(nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(tip.Hash, anchor.Hash) {
		return fmt.Errorf(
			"indexer: replay stopped at slot %d, before the Mithril anchor at slot %d",
			tip.Slot,
			anchor.Slot,
		)
	}
	return nil
}

// requireOrigin checks that the canonical chain starts at the chain's origin,
// which the replay seeds with genesis state: a first block whose parent is
// stored means the chain begins later and the seed would be wrong.
func (w *Writer) requireOrigin(
	ctx context.Context,
	anchor indexerdb.Tip,
) error {
	first, err := w.src.BlocksAfter(ctx, indexerdb.Tip{}, anchor.Slot, 1)
	if err != nil {
		return fmt.Errorf("read first block: %w", err)
	}
	if len(first) == 0 {
		return errors.New("indexer: no blocks at or before the Mithril anchor")
	}
	if len(first[0].PrevHash) == 0 {
		return nil
	}
	_, err = w.src.BlockByHash(ctx, first[0].PrevHash)
	if errors.Is(err, models.ErrBlockNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read parent of first block: %w", err)
	}
	return errors.New(
		"indexer: the canonical chain does not start at the chain origin",
	)
}

// finishAnchor builds the dropped indexes, checks the rows against the
// imported ledger state and applies that state's accounts.
func (w *Writer) finishAnchor(ctx context.Context, anchor indexerdb.Tip) error {
	start := time.Now()
	if w.buildIndexes != nil {
		if err := w.buildIndexes(ctx); err != nil {
			return fmt.Errorf("build deferred indexes at the anchor: %w", err)
		}
	}
	indexesBuilt := time.Now()
	facts, hasFacts := w.facts.(AnchorFacts)
	if hasFacts {
		replayed, err := w.store.ReplayTransactions(nil)
		if err != nil {
			return err
		}
		var captured *metadata.UtxoSetDigest
		count, digest, found, err := w.store.AnchorDigest(nil)
		if err != nil {
			return err
		}
		if found {
			captured = &metadata.UtxoSetDigest{Count: count, Digest: digest}
		}
		if err := facts.VerifyAnchor(
			ctx, anchor.Slot, replayed, captured,
		); err != nil {
			return err
		}
	}
	verified := time.Now()
	restorer, hasRestorer := w.idx.(AnchorRestorer)
	txn := w.store.Database().Transaction(ctx, true)
	err := txn.Do(func(txn *database.Txn) error {
		if hasFacts {
			if err := facts.CopyAnchorState(ctx, txn, anchor.Slot); err != nil {
				return fmt.Errorf("copy imported account state: %w", err)
			}
		}
		if err := w.store.SetTrustBoundary(anchor.Slot, txn); err != nil {
			return err
		}
		if hasRestorer {
			if err := restorer.RestoreAnchor(ctx, txn, anchor); err != nil {
				return fmt.Errorf("restore imported account state: %w", err)
			}
		}
		return w.store.SetBootstrap(indexerdb.PhaseDone, txn)
	})
	if err != nil {
		return fmt.Errorf("finish the anchor: %w", err)
	}
	w.logger.Info(
		"indexer reached the Mithril anchor",
		"slot", anchor.Slot, "block", anchor.BlockNumber,
		"build_indexes", indexesBuilt.Sub(start),
		"verify", verified.Sub(indexesBuilt),
		"copy_and_restore", time.Since(verified),
	)
	w.finalizePlannerStats(ctx)
	return nil
}

// finalizePlannerStats refreshes the IndexerDB's planner statistics once the
// anchor is reached. Statistics gathered while the backfill was still loading
// describe a fraction of the final rows, so API queries would plan against
// them until the next periodic refresh. A failure is logged, not returned:
// statistics only steer the planner, and the anchor is already durable.
func (w *Writer) finalizePlannerStats(ctx context.Context) {
	db := w.store.Database()
	if db == nil {
		return
	}
	start := time.Now()
	err := runFinalPlannerStats(ctx, db.Metadata())
	if errors.Is(err, errNoPlannerStats) {
		return
	}
	if err != nil {
		w.logger.Warn("indexer final planner statistics failed", "error", err)
		return
	}
	w.logger.Info(
		"indexer final planner statistics complete",
		"duration", time.Since(start),
	)
}

var errNoPlannerStats = errors.New("metadata store keeps no planner statistics")

// runFinalPlannerStats prefers the bounded incremental form, which samples
// every index where a full pass would read the whole IndexerDB, and falls back
// to the full refresh on a backend without one.
func runFinalPlannerStats(
	ctx context.Context,
	store metadata.MetadataStore,
) error {
	if updater, ok := store.(metadata.IncrementalPlannerStatsUpdater); ok {
		result, err := updater.OptimizePlannerStatsContext(
			ctx, types.PlannerStatsTriggerAnchor,
		)
		if err != nil || result.Supported {
			return err
		}
	}
	switch updater := store.(type) {
	case metadata.ContextPlannerStatsUpdater:
		return updater.UpdatePlannerStatsContext(ctx)
	case metadata.PlannerStatsUpdater:
		return updater.UpdatePlannerStats()
	default:
		return errNoPlannerStats
	}
}
