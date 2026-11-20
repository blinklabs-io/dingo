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
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// FactsSource supplies the ledger-derived rows block replay cannot
// reproduce: epochs, stake snapshots, reward inputs and outputs, pots,
// network state, enacted protocol parameters, governance outcomes, DRep
// expiry and the reward credits of accounts.
//
// The writer splits a batch at every epoch boundary the ledger crossed and,
// for each segment, calls CopyFacts before the segment's blocks and
// PatchFacts after them. Facts are keyed by their natural key, never by the
// ledger store's row ids.
type FactsSource interface {
	// Boundaries returns the epoch start slots in [from, through], ascending.
	Boundaries(ctx context.Context, from, through uint64) ([]uint64, error)
	// CopyFacts stores the rows dated in [from, through] that block replay
	// depends on or that happen at the segment start, and applies the reward
	// credits dated in the range, inside dst.
	CopyFacts(
		ctx context.Context,
		dst *database.Txn,
		from, through uint64,
	) error
	// PatchFacts applies, inside dst, the ledger's outcome columns onto the
	// rows block replay wrote for [from, through].
	PatchFacts(
		ctx context.Context,
		dst *database.Txn,
		from, through uint64,
	) error
	// RollbackFacts removes the facts dated after point inside dst.
	RollbackFacts(
		ctx context.Context,
		dst *database.Txn,
		point ocommon.Point,
	) error
}

// FactsRefresher copies facts completed after their slot was indexed, without
// replaying blocks or applying their withdrawals again.
type FactsRefresher interface {
	RefreshFacts(context.Context, *database.Txn, uint64) error
}

// maxCursorRestarts bounds how many times one table's copy starts again
// after the ledger loses its cursor.
const maxCursorRestarts = 3

// factPageRows bounds the rows read from the ledger store per statement.
const factPageRows = 5000

// MetadataFacts is the FactsSource over a ledger database's metadata store.
type MetadataFacts struct {
	src     *database.Database
	srcRows metadata.FactRowStore
	dstRows metadata.FactRowStore
	dst     *database.Database
	// anchorOps, when set, serves the op-cert counters certified at the
	// anchor in place of src's metadata store.
	anchorOps metadata.AnchorOpCertSource
	// anchorState, when set, serves the imported accounts, the reward journal
	// and the UTxO digest at the anchor in place of src's metadata store.
	anchorState metadata.AnchorStateSource
	// journal, when set, is the reader's reward journal retention: it is told
	// which records the committed batches have copied.
	journal JournalRetainer
	// sampled is the journal position read before the current batch's
	// credits, zero when none was taken.
	sampled uint64
	// through is the last slot the current batch copied facts for.
	through uint64
	// pending is the position of the latest committed batch that has not
	// been acknowledged.
	pending        bool
	pendingSeq     uint64
	pendingThrough uint64
}

// JournalRetainer is an optional capability of a FactRowReader whose reward
// journal is pruned as the reader acknowledges it.
type JournalRetainer interface {
	RewardJournalHigh() (uint64, error)
	AckRewardJournal(seq, throughSlot uint64) error
}

// FactsCommitted records that the batch just committed copied the reward
// credits up to the position sampled before it read them. The position is
// acknowledged by AckFacts, once the writer knows the commit is durable.
func (f *MetadataFacts) FactsCommitted() {
	f.pendingSeq, f.pendingThrough, f.pending = f.sampled, f.through, true
	f.sampled = 0
}

// AckFacts acknowledges to the ledger the reward credits the committed
// batches copied. A failed acknowledgement stays pending for the next call.
func (f *MetadataFacts) AckFacts() error {
	if !f.pending {
		return nil
	}
	if f.journal != nil {
		if err := f.journal.AckRewardJournal(f.pendingSeq, f.pendingThrough); err != nil {
			return err
		}
	}
	f.pending = false
	return nil
}

// NewMetadataFacts copies facts from src's metadata store into dst's. Both
// stores must implement metadata.FactRowStore.
func NewMetadataFacts(src, dst *database.Database) (*MetadataFacts, error) {
	srcRows, ok := src.Metadata().(metadata.FactRowStore)
	if !ok {
		return nil, errors.New(
			"indexer: ledger metadata store cannot export facts",
		)
	}
	dstRows, ok := dst.Metadata().(metadata.FactRowStore)
	if !ok {
		return nil, errors.New(
			"indexer: indexer metadata store cannot import facts",
		)
	}
	if _, ok := dst.Metadata().(metadata.RewardFoldStore); !ok {
		return nil, errors.New(
			"indexer: indexer metadata store cannot fold reward outputs",
		)
	}
	return &MetadataFacts{
		src:     src,
		srcRows: srcRows,
		dst:     dst,
		dstRows: dstRows,
	}, nil
}

// NewFactsOver is NewMetadataFacts reading the ledger's fact rows from rows
// instead of from src's metadata store, for a ledger whose families are
// drained from it. src still supplies the rows that stay in the store.
func NewFactsOver(
	src *database.Database,
	rows metadata.FactRowReader,
	dst *database.Database,
) (*MetadataFacts, error) {
	f, err := NewMetadataFacts(src, dst)
	if err != nil {
		return nil, err
	}
	f.srcRows = readOnlyFactRows{FactRowReader: rows}
	if ops, ok := rows.(metadata.AnchorOpCertSource); ok {
		f.anchorOps = ops
	}
	if st, ok := rows.(metadata.AnchorStateSource); ok {
		f.anchorState = st
	}
	if j, ok := rows.(JournalRetainer); ok {
		f.journal = j
	}
	return f, nil
}

// readOnlyFactRows lets a FactRowReader stand where the copy reads its source:
// the source is never written.
type readOnlyFactRows struct{ metadata.FactRowReader }

var errFactSourceReadOnly = errors.New("indexer: the fact source is read-only")

func (readOnlyFactRows) WriteFactRows(
	metadata.FactTable, metadata.FactRows, types.Txn,
) error {
	return errFactSourceReadOnly
}

func (readOnlyFactRows) DeleteFactRowsAfter(
	metadata.FactTable, uint64, types.Txn,
) error {
	return errFactSourceReadOnly
}

// pending returns the reader's pending-record capability,
// and reports ok false for a reader that keeps none.
func (r readOnlyFactRows) pending() (metadata.PendingStakeInputsReader, bool) {
	p, ok := r.FactRowReader.(metadata.PendingStakeInputsReader)
	return p, ok
}

var _ FactsSource = (*MetadataFacts)(nil)

// Boundaries implements FactsSource.
func (f *MetadataFacts) Boundaries(
	ctx context.Context,
	from, through uint64,
) ([]uint64, error) {
	var epochTable metadata.FactTable
	for _, t := range metadata.FactTables {
		if t.Name == "epoch" {
			epochTable = t
		}
	}
	var out []uint64
	var cursor int64
	for {
		rows, next, err := f.srcRows.ReadFactRows(
			ctx, epochTable, from, through, cursor, factPageRows, nil,
		)
		if err != nil {
			return nil, err
		}
		col := slices.Index(rows.Columns, "start_slot")
		if col < 0 {
			return nil, errors.New("indexer: epoch facts lack start_slot")
		}
		for _, r := range rows.Rows {
			slot, err := asUint64(r[col])
			if err != nil {
				return nil, err
			}
			out = append(out, slot)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// CopyFacts implements FactsSource.
func (f *MetadataFacts) CopyFacts(
	ctx context.Context,
	dst *database.Txn,
	from, through uint64,
) error {
	f.through = through
	if f.journal != nil && f.sampled == 0 {
		high, err := f.journal.RewardJournalHigh()
		if err != nil {
			return err
		}
		f.sampled = high
	}
	if err := f.copyDeferredStakeInputs(ctx, dst); err != nil {
		return err
	}
	rounds, err := f.finishedRewardRounds()
	if err != nil {
		return err
	}
	for _, table := range metadata.FactTables {
		if table.AfterBlocks() {
			continue
		}
		if err := f.copyTable(ctx, dst, table, from, through); err != nil {
			return err
		}
	}
	if err := f.copyLateRewardOutputs(
		ctx, dst, rounds, from, through,
	); err != nil {
		return err
	}
	return f.applyRewardCredits(ctx, dst, from, through)
}

// PatchFacts implements FactsSource.
func (f *MetadataFacts) PatchFacts(
	ctx context.Context,
	dst *database.Txn,
	from, through uint64,
) error {
	for _, table := range metadata.FactTables {
		if !table.AfterBlocks() {
			continue
		}
		if err := f.copyTable(ctx, dst, table, from, through); err != nil {
			return err
		}
	}
	if store, ok := f.dst.Metadata().(metadata.DrepExpiryAsOfStore); ok {
		return store.ApplyDrepExpiryAsOf(through, dst.Metadata())
	}
	return nil
}

// RollbackFacts implements FactsSource.
func (f *MetadataFacts) RollbackFacts(
	_ context.Context,
	dst *database.Txn,
	point ocommon.Point,
) error {
	for _, table := range metadata.FactTables {
		if err := f.dstRows.DeleteFactRowsAfter(
			table, point.Slot, dst.Metadata(),
		); err != nil {
			return err
		}
	}
	// Journal ids of the source may be reused after its own rollback, so no
	// read position survives one.
	if err := f.clearCreditMarks(dst); err != nil {
		return err
	}
	if err := f.dropCopiedRewardOutputsAfter(dst, point.Slot); err != nil {
		return err
	}
	return f.dropDeferredStakeInputsAfter(dst, point.Slot)
}

func (f *MetadataFacts) copyTable(
	ctx context.Context,
	dst *database.Txn,
	table metadata.FactTable,
	from, through uint64,
) error {
	// The ledger writes a snapshot's stake inputs after the boundary commits
	// and lists the snapshot as pending until the last chunk is in. The
	// pending set is read before the rows: an epoch absent from it was
	// complete when read, so the rows read afterwards are whole.
	var skip map[uint64]struct{}
	if table.Name == stakeInputTable.Name {
		pending, err := f.pendingStakeInputEpochs()
		if err != nil {
			return err
		}
		skip = pending
	}
	var cursor int64
	restarts := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, next, err := f.srcRows.ReadFactRows(
			ctx, table, from, through, cursor, factPageRows, nil,
		)
		if errors.Is(err, metadata.ErrFactCursorLost) && restarts < maxCursorRestarts {
			// The ledger lost its place in this export. The rows already
			// written are keyed, so reading the table again from the start
			// writes them over themselves and then the rest.
			restarts++
			cursor = 0
			continue
		}
		if err != nil {
			return err
		}
		if len(skip) > 0 {
			deferred, err := dropPendingEpochs(&rows, skip)
			if err != nil {
				return err
			}
			if err := f.deferStakeInputs(dst, deferred); err != nil {
				return err
			}
		}
		if err := f.dstRows.WriteFactRows(table, rows, dst.Metadata()); err != nil {
			return err
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// applyRewardCredits replays the ledger's reward-credit journal through the
// store's idempotent credit path, so each account balance moves exactly once
// per credit and a rollback reverts it with the other account changes.
//
// The ledger journals a credited round's credits lazily, when a credential is
// next touched, under the round's boundary slot. A credit can therefore be
// journaled after the copy has passed that slot, so the rounds that still
// hold unfolded outputs here are read again at their boundary slot.
func (f *MetadataFacts) applyRewardCredits(
	ctx context.Context,
	dst *database.Txn,
	from, through uint64,
) error {
	if _, err := f.applyJournalRange(ctx, dst, from, through, 0); err != nil {
		return err
	}
	folds, ok := f.dst.Metadata().(metadata.RewardFoldStore)
	if !ok {
		return errors.New("indexer: store cannot fold reward outputs")
	}
	rounds, err := folds.UnfoldedRewardCreditRounds(ctx, dst.Metadata())
	if err != nil {
		return err
	}
	for _, round := range rounds {
		if round.BoundarySlot >= from {
			continue
		}
		// Only credits journaled since the last read of this round are new.
		// The first read of a round starts at zero, since the ids of the
		// credits the range pass already applied are not known.
		key := creditMarkPrefix + strconv.FormatUint(round.BoundarySlot, 10)
		mark, err := f.creditMark(dst, key)
		if err != nil {
			return err
		}
		last, err := f.applyJournalRange(
			ctx, dst, round.BoundarySlot, round.BoundarySlot, mark,
		)
		if err != nil {
			return err
		}
		if err := f.dst.SetSyncState(
			key, strconv.FormatInt(last, 10), dst,
		); err != nil {
			return err
		}
	}
	return nil
}

// creditMarkPrefix prefixes the sync_state keys holding, per credited round's
// boundary slot, the highest ledger journal id the copy has read for it.
const creditMarkPrefix = "indexer_credit_mark:" //nolint:gosec // a key prefix, not a credential

func (f *MetadataFacts) creditMark(
	dst *database.Txn,
	key string,
) (int64, error) {
	v, err := f.dst.GetSyncState(key, dst)
	if err != nil || v == "" {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func (f *MetadataFacts) clearCreditMarks(dst *database.Txn) error {
	keys, err := f.dst.ListSyncStateKeysByPrefix(creditMarkPrefix, dst)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := f.dst.DeleteSyncState(k, dst); err != nil {
			return err
		}
	}
	return nil
}

func (f *MetadataFacts) applyJournalRange(
	ctx context.Context,
	dst *database.Txn,
	from, through uint64,
	after int64,
) (int64, error) {
	table := metadata.AccountRewardCreditTable
	store := f.dst.Metadata()
	folds, ok := store.(metadata.RewardFoldStore)
	if !ok {
		return 0, errors.New("indexer: store cannot fold reward outputs")
	}
	cursor, last := after, after
	for {
		rows, next, err := f.srcRows.ReadFactRows(
			ctx, table, from, through, cursor, factPageRows, nil,
		)
		if err != nil {
			return 0, err
		}
		last = max(last, rows.LastID)
		col := func(name string) int { return slices.Index(rows.Columns, name) }
		keyCol, tagCol, hashCol := col(
			"staking_key",
		), col(
			"credential_tag",
		), col(
			"tx_hash",
		)
		amountCol, slotCol, postCol := col(
			"amount",
		), col(
			"added_slot",
		), col(
			"post_snapshot",
		)
		if len(rows.Rows) > 0 &&
			slices.Min(
				[]int{keyCol, tagCol, hashCol, amountCol, slotCol, postCol},
			) < 0 {
			return 0, errors.New(
				"indexer: reward journal rows lack expected columns",
			)
		}
		var batch, applied []models.AccountRewardCredit
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			err := store.AddAccountRewardsByCredential(batch, dst.Metadata())
			batch = batch[:0]
			return err
		}
		for _, r := range rows.Rows {
			amount, err := asUint64(r[amountCol])
			if err != nil {
				return 0, err
			}
			slot, err := asUint64(r[slotCol])
			if err != nil {
				return 0, err
			}
			tag, err := asUint64(r[tagCol])
			if err != nil {
				return 0, err
			}
			if tag > math.MaxUint8 {
				return 0, fmt.Errorf(
					"indexer: credential tag %d out of range",
					tag,
				)
			}
			key, hash := asBytes(r[keyCol]), asBytes(r[hashCol])
			credit := models.AccountRewardCredit{
				StakingKey: key, SourceHash: hash, Amount: amount,
				Slot: slot, CredentialTag: uint8(tag),
			}
			applied = append(applied, credit)
			if asBool(r[postCol]) {
				if err := flush(); err != nil {
					return 0, err
				}
				if err := store.AddPostSnapshotAccountRewardByCredential(
					uint8(tag), key, amount, slot, hash, dst.Metadata(),
				); err != nil {
					return 0, err
				}
				continue
			}
			batch = append(batch, credit)
		}
		if err := flush(); err != nil {
			return 0, err
		}
		// A credit now in the account must stop counting as a pending
		// output, whichever side of the copy the ledger folded it on.
		if err := folds.FoldRewardOutputsForCredits(
			applied, dst.Metadata(),
		); err != nil {
			return 0, err
		}
		if next == 0 {
			return last, nil
		}
		cursor = next
	}
}

func asUint64(v any) (uint64, error) {
	switch x := v.(type) {
	case int64:
		if x < 0 {
			return 0, fmt.Errorf("indexer: negative value %d", x)
		}
		return uint64(x), nil
	case uint64:
		return x, nil
	case string:
		return strconv.ParseUint(x, 10, 64)
	case []byte:
		return strconv.ParseUint(string(x), 10, 64)
	case nil:
		return 0, errors.New("indexer: null value")
	}
	return 0, fmt.Errorf("indexer: cannot read %T as unsigned", v)
}

func asBytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	}
	return nil
}

func asBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case []byte:
		return len(x) > 0 && x[0] != '0' && x[0] != 0
	}
	return false
}
