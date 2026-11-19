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
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var (
	_ metadata.AnchorStateSource = (*LedgerState)(nil)
	_ metadata.AnchorStateViewer = (*LedgerState)(nil)
	_ metadata.AnchorStateSource = (*anchorView)(nil)
)

// ImportBaselinePager serves import baselines in credential order from a live
// UTxO set that keeps their delegations.
type ImportBaselinePager interface {
	// ImportBaselinesAfter returns at most limit baselines after the
	// credential after (nil to start) and the credential to continue from,
	// nil at the end.
	ImportBaselinesAfter(
		after *models.StakeCredentialRef,
		limit int,
	) ([]ImportBaseline, *models.StakeCredentialRef, error)
	// ImportBaselineDelegationsLost reports baselines whose delegations the
	// set never held and the metadata store drained.
	ImportBaselineDelegationsLost() (bool, error)
}

// RewardRecordStreamer serves the reward records a live UTxO set holds
// without collecting them.
type RewardRecordStreamer interface {
	EachRewardRecordIn(from, through uint64, fn func(RewardRecord) error) error
}

// anchorFamilyDrained reports whether the metadata store no longer holds f's
// rows. The writer asks before the ledger has started, when the ledger has not
// yet published the families the database records as drained, so the database
// is asked as well.
func (ls *LedgerState) anchorFamilyDrained(
	ctx context.Context,
	f nodesettings.Family,
) (bool, error) {
	if ls.familyDrained(f) {
		return true, nil
	}
	mask, err := ls.db.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return false, fmt.Errorf("read drained ledger families: %w", err)
	}
	return mask.Has(f), nil
}

// anchorStore is the ledger metadata store's anchor capability.
func (ls *LedgerState) anchorStore() (metadata.AnchorStateStore, error) {
	store, ok := ls.db.Metadata().(metadata.AnchorStateStore)
	if !ok {
		return nil, errors.New(
			"ledger metadata store cannot export anchor state",
		)
	}
	return store, nil
}

// AnchorStateReady implements metadata.AnchorStateSource. A drained accounts
// family is served from the set's import baselines and reward records, which
// the set holds only when it loaded them before the metadata store drained
// the rows they come from.
func (ls *LedgerState) AnchorStateReady(ctx context.Context) error {
	drained, err := ls.anchorFamilyDrained(ctx, nodesettings.FamilyAccounts)
	if err != nil || !drained {
		return err
	}
	if ls.accountReader(nil) == nil {
		return drainedUnserved(nodesettings.FamilyAccounts)
	}
	set := ls.readSet(nil)
	pager, ok := set.(ImportBaselinePager)
	if !ok {
		return drainedUnserved(nodesettings.FamilyAccounts)
	}
	lost, err := pager.ImportBaselineDelegationsLost()
	if err != nil {
		return err
	}
	if lost {
		return fmt.Errorf(
			"%w: the import baselines were written before the set kept the "+
				"delegations they record, and the rows were drained first",
			drainedUnserved(nodesettings.FamilyAccounts),
		)
	}
	keeper, ok := set.(RewardRecordRetainer)
	if !ok {
		return drainedUnserved(nodesettings.FamilyAccounts)
	}
	lost, err = keeper.RewardRecordsLost()
	if err != nil {
		return err
	}
	if lost {
		return fmt.Errorf(
			"%w: the reward journal was drained before the set held it, and "+
				"its entries before then are in neither store",
			drainedUnserved(nodesettings.FamilyAccounts),
		)
	}
	return nil
}

// anchorView is where an anchor read takes its rows from. The live view reads
// the ledger as it moves, one lock hold per call; a pinned view reads a fixed
// point and decides once whether the accounts are in the set or the store.
type anchorView struct {
	ls *LedgerState
	// pinned is set for a view over one point, whose source never changes
	// between calls.
	pinned  bool
	drained bool
	// txn is the metadata store read transaction of a pinned SQL view.
	txn types.Txn
	// set is the set snapshot of a pinned set view; nil reads the live set.
	set UtxoSetWriter
}

func (ls *LedgerState) liveAnchorView() *anchorView {
	return &anchorView{ls: ls}
}

func (v *anchorView) isDrained(ctx context.Context) (bool, error) {
	if v.pinned {
		return v.drained, nil
	}
	return v.ls.anchorFamilyDrained(ctx, nodesettings.FamilyAccounts)
}

func (v *anchorView) readSet() UtxoSetWriter {
	if v.set != nil {
		return v.set
	}
	return v.ls.readSet(nil)
}

// lock holds the apply lock around a read of the live set. A snapshot needs
// none, which is why a pinned set view can copy for as long as it takes
// without stalling block apply.
func (v *anchorView) lock() func() {
	if v.set != nil {
		return func() {}
	}
	v.ls.utxoSetApplyMu.RLock()
	return v.ls.utxoSetApplyMu.RUnlock
}

// AnchorStateView implements metadata.AnchorStateViewer. Accounts in the
// metadata store are read in one read transaction; accounts in the set are
// read from a set snapshot taken under the apply lock, so the reward records
// and the balances are of the same applied block.
func (ls *LedgerState) AnchorStateView(
	ctx context.Context,
) (metadata.AnchorStateSource, func(), error) {
	drained, err := ls.anchorFamilyDrained(ctx, nodesettings.FamilyAccounts)
	if err != nil {
		return nil, nil, err
	}
	v := &anchorView{ls: ls, pinned: true, drained: drained}
	if !drained {
		store, err := ls.anchorStore()
		if err != nil {
			return nil, nil, err
		}
		v.txn = ls.db.Metadata().ReadTransaction(ctx)
		release := func() { _ = v.txn.Rollback() }
		// A read transaction takes its snapshot at its first statement, so
		// run one now: the view is of this point, not of the first read.
		if _, _, err := store.ReadRewardJournalAfter(
			ctx, math.MaxInt64, 0, 1, v.txn,
		); err != nil {
			release()
			return nil, nil, err
		}
		return v, release, nil
	}
	if err := ls.AnchorStateReady(ctx); err != nil {
		return nil, nil, err
	}
	snapper, ok := ls.readSet(nil).(UtxoSetSnapshotter)
	if !ok {
		return nil, nil, drainedUnserved(nodesettings.FamilyAccounts)
	}
	ls.utxoSetApplyMu.RLock()
	snap, release, err := snapper.SnapshotUtxoSet()
	ls.utxoSetApplyMu.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	v.set = snap
	return v, release, nil
}

// AnchorStateReady implements metadata.AnchorStateSource. A pinned view was
// checked when it was taken.
func (v *anchorView) AnchorStateReady(ctx context.Context) error {
	if v.pinned {
		return nil
	}
	return v.ls.AnchorStateReady(ctx)
}

// AnchorUtxoDigest implements metadata.AnchorStateSource.
func (v *anchorView) AnchorUtxoDigest(
	ctx context.Context,
	anchor uint64,
) (metadata.UtxoSetDigest, error) {
	return v.ls.AnchorUtxoDigest(ctx, anchor)
}

// AnchorUtxoInMetadata implements metadata.AnchorStateSource.
func (v *anchorView) AnchorUtxoInMetadata(anchor uint64) (bool, error) {
	return v.ls.AnchorUtxoInMetadata(anchor)
}

// AnchorImportedAccounts implements metadata.AnchorStateSource. The cursor is
// the metadata store's account id when it serves the read, and the credential
// of the last baseline, its tag then its key, when the set does.
func (ls *LedgerState) AnchorImportedAccounts(
	ctx context.Context,
	anchor uint64,
	cursor []byte,
	limit int,
) ([]metadata.ImportedAccount, []byte, error) {
	return ls.liveAnchorView().AnchorImportedAccounts(ctx, anchor, cursor, limit)
}

// AnchorImportedAccounts implements metadata.AnchorStateSource.
func (v *anchorView) AnchorImportedAccounts(
	ctx context.Context,
	anchor uint64,
	cursor []byte,
	limit int,
) ([]metadata.ImportedAccount, []byte, error) {
	ls := v.ls
	drained, err := v.isDrained(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !drained {
		store, err := ls.anchorStore()
		if err != nil {
			return nil, nil, err
		}
		var after int64
		if len(cursor) != 0 {
			if len(cursor) != 8 {
				return nil, nil, fmt.Errorf("malformed account cursor %x", cursor)
			}
			after = int64(binary.BigEndian.Uint64(cursor)) //nolint:gosec // an id
		}
		accounts, next, err := store.ReadImportedAccounts(
			ctx, anchor, after, limit, v.txn,
		)
		if err != nil || next == 0 {
			return accounts, nil, err
		}
		return accounts, binary.BigEndian.AppendUint64(nil, uint64(next)), nil //nolint:gosec // an id
	}
	if !v.pinned {
		if err := ls.AnchorStateReady(ctx); err != nil {
			return nil, nil, err
		}
	}
	set := v.readSet()
	r, _ := set.(AccountReader)
	if !ls.keepsEpochState() {
		r = nil
	}
	pager, ok := set.(ImportBaselinePager)
	if r == nil || !ok {
		return nil, nil, drainedUnserved(nodesettings.FamilyAccounts)
	}
	var after *models.StakeCredentialRef
	if len(cursor) != 0 {
		ref := models.NewStakeCredentialRef(cursor[0], cursor[1:])
		after = &ref
	}
	defer v.lock()()
	baselines, next, err := pager.ImportBaselinesAfter(after, limit)
	if err != nil {
		return nil, nil, err
	}
	var out []metadata.ImportedAccount
	for _, b := range baselines {
		if b.AddedSlot < anchor {
			continue
		}
		// The metadata store joins a baseline to its account, so a baseline
		// whose account is gone is not an imported account.
		acct, err := r.Account(b.Ref.Tag, b.Ref.Key)
		if err != nil {
			return nil, nil, err
		}
		if acct == nil {
			continue
		}
		a := metadata.ImportedAccount{
			StakingKey: b.Ref.Key, Pool: b.Pool, Drep: b.Drep,
			Reward: acct.Reward, DrepType: b.DrepType,
			AddedSlot: b.AddedSlot, CredentialTag: b.Ref.Tag, Active: b.Active,
		}
		if b.Deposit != nil {
			d := types.Uint64(*b.Deposit)
			a.DepositAmount = &d
		}
		out = append(out, a)
	}
	if next == nil {
		return out, nil, nil
	}
	return out, append([]byte{next.Tag}, next.Key...), nil
}

// EachAnchorRewardJournal implements metadata.AnchorStateSource.
func (ls *LedgerState) EachAnchorRewardJournal(
	ctx context.Context,
	slot uint64,
	fn func(metadata.RewardJournalEntry) error,
) error {
	return ls.liveAnchorView().EachAnchorRewardJournal(ctx, slot, fn)
}

// EachAnchorRewardJournal implements metadata.AnchorStateSource.
func (v *anchorView) EachAnchorRewardJournal(
	ctx context.Context,
	slot uint64,
	fn func(metadata.RewardJournalEntry) error,
) error {
	ls := v.ls
	drained, err := v.isDrained(ctx)
	if err != nil {
		return err
	}
	if !drained {
		store, err := ls.anchorStore()
		if err != nil {
			return err
		}
		var cursor int64
		for {
			entries, next, err := store.ReadRewardJournalAfter(
				ctx, slot, cursor, 20_000, v.txn,
			)
			if err != nil {
				return err
			}
			for _, e := range entries {
				if err := fn(e); err != nil {
					return err
				}
			}
			if next == 0 {
				return nil
			}
			cursor = next
		}
	}
	if !v.pinned {
		if err := ls.AnchorStateReady(ctx); err != nil {
			return err
		}
	}
	r, ok := v.readSet().(RewardRecordStreamer)
	if !ok {
		return drainedUnserved(nodesettings.FamilyAccounts)
	}
	defer v.lock()()
	return r.EachRewardRecordIn(slot+1, math.MaxUint64, func(rec RewardRecord) error {
		return fn(metadata.RewardJournalEntry{
			StakingKey: rec.Ref.Key, Amount: rec.Amount,
			CredentialTag: rec.Ref.Tag, Withdrawal: rec.Withdrawal,
		})
	})
}

// utxoSetServesAnchor reports whether the live UTxO set, rather than the
// metadata store, holds the UTxO set a digest at the anchor is taken of: the
// set is the only copy and has loaded up to the anchor. A set that has not
// been loaded yet leaves the outputs in the metadata store, which the node
// moves into the set only as it starts.
func (ls *LedgerState) utxoSetServesAnchor(anchor uint64) (bool, error) {
	tipper, ok := ls.config.UtxoSet.(interface {
		Tip() (ocommon.Point, bool, error)
	})
	if !ok {
		return true, nil
	}
	tip, has, err := tipper.Tip()
	if err != nil {
		return false, err
	}
	return has && tip.Slot >= anchor, nil
}

// AnchorUtxoInMetadata implements metadata.AnchorStateSource.
func (ls *LedgerState) AnchorUtxoInMetadata(anchor uint64) (bool, error) {
	inSet, err := ls.utxoSetServesAnchor(anchor)
	if err != nil {
		return false, err
	}
	return !inSet, nil
}

// AnchorUtxoDigest implements metadata.AnchorStateSource. With the live UTxO
// set the only copy, the set at the anchor is rebuilt from its journal, which
// reaches back only one stability window.
func (ls *LedgerState) AnchorUtxoDigest(
	ctx context.Context,
	anchor uint64,
) (metadata.UtxoSetDigest, error) {
	inSet, err := ls.utxoSetServesAnchor(anchor)
	if err != nil {
		return metadata.UtxoSetDigest{}, err
	}
	if !inSet {
		store, err := ls.anchorStore()
		if err != nil {
			return metadata.UtxoSetDigest{}, err
		}
		return store.LiveUtxoDigestWithOutputs(
			ctx, anchor,
			func(txHash []byte, idx uint32) ([]byte, error) {
				return ls.db.ResolveUtxoCborWithRecovery(ctx, txHash, idx, nil)
			},
			nil,
		)
	}
	h, ok := ls.config.UtxoSet.(UtxoSetHistory)
	if !ok {
		return metadata.UtxoSetDigest{}, fmt.Errorf(
			"%w: the live UTxO set keeps no history",
			metadata.ErrAnchorDigestUnavailable,
		)
	}
	view, err := h.UtxoSetAt(ocommon.Point{Slot: anchor})
	if errors.Is(err, ErrUtxoAsOfUnavailable) {
		return metadata.UtxoSetDigest{}, fmt.Errorf(
			"%w: %w", metadata.ErrAnchorDigestUnavailable, err,
		)
	}
	if err != nil {
		return metadata.UtxoSetDigest{}, err
	}
	defer view.Close()
	d := metadata.NewUtxoDigester()
	err = view.EachUtxo(func(txID []byte, index uint32, cbor []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		out, err := gledger.NewTransactionOutputFromCbor(cbor)
		if err != nil {
			return fmt.Errorf("decode output %x#%d: %w", txID, index, err)
		}
		input, err := shelley.NewShelleyTransactionInput(
			hex.EncodeToString(txID), int(index),
		)
		if err != nil {
			return err
		}
		row, err := models.UtxoLedgerToModel(
			gledger.Utxo{Id: lcommon.TransactionInput(input), Output: out}, anchor,
		)
		if err != nil {
			return fmt.Errorf("convert output %x#%d: %w", txID, index, err)
		}
		d.Add(
			txID, index, strconv.FormatUint(uint64(row.Amount), 10),
			row.PaymentKey, row.StakingKey, row.DatumHash, true, cbor,
		)
		return nil
	})
	if err != nil {
		return metadata.UtxoSetDigest{}, err
	}
	return d.Sum(), nil
}
