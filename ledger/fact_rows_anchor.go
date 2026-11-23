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
}

// RewardRecordStreamer serves the reward records a live UTxO set holds
// without collecting them.
type RewardRecordStreamer interface {
	EachRewardRecordIn(from, through uint64, fn func(RewardRecord) error) error
}

// AnchorStateReady implements metadata.AnchorStateSource. The accounts are
// served from the set's import baselines and reward records.
func (ls *LedgerState) AnchorStateReady(ctx context.Context) error {
	if ls.accountReader(nil) == nil {
		return ledgerStateUnserved(nodesettings.FamilyAccounts)
	}
	set := ls.readSet(nil)
	if _, ok := set.(ImportBaselinePager); !ok {
		return ledgerStateUnserved(nodesettings.FamilyAccounts)
	}
	if _, ok := set.(RewardRecordRetainer); !ok {
		return ledgerStateUnserved(nodesettings.FamilyAccounts)
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
	pinned bool
	// set is the set snapshot of a pinned set view; nil reads the live set.
	set UtxoSetWriter
}

func (ls *LedgerState) liveAnchorView() *anchorView {
	return &anchorView{ls: ls}
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
	v := &anchorView{ls: ls, pinned: true}
	if err := ls.AnchorStateReady(ctx); err != nil {
		return nil, nil, err
	}
	snapper, ok := ls.readSet(nil).(UtxoSetSnapshotter)
	if !ok {
		return nil, nil, ledgerStateUnserved(nodesettings.FamilyAccounts)
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
	if !v.pinned {
		if err := ls.AnchorStateReady(ctx); err != nil {
			return nil, nil, err
		}
	}
	set := v.readSet()
	r, _ := set.(AccountReader)
	pager, ok := set.(ImportBaselinePager)
	if r == nil || !ok {
		return nil, nil, ledgerStateUnserved(nodesettings.FamilyAccounts)
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
	if !v.pinned {
		if err := ls.AnchorStateReady(ctx); err != nil {
			return err
		}
	}
	r, ok := v.readSet().(RewardRecordStreamer)
	if !ok {
		return ledgerStateUnserved(nodesettings.FamilyAccounts)
	}
	defer v.lock()()
	return r.EachRewardRecordIn(slot+1, math.MaxUint64, func(rec RewardRecord) error {
		return fn(metadata.RewardJournalEntry{
			StakingKey: rec.Ref.Key, Amount: rec.Amount,
			CredentialTag: rec.Ref.Tag, Withdrawal: rec.Withdrawal,
		})
	})
}

// utxoSetServesAnchor reports whether the live UTxO set has loaded up to the
// anchor.
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

// AnchorUtxoDigest implements metadata.AnchorStateSource. The set at the
// anchor is rebuilt from its journal, which reaches back only one stability
// window.
func (ls *LedgerState) AnchorUtxoDigest(
	ctx context.Context,
	anchor uint64,
) (metadata.UtxoSetDigest, error) {
	inSet, err := ls.utxoSetServesAnchor(anchor)
	if err != nil {
		return metadata.UtxoSetDigest{}, err
	}
	if !inSet {
		return metadata.UtxoSetDigest{}, fmt.Errorf(
			"%w: the live UTxO set has not loaded up to the anchor",
			metadata.ErrAnchorDigestUnavailable,
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
