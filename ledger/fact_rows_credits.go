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
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

// RewardRecordReader serves the reward records a live UTxO set holds.
type RewardRecordReader interface {
	// RewardRecordsIn returns the records dated in [from, through], oldest
	// write first, each with its sequence number.
	RewardRecordsIn(from, through uint64) ([]RewardRecord, error)
}

// RewardCreditPager is the part of a set that serves reward credits by
// sequence number without reading the records it skips.
type RewardCreditPager interface {
	// RewardCreditsAfter returns the credits dated in [from, through] in the
	// order the set wrote them, those numbered above cursor when it is
	// positive, at most limit of them; more reports another follows.
	RewardCreditsAfter(
		from, through, cursor uint64,
		limit int,
	) ([]RewardRecord, bool, error)
}

// RewardRecordRetainer is the part of a set that keeps reward records for the
// reader that copies them.
type RewardRecordRetainer interface {
	// RewardRecordHigh returns the sequence number the next record takes.
	RewardRecordHigh() (uint64, error)
	// AckRewardRecords records that every record numbered below seq was
	// copied.
	AckRewardRecords(seq, slot uint64) error
	// RewardConsumedSlot returns the slot through which the consumer has
	// copied the reward rows; ok is false without a consumer.
	RewardConsumedSlot() (slot uint64, ok bool, err error)
	// RewardRecordsLost reports records the metadata store drained before the
	// set could load them.
	RewardRecordsLost() (bool, error)
}

// RewardJournalHigh returns the sequence number the set's next reward record
// takes, zero when the set holds no retention. A reader samples it before it
// reads and acknowledges it once its copy is durable. It does not depend on
// whether the accounts family is drained: the set retains records for a
// registered consumer either way, and a reader of the metadata store's journal
// has copied every record below the sampled position just as one of the set's
// has.
func (ls *LedgerState) RewardJournalHigh() (uint64, error) {
	r, ok := ls.readSet(nil).(RewardRecordRetainer)
	if !ok {
		return 0, nil
	}
	return r.RewardRecordHigh()
}

// AckRewardJournal tells the set that the reader has copied every reward
// record numbered below seq and the rows dated through throughSlot, which it may then prune below its rollback window.
func (ls *LedgerState) AckRewardJournal(seq, throughSlot uint64) error {
	r, ok := ls.readSet(nil).(RewardRecordRetainer)
	if !ok {
		return nil
	}
	return r.AckRewardRecords(seq, throughSlot)
}

// rewardPruneBelow lowers the epoch a reward prune drops rows below to the
// epoch the consumer has copied, so the stake and derived rows it still needs
// stay.
func (ls *LedgerState) rewardPruneBelow(epoch uint64) uint64 {
	r, ok := ls.readSet(nil).(RewardRecordRetainer)
	if !ok {
		return epoch
	}
	slot, held, err := r.RewardConsumedSlot()
	if err != nil || !held {
		return epoch
	}
	// Rows of an epoch reach the boundary RewardInputEpochWindow epochs
	// later, so the epoch before the consumer's, less that window, is whole.
	consumed, err := ls.SlotToEpoch(slot)
	if err != nil {
		return 0
	}
	return min(epoch, consumed.EpochId-min(consumed.EpochId, uint64(RewardInputEpochWindow)+1))
}

// accountRewardCreditPage exports the reward credits the set recorded dated in
// [from, through], as the metadata store's reward journal holds them. The
// cursor is the sequence number of the last credit served: the set numbers its
// records in the order it wrote them whatever their slot, so a credit
// journaled later under an old boundary slot follows every credit a reader
// has already taken. The set keeps its records only for the rollback window,
// so a credit older than that is not exported.
func (ls *LedgerState) accountRewardCreditPage(
	_ metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
) (metadata.FactRows, int64, error) {
	r, ok := ls.readSet(nil).(RewardRecordReader)
	if !ok || ls.accountReader(nil) == nil {
		return metadata.FactRows{}, 0, drainedUnserved(nodesettings.FamilyAccounts)
	}
	if keeper, ok := r.(RewardRecordRetainer); ok {
		lost, err := keeper.RewardRecordsLost()
		if err != nil {
			return metadata.FactRows{}, 0, err
		}
		if lost {
			return metadata.FactRows{}, 0, fmt.Errorf(
				"%w: the reward journal was drained before the set held it, "+
					"and its credits before then are in neither store",
				drainedUnserved(nodesettings.FamilyAccounts),
			)
		}
	}
	out := metadata.FactRows{Columns: []string{
		"staking_key", "credential_tag", "tx_hash", "amount", "added_slot",
		"post_snapshot",
	}}
	var recs []RewardRecord
	more := false
	if pager, ok := r.(RewardCreditPager); ok {
		ls.utxoSetApplyMu.RLock()
		var err error
		recs, more, err = pager.RewardCreditsAfter(
			from, through, uint64(max(cursor, 0)), limit, //nolint:gosec // non-negative
		)
		ls.utxoSetApplyMu.RUnlock()
		if err != nil {
			return metadata.FactRows{}, 0, err
		}
	} else {
		ls.utxoSetApplyMu.RLock()
		all, err := r.RewardRecordsIn(from, through)
		ls.utxoSetApplyMu.RUnlock()
		if err != nil {
			return metadata.FactRows{}, 0, err
		}
		all = slices.DeleteFunc(all, func(rec RewardRecord) bool {
			return rec.Withdrawal ||
				(cursor > 0 && int64(rec.Seq) <= cursor) //nolint:gosec // a count of records
		})
		for _, rec := range all {
			// A record from before sequences were kept has none to resume
			// from, so those are served together on the first page.
			if len(recs) >= limit && rec.Seq > 0 {
				more = true
				break
			}
			recs = append(recs, rec)
		}
	}
	var last int64
	for _, rec := range recs {
		last = int64(rec.Seq) //nolint:gosec // a count of records
		out.Rows = append(out.Rows, []any{
			nilBytes(rec.Ref.Key), sqlInt(uint64(rec.Ref.Tag)),
			nilBytes(rec.Source), uint64Text(types.Uint64(rec.Amount)),
			sqlInt(rec.Slot), boolInt(rec.PostSnapshot),
		})
	}
	out.LastID = last
	if more {
		return out, last, nil
	}
	return out, 0, nil
}
