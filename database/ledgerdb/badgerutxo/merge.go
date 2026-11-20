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

// Package badgerutxo keeps the live UTxO set in Badger. Changes arrive
// complete, as outputs added and outputs removed, so the store keeps no
// history: a rollback arrives as the outputs it restores and removes.
//
// Keys:
//
//	u <txid:32> <index:4 BE>   output CBOR, one per live output
//	n <slot:8 BE> <hash>       block nonce, a checkpoint flag then the nonce
//	m tip                      the point of the last applied change
//	m count                    the number of live outputs
//	m nonce                    set once every nonce up to the tip is held
//	e, s, m epoch              epoch and pool stake snapshot rows (epoch.go)
//	f, m esummary              epoch summary rows (summary.go)
package badgerutxo

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// loadedMarkers gate the family loads: a family whose marker is set is not
// loaded again. A merge clears them so every family loads the snapshot's rows
// over the ones it holds.
var loadedMarkers = [][]byte{
	noncesKey, legacyNoncesKey, epochStateKey, summaryKey,
	legacyEpochStateKey, networkStateKey, pparamsKey, accountsKey,
	legacyAccountsKey, syncMarkersKey, drepsKey, poolsKey, poolHistoryKey,
	legacyPoolHistoryKey, registrationHistoryKey,
	legacyRegistrationHistoryKey, genesisMIRKey, opCertsKey, legacyOpCertsKey,
	committeeKey, governanceKey, legacyGovernanceKey, pparamUpdatesKey,
	rewardOutputsKey, importBaselinesKey, importBaselineDelegationsKey,
	witnessSlotsKey,
	legacyWitnessSlotsKey, rewardBlocksKey, legacyRewardBlocksKey,
	rewardInputsKey, rewardDerivedKey, closureUtxoKey, delegationsKey,
	importedBlocksKey, drepHistoryLoadedKey, rewardRecordsKey,
}

// utxoMarkers describe the live outputs and the journal over them.
var utxoMarkers = [][]byte{
	tipKey, countKey, seqKey, formatKey, failedKey, rollbackPendingKey,
	loadFloorKey,
}

// valueMarkers hold values a full bootstrap resets with the rows they
// describe. The reward consumer's marks are positions in the reward record
// numbering, so they restart with it; the consumer's registration is not
// listed and stays.
var valueMarkers = [][]byte{
	eraSwitchKey, pparamsSeqKey, syntheticV2Key, syntheticV2ClearedKey,
	governanceSeqKey, rewardSeqKey, rewardConsumedKey, rewardConsumedSlotKey,
	exportFloorKey,
}

var (
	bootstrapMarkers = concatKeys(
		utxoMarkers, loadedMarkers, valueMarkers, [][]byte{mergeKey},
	)
	catchUpMarkers = utxoMarkers

	catchUpPrefixes = [][]byte{{UTxOTag}, {JournalTag}, {AggTag}}
	// bootstrapPrefixes drops every family a full bootstrap replaces.
	bootstrapPrefixes = [][]byte{
		{UTxOTag},
		{JournalTag},
		{AggTag},
		{NonceTag},
		{EpochTag},
		{SnapshotTag},
		{SummaryTag},
		{NetworkStateTag},
		{DonationTag},
		{PParamsTag},
		{PParamsSlotTag},
		{AccountTag},
		{AccountPoolTag},
		{AccountDRepTag},
		{StateJournalTag},
		{RewardRecordTag},
		rewardSeqIndexPrefix,
		{DRepTag},
		{DRepLinkTag},
		{DormancyTag},
		{PoolTag},
		{PoolHistoryTag},
		{StakeHistoryTag},
		{DRepDepositTag},
		{OpCertTag},
		{GenesisDelegationTag},
		{MIRCertTag},
		{CommitteeMemberTag},
		{CommitteeQuorumTag},
		{ConstitutionTag},
		{HotAuthTag},
		{ResignTag},
		{ProposalTag},
		{VoteTag},
		{PParamUpdateTag},
		{RewardOutputTag},
		{RewardEpochTag},
		{RewardRoundTag},
		{RewardPostSnapTag},
		{ImportBaselineTag},
		{WitnessTag},
		{RewardBlocksTag},
		{ClosureUtxoTag},
		{DelegationTag},
		{RewardStakeInputTag},
		{RewardPoolInputTag},
		{RewardSnapshotTag},
		{RewardPotsTag},
		{RewardDerivedTag},
		{RewardCursorTag},
		{ImportedBlocksTag},
		{DRepHistoryTag},
	}
)

func concatKeys(groups ...[][]byte) [][]byte {
	var out [][]byte
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// MergePending reports whether a catch-up bootstrap left the families'
// snapshot rows unmerged.
func (s *Store) MergePending() (bool, error) {
	return s.hasMarker(mergeKey)
}

// BeginMerge makes every family load the rows its feed supplies over the rows
// the set holds, in place of replacing them, until EndMerge: a load upserts by
// key, journals the row it overwrites at the tip's slot so a read as of an
// earlier slot still finds it, and drops nothing. It clears the loaded
// markers, so every family loads, and leaves the pending mark set until
// EndMerge, so a merge that stops part way runs again from the start, which
// writes the same rows.
func (s *Store) BeginMerge() error {
	if s.merging.Swap(true) {
		return errors.New("badgerutxo: a merge is already running")
	}
	// The reconcile that ends a merge rewrites rows below the floor the
	// merge raised, outside any load, so the whole merge is one rewrite.
	end := s.rewriteRows()
	s.added.endMerge.Store(&end)
	if err := s.update(func(txn *badger.Txn) error {
		for _, k := range loadedMarkers {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		s.endMergeRewrite()
		s.merging.Store(false)
		return fmt.Errorf("badgerutxo: begin merge: %w", err)
	}
	return nil
}

// EndMerge ends a merge and clears the pending mark. After a failed merge it
// only stops the merge mode, leaving the mark for the run that finishes it.
func (s *Store) EndMerge(complete bool) error {
	s.merging.Store(false)
	s.endMergeRewrite()
	if !complete {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Delete(mergeKey)
	})
}

// endMergeRewrite ends the rewrite BeginMerge began, once.
func (s *Store) endMergeRewrite() {
	if end := s.added.endMerge.Swap(nil); end != nil {
		(*end)()
	}
}

// Merging reports whether family loads are merging.
func (s *Store) Merging() bool {
	return s.merging.Load()
}

// loadSink is where a family load writes its rows. Outside a merge it is a
// Badger write batch. In a merge it upserts each row by key in small
// transactions, after journaling the row it overwrites at the tip's slot, so
// the load drops nothing and a read as of an earlier slot finds the earlier
// row.
type loadSink struct {
	wb    *badger.WriteBatch
	s     *Store
	slot  uint64
	err   error
	buf   []sinkRow
	bytes int
	// end ends the rewrite the load is, at its Flush or Cancel.
	end func()
	// mu is held only to satisfy the race detector's view of a feed that
	// calls put from another goroutine; a load is sequential.
	mu sync.Mutex
}

type sinkRow struct{ k, v []byte }

const (
	sinkMaxRows  = 256
	sinkMaxBytes = 1 << 20
)

func (s *Store) newLoadSink() *loadSink {
	end := sync.OnceFunc(s.rewriteRows())
	if !s.merging.Load() {
		return &loadSink{wb: s.newWriteBatch(), s: s, end: end}
	}
	l := &loadSink{s: s, end: end}
	// A failed read leaves the sink failing every write: journaling a row at
	// slot zero would let a rollback to the real tip keep it.
	l.err = s.view(func(txn *badger.Txn) error {
		tip, ok, err := readTip(txn)
		if err == nil && ok {
			l.slot = tip.Slot
		}
		return err
	})
	if l.err == nil {
		// Merged rows keep the AddedSlot they were written with, which
		// the entry journaled at the tip does not match.
		l.err = s.raiseFloor(exportFloorKey, l.slot+1)
	}
	return l
}

// Set writes the row under k.
func (l *loadSink) Set(k, v []byte) error {
	if l.wb != nil {
		return l.wb.Set(k, v)
	}
	if l.err != nil {
		return l.err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, sinkRow{append([]byte(nil), k...), append([]byte(nil), v...)})
	l.bytes += len(k) + len(v)
	if len(l.buf) >= sinkMaxRows || l.bytes >= sinkMaxBytes {
		return l.flush()
	}
	return nil
}

// Delete deletes the row under k.
func (l *loadSink) Delete(k []byte) error {
	if l.wb != nil {
		return l.wb.Delete(k)
	}
	return errors.New("badgerutxo: a merge deletes no row")
}

// Flush writes what is buffered.
func (l *loadSink) Flush() error {
	defer l.end()
	if l.wb != nil {
		return l.wb.Flush()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flush()
}

// Cancel discards the sink.
func (l *loadSink) Cancel() {
	defer l.end()
	if l.wb != nil {
		l.wb.Cancel()
	}
}

func (l *loadSink) flush() error {
	if l.err != nil {
		return l.err
	}
	if len(l.buf) == 0 {
		return nil
	}
	rows := l.buf
	l.buf, l.bytes = nil, 0
	return l.s.update(func(txn *badger.Txn) error {
		for _, r := range rows {
			if len(r.k) > 0 && isJournaledTag(r.k[0]) {
				if err := journalRow(txn, l.slot, r.k); err != nil {
					return err
				}
			}
			if err := setRow(txn, r.k, r.v, true); err != nil {
				return err
			}
		}
		return nil
	})
}

func isJournaledTag(tag byte) bool {
	return slices.Contains(journaledFamilyTags, tag)
}

// tombstoneFor returns the tombstone writer a bulk load hands its feed. A row
// loaded from the metadata store has no journal entry, so without the
// tombstone a rollback below the slot the metadata store drops it at would
// keep it. The prefix must start with one of tags. A merge keeps the rows
// under the prefix, so it records no tombstone.
func (l *loadSink) tombstoneFor(tags []byte) func(slot uint64, prefix []byte) error {
	return func(slot uint64, prefix []byte) error {
		if len(prefix) == 0 || !slices.Contains(tags, prefix[0]) {
			return fmt.Errorf("badgerutxo: %x is not a loadable prefix", prefix)
		}
		if l.wb == nil {
			return nil
		}
		return l.wb.Set(stateJournalKey(slot, prefix), []byte{tombstoneMark})
	}
}

// windowedTags are the families a catch-up's import replaces rather than adds
// to: the reward inputs, outputs and block counts only the epochs reward
// calculation can still read, which the snapshot restates in full. The
// imported block counts are not among them: the metadata store keeps the
// counts of an epoch a later snapshot does not carry.
var windowedTags = []byte{
	RewardOutputTag, RewardEpochTag, RewardRoundTag, RewardPostSnapTag,
	RewardBlocksTag, RewardStakeInputTag, RewardPoolInputTag,
	RewardSnapshotTag, RewardPotsTag, RewardDerivedTag, RewardCursorTag,
}

// dropWindowed drops the windowed families among prefixes, which a merge
// replaces, and keeps every other.
func (s *Store) dropWindowed(prefixes [][]byte) error {
	var drop [][]byte
	for _, p := range prefixes {
		if len(p) > 0 && slices.Contains(windowedTags, p[0]) {
			drop = append(drop, p)
		}
	}
	if len(drop) == 0 {
		return nil
	}
	if s.snap != nil {
		return ErrSnapshotReadOnly
	}
	defer s.rewriteRows()()
	return s.db.DropPrefix(drop...)
}
