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
	"fmt"
	"slices"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// loadedMarkers gate the family loads: a family whose marker is set is not
// loaded again.
var loadedMarkers = [][]byte{
	noncesKey, legacyNoncesKey, epochStateKey, summaryKey,
	legacyEpochStateKey, networkStateKey, pparamsKey, accountsKey,
	legacyAccountsKey, accountInactivityActivationKey, syncMarkersKey, drepsKey, poolsKey, poolHistoryKey,
	legacyPoolHistoryKey, registrationHistoryKey,
	legacyRegistrationHistoryKey, genesisMIRKey, opCertsKey, legacyOpCertsKey,
	committeeKey, governanceKey, legacyGovernanceKey, pparamUpdatesKey,
	rewardOutputsKey, importBaselinesKey, importBaselineDelegationsKey,
	witnessSlotsKey,
	legacyWitnessSlotsKey, rewardBlocksKey, legacyRewardBlocksKey,
	rewardInputsKey, rewardDerivedKey, closureUtxoKey, delegationsKey,
	importedBlocksKey, drepHistoryLoadedKey, rewardRecordsKey,
	inactivityKey, ratifyPendingKey,
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
	eraSwitchKey, snapshotSlotsKey, pparamsSeqKey, syntheticV2Key, syntheticV2ClearedKey,
	governanceSeqKey, rewardSeqKey, rewardConsumedKey, rewardConsumedSlotKey,
	exportFloorKey,
}

var (
	bootstrapMarkers = concatKeys(
		utxoMarkers, loadedMarkers, valueMarkers,
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
		{SnapshotSlotTag},
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
		{RewardLikelihoodTag},
		{RewardDerivedTag},
		{RewardCursorTag},
		{ImportedBlocksTag},
		{DRepHistoryTag},
		{GovernanceBoundaryTag},
		{InactivityTag},
		{RatifyPendingTag},
	}
)

func concatKeys(groups ...[][]byte) [][]byte {
	var out [][]byte
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// loadSink is where a family load writes its rows: a Badger write batch.
type loadSink struct {
	wb *badger.WriteBatch
	// end ends the rewrite the load is, at its Flush or Cancel.
	end func()
}

func (s *Store) newLoadSink() *loadSink {
	return &loadSink{
		wb:  s.newWriteBatch(),
		end: sync.OnceFunc(s.rewriteRows()),
	}
}

// Set writes the row under k.
func (l *loadSink) Set(k, v []byte) error {
	return l.wb.Set(k, v)
}

// Delete deletes the row under k.
func (l *loadSink) Delete(k []byte) error {
	return l.wb.Delete(k)
}

// Flush writes what is buffered.
func (l *loadSink) Flush() error {
	defer l.end()
	return l.wb.Flush()
}

// Cancel discards the sink.
func (l *loadSink) Cancel() {
	defer l.end()
	l.wb.Cancel()
}

// tombstoneFor returns the tombstone writer a bulk load hands its feed. A row
// loaded from the metadata store has no journal entry, so without the
// tombstone a rollback below the slot the metadata store drops it at would
// keep it. The prefix must start with one of tags.
func (l *loadSink) tombstoneFor(tags []byte) func(slot uint64, prefix []byte) error {
	return func(slot uint64, prefix []byte) error {
		if len(prefix) == 0 || !slices.Contains(tags, prefix[0]) {
			return fmt.Errorf("badgerutxo: %x is not a loadable prefix", prefix)
		}
		return l.wb.Set(stateJournalKey(slot, prefix), []byte{tombstoneMark})
	}
}
