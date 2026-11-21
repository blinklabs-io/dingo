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
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// CountEpochPoolBlocks answers the epoch-boundary capture's block counts from
// the set's all-slot rows when the set serves reward block rows.
func (s stagingLedgerDB) CountEpochPoolBlocks(
	epoch uint64,
	poolKeys []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	if r := s.ls.rewardBlocksServing(txn); r != nil {
		counts, total, ok, err := s.ls.epochPoolBlocksFromSet(
			r,
			txn,
			epoch,
			poolKeys,
		)
		if err != nil {
			return nil, 0, false, err
		}
		if ok {
			return counts, total, true, nil
		}
		return nil, 0, false, s.ls.setFamilyUnserved(nodesettings.FamilyRewardBlocksFees)
	}
	return nil, 0, false, s.ls.setFamilyUnserved(nodesettings.FamilyRewardBlocksFees)
}

// epochPoolBlocksFromSet is CountEpochPoolBlocks from the set. ok is false
// when the set holds no all-slot counts for the epoch.
func (ls *LedgerState) epochPoolBlocksFromSet(
	r RewardBlocksReader,
	txn ledgerdb.Txn,
	epoch uint64,
	poolKeys []lcommon.PoolKeyHash,
) (counts map[string]uint64, total uint64, ok bool, err error) {
	view, err := ls.rewardBlocksView(r, txn, epoch)
	if err != nil || !view.AllKnown {
		return nil, 0, false, err
	}
	counts = make(map[string]uint64, len(poolKeys))
	for _, k := range poolKeys {
		counts[string(k[:])] = view.AllPools[string(k[:])]
	}
	return counts, view.AllTotal, true, nil
}
