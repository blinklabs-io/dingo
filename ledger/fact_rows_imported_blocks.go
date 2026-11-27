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
	"bytes"
	"slices"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// importedBlocks lists the imported block counts the set retains, by epoch.
func (ls *LedgerState) importedBlocks() (map[uint64]ImportedBlockEpoch, error) {
	if ls.rewardBlocksReader(nil) == nil {
		return nil, ledgerStateUnserved(nodesettings.FamilyRewardBlocksFees)
	}
	r, ok := ls.readSet(nil).(ImportedBlocksReader)
	if !ok {
		return nil, ledgerStateUnserved(nodesettings.FamilyRewardBlocksFees)
	}
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	return r.ImportedBlocks()
}

func sortedImportedEpochs(
	rows map[uint64]ImportedBlockEpoch,
	from, through uint64,
) []uint64 {
	var out []uint64
	for epoch, row := range rows {
		if inSlotRange(row.CapturedSlot, from, through) {
			out = append(out, epoch)
		}
	}
	slices.Sort(out)
	return out
}

func (ls *LedgerState) importedEpochBlockTotalFactRows(
	table metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	rows, err := ls.importedBlocks()
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"epoch", "total_blocks", "captured_slot",
	}}
	for _, epoch := range sortedImportedEpochs(rows, from, through) {
		out.Rows = append(out.Rows, []any{
			sqlInt(epoch), sqlInt(rows[epoch].Total),
			sqlInt(rows[epoch].CapturedSlot),
		})
	}
	return out, nil
}

func (ls *LedgerState) importedPoolBlockCountFactRows(
	table metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	rows, err := ls.importedBlocks()
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"epoch", "pool_key_hash", "blocks_produced", "captured_slot",
	}}
	for _, epoch := range sortedImportedEpochs(rows, from, through) {
		row := rows[epoch]
		pools := make([]string, 0, len(row.Pools))
		for pool := range row.Pools {
			pools = append(pools, pool)
		}
		slices.SortFunc(pools, func(a, b string) int {
			return bytes.Compare([]byte(a), []byte(b))
		})
		for _, pool := range pools {
			out.Rows = append(out.Rows, []any{
				sqlInt(epoch), []byte(pool), sqlInt(row.Pools[pool]),
				sqlInt(row.CapturedSlot),
			})
		}
	}
	return out, nil
}
