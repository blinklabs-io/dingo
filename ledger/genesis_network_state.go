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
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// rejectDuplicateGenesisUtxos returns an error if any two genesis UTxOs
// (Byron or Shelley) reference the same transaction ID and output index.
// The reference implementation rejects overlapping initial UTxOs before
// constructing genesis state; Dingo's downstream views of this list
// (reserve-balance summation, synthetic CBOR, and the offset-keyed metadata
// insert) each resolve a duplicate differently, so a duplicate must be
// caught here rather than left for one of those views to silently win.
func rejectDuplicateGenesisUtxos(genesisUtxos []lcommon.Utxo) error {
	seen := make(map[database.UtxoRef]struct{}, len(genesisUtxos))
	for i := range genesisUtxos {
		if genesisUtxos[i].Id == nil {
			return fmt.Errorf("genesis UTxO %d has no input reference", i)
		}
		ref := database.UtxoRef{
			TxId:      genesisUtxos[i].Id.Id(),
			OutputIdx: genesisUtxos[i].Id.Index(),
		}
		if _, ok := seen[ref]; ok {
			return fmt.Errorf(
				"duplicate genesis UTxO reference %x#%d",
				ref.TxId,
				ref.OutputIdx,
			)
		}
		seen[ref] = struct{}{}
	}
	return nil
}

// genesisReserveBalance returns the reserves remaining after every Byron and
// Shelley genesis UTxO has been placed into circulation.
func genesisReserveBalance(
	maxLovelaceSupply uint64,
	genesisUtxos []lcommon.Utxo,
) (uint64, error) {
	circulating := new(big.Int)
	for i := range genesisUtxos {
		if genesisUtxos[i].Output == nil {
			return 0, fmt.Errorf("genesis UTxO %d has no output", i)
		}
		amount := genesisUtxos[i].Output.Amount()
		if amount == nil || amount.Sign() < 0 {
			return 0, fmt.Errorf(
				"genesis UTxO %d has invalid amount %v",
				i,
				amount,
			)
		}
		circulating.Add(circulating, amount)
	}

	maxSupply := new(big.Int).SetUint64(maxLovelaceSupply)
	if circulating.Cmp(maxSupply) > 0 {
		return 0, fmt.Errorf(
			"genesis circulating supply %s exceeds max lovelace supply %d",
			circulating,
			maxLovelaceSupply,
		)
	}
	return new(big.Int).Sub(maxSupply, circulating).Uint64(), nil
}
