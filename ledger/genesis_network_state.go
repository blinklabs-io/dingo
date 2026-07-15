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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

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
