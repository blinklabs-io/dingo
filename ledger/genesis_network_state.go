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
	"errors"
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

// applyByronTranslationReserves resets reserves at the Byron-to-Shelley
// boundary to the maximum lovelace supply minus the live UTxO balance being
// translated, as cardano-ledger's Shelley/API/ByronTranslation.hs does.
// Value that left circulation during Byron, such as fees, is therefore
// reserves rather than missing from every pot. It must run before the first
// Shelley epoch's ADA pots are captured, since the first Shelley reward round
// reads them.
func (ls *LedgerState) applyByronTranslationReserves(
	txn *database.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) error {
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return errors.New("missing Shelley genesis")
	}
	meta := ls.db.Metadata()
	live, err := meta.GetLiveUtxoLovelace(txn.Metadata())
	if err != nil {
		return fmt.Errorf("sum live UTxO balance: %w", err)
	}
	if live > shelleyGenesis.MaxLovelaceSupply {
		return fmt.Errorf(
			"live UTxO balance %d exceeds max lovelace supply %d",
			live,
			shelleyGenesis.MaxLovelaceSupply,
		)
	}
	reserves := shelleyGenesis.MaxLovelaceSupply - live
	state, err := meta.GetNetworkState(txn.Metadata())
	if err != nil {
		return fmt.Errorf("get network state: %w", err)
	}
	var treasury uint64
	if state != nil {
		treasury = uint64(state.Treasury)
	}
	if err := meta.SetNetworkState(
		treasury, reserves, boundarySlot, txn.Metadata(),
	); err != nil {
		return fmt.Errorf("set translated reserves: %w", err)
	}
	ls.config.Logger.Info(
		"set Shelley reserves from Byron UTxO balance",
		"reserves", reserves,
		"live_utxo_lovelace", live,
		"epoch", newEpoch,
		"boundary_slot", boundarySlot,
		"component", "ledger",
	)
	return nil
}
