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
)

// applyByronTranslationReserves resets reserves at the Byron-to-Shelley
// boundary to the maximum lovelace supply minus the live UTxO balance being
// translated, as cardano-ledger's Shelley/API/ByronTranslation.hs does.
// Value that left circulation during Byron, such as fees, is therefore
// reserves rather than missing from every pot. It runs before any Shelley
// boundary rule, as the hard-fork combinator translates the ledger before the
// first Shelley TICK, and before the first Shelley epoch's ADA pots are
// captured, since the first Shelley reward round reads them.
//
// An empty live set is never a translated UTxO: genesis sync refuses a
// configuration with no genesis UTxO, and those funds can only move, not
// vanish, so zero means the UTxO table was not populated. Neither max supply
// nor the genesis baseline is the translated reserves then, so the boundary
// fails rather than commit the first Shelley epoch from an unverified supply.
func (ls *LedgerState) applyByronTranslationReserves(
	txn ledgerdb.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) error {
	return ls.engine().ApplyByronTranslationReserves(txn, newEpoch, boundarySlot)
}
