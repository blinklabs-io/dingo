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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// removeAvvmUtxos ports cardano-ledger's
// Allegra/Translation.hs:returnRedeemAddrsToReserves to the dingo
// ledger. At the Shelley→Allegra boundary every live UTxO at a Byron
// redeem (AVVM) address must leave the active UTxO set; its lovelace
// is conceptually returned to reserves (full reserves accounting is
// tracked separately and is out of scope here — we log the reclaimed
// total instead).
//
// The database layer exposes only era-agnostic primitives: an
// iterator over live UTxO rows (with CBOR populated) and a bulk
// "mark deleted at slot" call. The AVVM classification — decoding
// each output's address and checking for Byron + redeem — lives here
// because that knowledge is ledger-domain.
//
// Divergence from cardano-ledger:
//
// Haskell's Shelley ledger stashes the AVVM UTxO set into a dedicated
// `stashedAVVMAddresses` NewEpochState field at genesis-build time, and
// Allegra translation consumes that stash via
// `shelleyToAllegraAVVMsToDelete = stashedAVVMAddresses`, then discards
// the field. The comment at
// cardano-ledger/eras/allegra/.../Translation.hs:53-54 is explicit about
// the reason: a full UTxO scan at the boundary would be prohibitive on a
// disk-persisted state. Dingo scans the live UTxO instead, which is
// mainnet-equivalent because no AVVM UTxOs were spent before the
// Allegra boundary so the live AVVM set equals the stashed set there.
// On a hypothetical chain where AVVM UTxOs ARE spent pre-Allegra, the
// scan-based approach is still correct — the stash variant gives the
// same answer because spent UTxOs are absent from both views — so the
// divergence is purely an implementation choice driven by dingo's
// SQLite-backed UTxO model, not a semantic difference.
//
// Returns the count and total lovelace of the rows marked deleted.
func (ls *LedgerState) removeAvvmUtxos(
	txn *database.Txn,
	boundarySlot uint64,
) (int, uint64, error) {
	var (
		refs          []types.UtxoKey
		totalLovelace uint64
	)
	if err := ls.db.IterateLiveUtxos(txn, func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf(
				"decode utxo %x#%d: %w",
				u.TxId[:8], u.OutputIdx, err,
			)
		}
		addr := out.Address()
		if addr.Type() != lcommon.AddressTypeByron {
			return nil
		}
		if addr.ByronType() != lcommon.ByronAddressTypeRedeem {
			return nil
		}
		refs = append(refs, types.UtxoKey{
			TxId:      u.TxId,
			OutputIdx: u.OutputIdx,
		})
		totalLovelace += uint64(u.Amount)
		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("scan live utxos: %w", err)
	}
	if err := ls.db.MarkUtxosDeletedAtSlot(
		txn, refs, boundarySlot,
	); err != nil {
		return 0, 0, fmt.Errorf("mark avvm utxos deleted: %w", err)
	}
	return len(refs), totalLovelace, nil
}
