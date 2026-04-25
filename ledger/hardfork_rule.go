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
)

// applyIntraEraHardForkRule dispatches cardano-ledger's per-major-version
// HARDFORK rule. Unlike the inter-era HFC detection that fires when the
// era ID changes, this runs on any bump of the protocol *major* version
// — including intra-era ones that do not cross an era boundary, and
// inter-era ones whose translation carries a state rewrite.
//
// The rule is called at the epoch-rollover boundary, in the same
// database transaction as the pparams write, so the state rewrite
// commits atomically with the major-version bump that triggers it.
//
// Currently implemented cases:
//
//   - major == 3 (Shelley→Allegra, mainnet December 2020): remove every
//     live UTxO at a Byron redeem (AVVM) address and return its lovelace
//     to reserves. Ports cardano-ledger
//     Allegra/Translation.hs:returnRedeemAddrsToReserves.
//   - major == 10 (Plomin, mainnet January 2025): clear any dangling
//     credential-backed DRep delegation whose target DRep credential is
//     not currently registered as an active DRep. Pseudo-DRep
//     delegations (AlwaysAbstain, AlwaysNoConfidence) are preserved.
//
// Cases known but not yet required (no era defined in dingo yet):
//
//   - major == 11 (Dijkstra): populate per-pool VRF key hashes. Stubbed
//     until the Dijkstra era lands in gouroboros + dingo's era table.
//
// Any future major-version bump that lands without a case here is a
// no-op, matching the Haskell rule's `otherwise = id` branch.
func (ls *LedgerState) applyIntraEraHardForkRule(
	txn *database.Txn,
	newMajor uint,
	boundarySlot uint64,
	newEpoch uint64,
) error {
	switch newMajor {
	case 3:
		count, total, err := ls.db.RemoveByronAvvmUtxos(boundarySlot, txn)
		if err != nil {
			return fmt.Errorf(
				"pv3 remove Byron AVVM UTxOs at slot %d: %w",
				boundarySlot, err,
			)
		}
		// Reserves are not maintained on every epoch-rollover in dingo
		// today — governance enactment is the only caller that writes
		// NetworkState — so we log the reclaimed total rather than
		// writing a partial, likely-misleading reserves value. When
		// full reserves tracking lands, consume totalLovelace here.
		ls.config.Logger.Info(
			"applied Allegra HARDFORK rule (pv3 AVVM return)",
			"removed_avvm_utxos", count,
			"reclaimed_lovelace", total,
			"epoch", newEpoch,
			"boundary_slot", boundarySlot,
			"component", "ledger",
		)
	case 10:
		n, err := ls.db.ClearDanglingDRepDelegations(boundarySlot, txn)
		if err != nil {
			return fmt.Errorf(
				"pv10 clear dangling DRep delegations at slot %d: %w",
				boundarySlot, err,
			)
		}
		ls.config.Logger.Info(
			"applied Conway HARDFORK rule (pv10 Plomin)",
			"cleared_dangling_drep_delegations", n,
			"epoch", newEpoch,
			"boundary_slot", boundarySlot,
			"component", "ledger",
		)
	}
	return nil
}
