// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	ledger "github.com/blinklabs-io/gouroboros/ledger"
)

// Pointer addresses (types 4 and 5) designate a stake credential by the
// position of the certificate that registered it rather than by carrying the
// credential, so gouroboros reports an empty StakeKeyHash for one. Dingo stored
// the output with a NULL staking_key and its value never reached the stake
// distribution, understating the delegated stake of any account holding funds
// at a pointer address. That tightens the pool's Praos leader threshold and
// makes the node reject blocks the network accepted (dingo #3854, #3811).
//
// The position is persisted in utxo_pointer when the output is written; which
// credential it designates is decided when stake is computed, because that is
// what the ledger does. resolveShelleyInstantStake looks the Ptr up in saPtrs
// at snapshot time, and saPtrs is mutable state:
//
//   - a pointer may name a position no certificate occupies yet -- nothing in
//     any era validates an address's pointer payload -- and starts counting
//     once the registration lands;
//   - de-registration removes the Ptr (removePtr), so the address is
//     permanently dangling afterwards, and a later re-registration mints a new
//     Ptr at a new position that the old address does not name;
//   - Conway drops the pointer map entirely, so pointer stake stops counting
//     for every such output, not only for outputs created after the fork.
//
// Resolving once at ingest cannot express any of those. Resolving from the
// persisted position at the slot being evaluated expresses all three, and needs
// no repair pass: it is a pure function of the certificate rows, which rollback
// already restores.

// pointerStakeCounted reports whether a stake computation at slot must count
// stake held at pointer addresses.
//
// Shelley through Babbage count it: ShelleyInstantStake carries sisPtrStake
// alongside sisCredentialStake, and Babbage reuses the Shelley instant stake
// wholesale. Conway does not: ConwayInstantStake has only a credential map, and
// a StakeRefPtr output falls through its accumulator and is dropped. Dijkstra
// aliases Conway's. The cutover is the Babbage->Conway translation at protocol
// major 8 -> 9, which rebuilds the instant stake from sisCredentialStake alone
// and drops saPtrs from the translated accounts, so the pointer index itself is
// gone. Pointer addresses stay spendable in Conway; they simply confer no
// stake.
//
// The era comes from the epoch containing slot. An unknown era is treated as
// not counting: that is the behaviour before pointer resolution existed, so a
// missing epoch row understates rather than inflating a pool's snapshot stake
// and the shared active-stake denominator.
func pointerStakeCounted(
	ctx context.Context,
	db queryer,
	slot uint64,
) (bool, error) {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return false, err
	}
	// The epoch's own length is deliberately not an upper bound here. A slot
	// past the last recorded epoch still belongs to the era that epoch
	// started, and eras only advance.
	var eraID sql.NullInt64
	err = db.QueryRowContext(ctx, `
SELECT era_id FROM epoch
WHERE start_slot <= ?
ORDER BY start_slot DESC
LIMIT 1`,
		slotValue,
	).Scan(&eraID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("resolve era for slot %d: %w", slot, err)
	}
	if !eraID.Valid {
		return false, nil
	}
	return eraID.Int64 >= ledger.EraIdShelley &&
		eraID.Int64 <= ledger.EraIdBabbage, nil
}

// persistUtxoPointer records the certificate position a pointer address names.
//
// The position is a property of the address, so re-applying the same output
// converges rather than conflicting: an output a snapshot import created before
// its producing transaction was replayed has no row yet, and an output applied
// twice writes the same values.
func persistUtxoPointer(
	ctx context.Context,
	db queryer,
	utxoID int64,
	pointer *models.UtxoPointer,
) error {
	if pointer == nil {
		return nil
	}
	slot, err := checkedInt64(pointer.Slot)
	if err != nil {
		return fmt.Errorf("pointer slot: %w", err)
	}
	txIndex, err := checkedInt64(pointer.TxIndex)
	if err != nil {
		return fmt.Errorf("pointer tx index: %w", err)
	}
	certIndex, err := checkedInt64(pointer.CertIndex)
	if err != nil {
		return fmt.Errorf("pointer cert index: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO utxo_pointer (utxo_id, ptr_slot, ptr_tx_index, ptr_cert_index)
VALUES (?, ?, ?, ?)
ON CONFLICT (utxo_id) DO UPDATE SET
    ptr_slot = excluded.ptr_slot,
    ptr_tx_index = excluded.ptr_tx_index,
    ptr_cert_index = excluded.ptr_cert_index`,
		utxoID, slot, txIndex, certIndex,
	); err != nil {
		return fmt.Errorf("record UTxO pointer: %w", err)
	}
	return nil
}

// pointerResolutionSQL is a CTE resolving every recorded pointer position to
// the credential it designates as of slot. It must follow activeDelegationSQL,
// whose registration_events it reads.
//
// The three components of a pointer are (slot, transaction index within the
// block, certificate index within the transaction), which is why the join runs
// through the transaction's block_index and certs.cert_index. cert_index counts
// every certificate in the transaction, not only the registrations, matching
// the reference's CertIx (length gamma).
//
// Only stake_registration mints a Ptr in the eras that count pointer stake.
// Conway's registration certificates mint one in principle, but no stake
// computation this CTE serves is ever asked about a Conway slot, so they cannot
// contribute a resolution.
//
// The NOT EXISTS is removePtr: a de-registration of the same credential at a
// position after the registration deletes the Ptr, and the address is dangling
// from then on. A later re-registration mints a Ptr at its own position, which
// the address does not name, so it must not revive this one -- hence the
// comparison is against the registration's own position rather than against the
// credential's latest registration.
func pointerResolutionSQL(slot uint64) (string, []any, error) {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return "", nil, err
	}
	return `,
pointer_resolution AS (
 SELECT utxo_pointer.utxo_id AS utxo_id,
        stake_registration.credential_tag AS credential_tag,
        stake_registration.staking_key AS staking_key
 FROM utxo_pointer
 JOIN certs
   ON certs.slot = utxo_pointer.ptr_slot
  AND certs.cert_index = utxo_pointer.ptr_cert_index
 JOIN "transaction"
   ON "transaction".id = certs.transaction_id
  AND "transaction".block_index = utxo_pointer.ptr_tx_index
 JOIN stake_registration
   ON stake_registration.certificate_id = certs.id
 WHERE certs.slot <= ?
   AND NOT EXISTS (
     SELECT 1 FROM registration_events removal
     WHERE removal.registered = 0
       AND removal.credential_tag = stake_registration.credential_tag
       AND removal.staking_key = stake_registration.staking_key
       AND (removal.added_slot > certs.slot
         OR (removal.added_slot = certs.slot
           AND removal.block_index > "transaction".block_index)
         OR (removal.added_slot = certs.slot
           AND removal.block_index = "transaction".block_index
           AND removal.cert_index > certs.cert_index))
   )
)`, []any{slotValue}, nil
}
