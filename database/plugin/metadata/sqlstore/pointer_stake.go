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

//nolint:sqlclosecheck // Cursors are explicitly closed and close errors are propagated before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
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
// boundarySlot names the epoch-boundary snapshot slot is +1 into (0 when the
// caller has no boundary, e.g. a plain "stake at slot" reconstruction). It
// matters only at the era cutover: cardano-ledger's hard-fork combinator
// translates the ledger state into the incoming era in extendToSlot, and SNAP
// runs inside TICK for the first slot of that incoming epoch -- after the
// translation. So the mark snapshot at a Babbage->Conway boundary is produced
// by ConwayInstantStake, under the *incoming* epoch's era, even though slot
// itself (the last slot of the outgoing epoch, boundarySlot-1) is still
// Babbage. Resolving the era from slot alone would over-attribute pointer
// stake for exactly that one snapshot per network -- the direction that
// loosens the Praos leader threshold. When boundarySlot is the later of the
// two, it is used for the era lookup instead; a plain "stake at slot" query
// (boundarySlot == 0) is unaffected and keeps resolving the era at slot.
//
// The era comes from the epoch containing that slot. An unknown era is treated
// as not counting: that is the behaviour before pointer resolution existed, so
// a missing epoch row understates rather than inflating a pool's snapshot
// stake and the shared active-stake denominator.
func pointerStakeCounted(
	ctx context.Context,
	db queryer,
	slot uint64,
	boundarySlot uint64,
) (bool, error) {
	eraSlot := slot
	if boundarySlot > eraSlot {
		eraSlot = boundarySlot
	}
	slotValue, err := checkedInt64(eraSlot)
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
		return false, fmt.Errorf("resolve era for slot %d: %w", eraSlot, err)
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
//
// A component above int64 is dropped rather than raised. Nothing validates an
// address's pointer payload, and gouroboros decodes each component with an
// unbounded shift-accumulate loop (AddressPayloadPointer.decode), so a
// spendable output can carry a position no int64 column can hold. Such a
// position names no certificate -- certs.slot, "transaction".block_index and
// certs.cert_index are int64 columns holding real chain values -- so it is
// dangling by construction, and dropping it leaves the output unattributed
// exactly as a pointer to an unoccupied position is. Returning an error here
// would instead fail the enclosing setUtxo and stall ingestion of a block the
// network accepted, which is the failure #3854 exists to avoid.
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
		return nil //nolint:nilerr // unrepresentable position: see above
	}
	txIndex, err := checkedInt64(pointer.TxIndex)
	if err != nil {
		return nil //nolint:nilerr // unrepresentable position: see above
	}
	certIndex, err := checkedInt64(pointer.CertIndex)
	if err != nil {
		return nil //nolint:nilerr // unrepresentable position: see above
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

// GetPointerStakeInputsForPools returns the additional per-credential stake
// held at a pointer address, for pools in poolKeyHashes and credentials
// resolved and delegated as of slot.
//
// It exists for the live snapshot path (calculateLiveStakeDistributionInTxn),
// which otherwise reads only reward_live_stake. reward_live_stake never
// carries pointer-derived UTxO stake: attribution is a function of
// certificate history at the slot being evaluated -- a registration or
// de-registration anywhere can change which credential an existing pointer
// output belongs to -- and reward_live_stake is an incrementally maintained
// aggregate keyed on (credential_tag, staking_key) with its own consistency
// verifier (RewardLiveStakeNeedsBackfill) that has no notion of "as of slot".
// Rather than teach that aggregate to react to registration/de-registration/
// era-translation events out of band, this recomputes the same
// activeDelegationSQL/pointerResolutionSQL join the historical fallback
// already uses, restricted to what the live aggregate is missing, and the
// caller adds the result to what GetLiveStakeInputsForPools returned.
//
// boundarySlot is threaded through to the era gate exactly as in
// historicalStakeCTE; see pointerStakeCounted. When the era at slot (or the
// boundary it belongs to) does not count pointer stake, this returns nil
// without issuing a query -- the live path's SQL and result stay exactly what
// they were before pointer resolution existed.
func (s *Store) GetPointerStakeInputsForPools(
	poolKeyHashes [][]byte,
	slot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetPointerStakeInputsForPools: resolve db: %w",
			err,
		)
	}
	counted, err := pointerStakeCounted(ctx, db, slot, boundarySlot)
	if err != nil {
		return nil, err
	}
	if !counted {
		return nil, nil
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	ret := make([]*models.RewardStakeInput, 0)
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		query, args := activeDelegationSQL(slot)
		resolution, resolutionArgs, err := pointerResolutionSQL(slot)
		if err != nil {
			return nil, err
		}
		query += resolution
		args = append(args, resolutionArgs...)
		expiryJoin := ""
		expiryPredicate := ""
		if expiryEpoch > 0 {
			// Same account.expiration_epoch gate GetLiveStakeInputsForPools
			// applies to the base-address side, so a pointer-attributed
			// credential is subject to the identical CIP-0163 rule as one
			// discovered by GetLiveStakeInputsForPools's live join, not the
			// historical fallback's witness-history reconstruction.
			expiryJoin = `
LEFT JOIN account acct
  ON acct.credential_tag = active_delegation.credential_tag
 AND acct.staking_key = active_delegation.staking_key`
			expiryPredicate = `(acct.expiration_epoch = 0
 OR acct.expiration_epoch >= ? OR acct.expiration_epoch IS NULL) AND `
		}
		query += `
SELECT active_delegation.pool_key_hash, active_delegation.credential_tag,
       active_delegation.staking_key, utxo.amount AS utxo_amount
FROM active_delegation
JOIN pointer_resolution
  ON pointer_resolution.credential_tag = active_delegation.credential_tag
 AND pointer_resolution.staking_key = active_delegation.staking_key
JOIN utxo
  ON utxo.id = pointer_resolution.utxo_id
 AND utxo.added_slot <= ?
 AND (utxo.deleted_slot = 0 OR utxo.deleted_slot > ?)
` + expiryJoin + `
WHERE ` + expiryPredicate + `active_delegation.pool_key_hash IN (` +
			bindPlaceholders(end-start) + `)`
		args = append(args, slot, slot)
		if expiryEpoch > 0 {
			args = append(args, expiryEpoch)
		}
		args = append(args, byteSliceArgs(poolKeyHashes[start:end])...)

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("query pointer stake inputs: %w", err)
		}
		type pointerKey struct {
			pool string
			tag  uint8
			key  string
		}
		amounts := make(map[pointerKey]uint64)
		for rows.Next() {
			var pool, key []byte
			var tag uint8
			var rawAmount sql.NullString
			if err := rows.Scan(&pool, &tag, &key, &rawAmount); err != nil {
				rows.Close()
				return nil, err
			}
			ref := pointerKey{pool: string(pool), tag: tag, key: string(key)}
			if _, ok := amounts[ref]; !ok {
				amounts[ref] = 0
			}
			if rawAmount.Valid && rawAmount.String != "" {
				value, err := parseUint64(
					"pointer stake overlay UTxO amount",
					rawAmount.String,
				)
				if err != nil {
					rows.Close()
					return nil, err
				}
				if ^uint64(0)-amounts[ref] < value {
					rows.Close()
					return nil, fmt.Errorf(
						"pointer stake overlay overflow for credential %d:%x",
						tag,
						key,
					)
				}
				amounts[ref] += value
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		for ref, amount := range amounts {
			if amount == 0 {
				continue
			}
			ret = append(ret, &models.RewardStakeInput{
				PoolKeyHash:   []byte(ref.pool),
				CredentialTag: ref.tag,
				StakingKey:    []byte(ref.key),
				Stake:         types.Uint64(amount),
				Registered:    true,
			})
		}
	}
	return ret, nil
}
