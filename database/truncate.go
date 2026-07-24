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

package database

import (
	"fmt"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TruncateAfterSlot reverts all metadata rows and blob-referenced UTxO/
// transaction CBOR added strictly after point.Slot: certificates, account
// reward deltas, account/pool/DRep delegation state, protocol parameters,
// governance proposals/votes, constitutions, committee state, epochs,
// reward state, block nonces, network state/donations, and UTxOs/
// transactions. UTxOs spent after point.Slot are restored as unspent. It
// then sets the tip to point and returns the resulting tip and block nonce.
//
// mithrilFloor floors the UTxO/transaction deletion slot at the Mithril
// ledger boundary, if any (pass 0 if there is none). UTxOs produced by
// gap blocks during Mithril bootstrap are written via
// SetGapBlockTransaction without advancing the ledger tip, so their
// added_slot values can be well above the persisted tip; deleting below
// the Mithril boundary would bulk-delete every gap-block-produced UTxO,
// leaving the chain unable to validate the first post-gap block that
// consumes one of them. The Mithril snapshot is the trust anchor and is
// never rewound past, so the authoritative deletion slot is point.Slot or
// mithrilFloor, whichever is later.
//
// This is the shared metadata+blob truncation sweep used by both live
// ledger rollback (ledger.LedgerState.rollback, bounded by the security
// parameter) and offline/live database truncation (database/lifecycle,
// which may go far deeper for CIP-0135 disaster recovery). It performs no
// in-memory cache updates of its own — callers that hold additional
// in-memory state (epoch cache, era, protocol parameters, chain tip)
// must reload it themselves from the database after this returns
// successfully.
//
// If txn is nil, a new read-write transaction is opened and committed
// internally; otherwise the caller is responsible for committing txn.
func (d *Database) TruncateAfterSlot(
	point ocommon.Point,
	mithrilFloor uint64,
	txn *Txn,
) (ochainsync.Tip, []byte, error) {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}

	// Restore pool state before deleting any certificates: unlike account
	// restoration (which reads the Account row's own denormalized
	// AddedSlot field, independent of certificate rows),
	// RestorePoolStateAtSlot detects which pools need their denormalized
	// pledge/cost/margin/VRF/reward-account fields reverted by querying
	// PoolRegistration rows with added_slot > slot -- the very rows
	// DeleteCertificatesAfterSlot removes. Restoring first while those
	// rows still exist lets it correctly identify affected pools and
	// reload their fields from the surviving prior registration; deleting
	// certificates first (as this used to) leaves that query finding
	// nothing, silently keeping every re-registered pool's stale,
	// discarded values.
	if err := d.RestorePoolStateAtSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"restore pool state after rollback: %w",
			err,
		)
	}
	// Delete certificates (they reference transactions)
	if err := d.DeleteCertificatesAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete certificates after rollback: %w",
			err,
		)
	}
	// Revert reward-account changes before account restoration can delete
	// accounts registered after the rollback slot.
	if err := d.DeleteAccountRewardsAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete account reward deltas after rollback: %w",
			err,
		)
	}
	// Restore account delegation state
	if err := d.RestoreAccountStateAtSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"restore account state after rollback: %w",
			err,
		)
	}
	// Restore DRep state
	if err := d.RestoreDrepStateAtSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"restore DRep state after rollback: %w",
			err,
		)
	}
	// Delete rolled-back protocol parameters
	if err := d.DeletePParamsAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete protocol params after rollback: %w",
			err,
		)
	}
	// Delete rolled-back protocol parameter updates
	if err := d.DeletePParamUpdatesAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete protocol param updates after rollback: %w",
			err,
		)
	}
	// Delete rolled-back governance proposals
	if err := d.DeleteGovernanceProposalsAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete governance proposals after rollback: %w",
			err,
		)
	}
	// Delete rolled-back governance votes
	if err := d.DeleteGovernanceVotesAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete governance votes after rollback: %w",
			err,
		)
	}
	// Delete rolled-back constitutions
	if err := d.DeleteConstitutionsAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete constitutions after rollback: %w",
			err,
		)
	}
	// Delete rolled-back committee state
	if err := d.DeleteCommitteeMembersAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete committee state after rollback: %w",
			err,
		)
	}
	// Delete epoch entries whose nonces were computed from rolled-back
	// blocks. Epochs starting after the rollback slot used blocks that no
	// longer exist, so their nonces are stale and must be recomputed
	// during re-sync.
	if err := d.DeleteEpochsAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete epochs after rollback: %w",
			err,
		)
	}
	// Delete reward-state rows captured at epoch boundaries that no
	// longer exist on the selected chain.
	if err := d.DeleteRewardStateAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete reward state after rollback: %w",
			err,
		)
	}
	// Delete block nonce rows from the abandoned fork. Epoch nonces are
	// derived from slot-range block_nonce lookups, so same-slot
	// competitors and later fork rows must not survive rollback.
	if err := d.DeleteBlockNoncesAfterPoint(
		point,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete block nonces after rollback: %w",
			err,
		)
	}
	// Delete rolled-back network state records
	if err := d.DeleteNetworkStateAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete network state after rollback: %w",
			err,
		)
	}
	// Delete rolled-back treasury donation records
	if err := d.DeleteNetworkDonationsAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete network donations after rollback: %w",
			err,
		)
	}
	// Delete rolled-back UTxOs (blob offsets and metadata).
	//
	// Floor the deletion slot at mithrilFloor. UTxOs produced by gap
	// blocks during Mithril bootstrap are written via
	// SetGapBlockTransaction without advancing the ledger tip, so their
	// added_slot values are well above the persisted ledger tip. A
	// rollback whose target is below the Mithril boundary would
	// otherwise bulk-delete every gap-block-produced UTxO, leaving the
	// chain unable to validate the first post-gap block that consumes
	// one of them. The Mithril snapshot is the trust anchor — we never
	// rewind below it — so the authoritative deletion slot is the
	// rollback target or the Mithril boundary, whichever is later.
	deleteSlot := max(mithrilFloor, point.Slot)
	if err := d.UtxosDeleteRolledback(deleteSlot, txn); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"remove rolled-back UTxOs: %w",
			err,
		)
	}
	// Delete rolled-back transaction offsets and metadata
	if err := d.TransactionsDeleteRolledback(deleteSlot, txn); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"remove rolled-back transactions: %w",
			err,
		)
	}
	// Restore spent UTxOs. Use the same floored slot as the delete calls
	// above so gap-block transactions and the UTxOs they consumed stay
	// in sync: preserving a tx at slot S while restoring its consumed
	// UTxO at deleted_slot=S would leave the tx pointing at a live UTxO
	// it claims to have spent.
	if err := d.UtxosUnspend(deleteSlot, txn); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"restore spent UTxOs after rollback: %w",
			err,
		)
	}

	// Build new tip value
	newTip := ochainsync.Tip{
		Point: point,
	}
	var newNonce []byte
	if point.Slot > 0 {
		truncateBlock, err := BlockByPointTxn(txn, point)
		if err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"failed to get rollback block: %w",
				err,
			)
		}
		newTip.BlockNumber = truncateBlock.Number
		// Load nonce for rollback point
		newNonce, err = d.GetBlockNonce(point, txn)
		if err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"failed to get block nonce: %w",
				err,
			)
		}
	}
	// Write tip to DB
	if err := d.SetTip(newTip, txn); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"failed to set tip: %w",
			err,
		)
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"commit transaction: %w",
				err,
			)
		}
		owned = false
	}
	return newTip, newNonce, nil
}
