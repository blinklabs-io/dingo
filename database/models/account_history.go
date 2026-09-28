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

package models

import "math/big"

// AccountDelegationHistoryRow holds delegation history
// query results for a stake account.
type AccountDelegationHistoryRow struct {
	AddedSlot   uint64
	BlockIndex  uint32
	CertIndex   uint32
	TxHash      []byte
	PoolKeyHash []byte
	// TxSlot is the slot of the transaction containing the
	// delegation certificate.
	TxSlot uint64
	// BlockHash is the hash of the block containing the
	// delegation certificate's transaction. The block height is
	// resolved from the block store, which is not part of the
	// metadata SQL schema.
	BlockHash []byte
}

// AccountRegistrationHistoryRow holds registration
// history query results for a stake account.
type AccountRegistrationHistoryRow struct {
	AddedSlot  uint64
	BlockIndex uint32
	CertIndex  uint32
	TxHash     []byte
	Action     string
	// Deposit is the registration deposit (or refund, for
	// deregistrations) in lovelace. Zero for certificate types
	// that do not record an explicit deposit.
	//
	// Nil means the deposit is *unknown*: the certificate type does record
	// one, but it could not be computed when the certificate was ingested
	// (an era whose CertDepositFunc rejected the active protocol
	// parameters, or a backfill that could not resolve them), so the column
	// is NULL. Nil is not the same answer as zero, and callers must not
	// substitute the current protocol-parameter value for it -- see
	// AccountImportRegistration.Deposit for the same rule on baselines.
	// A genuinely recorded zero (KeyDeposit was 0, as on the devnet) is a
	// non-nil zero.
	Deposit *uint64
	// TxSlot is the slot of the transaction containing the
	// (de)registration certificate.
	TxSlot uint64
	// BlockHash is the hash of the block containing the
	// (de)registration certificate's transaction. The block
	// height is resolved from the block store, which is not part
	// of the metadata SQL schema.
	BlockHash []byte
}

// AccountImportRegistration is the virtual registration state established by
// a snapshot import or Shelley genesis delegation. No registration certificate
// exists in local history, so AddedSlot is the baseline boundary and Deposit
// may be nil for a legacy baseline whose import discarded the historical value.
type AccountImportRegistration struct {
	AddedSlot uint64
	Deposit   *uint64
}

// AccountWithdrawalHistoryRow holds withdrawal history
// query results for a stake account.
type AccountWithdrawalHistoryRow struct {
	TxHash []byte
	Amount uint64
	// TxSlot is the slot of the transaction containing the
	// withdrawal.
	TxSlot uint64
	// BlockIndex is the withdrawal transaction's position within
	// its containing block, used as an ordering tie-break for
	// transactions sharing a slot.
	BlockIndex uint32
	// BlockHash is the hash of the block containing the
	// withdrawal transaction. The block height is resolved from
	// the block store, which is not part of the metadata SQL
	// schema.
	BlockHash []byte
}

// AddressTransactionPosition is an inclusive (slot, tx_index) boundary
// used to filter address_transaction rows for the Blockfrost account
// transactions endpoint's from/to block-range query. Both fields are
// compared as one tuple: a row qualifies as a lower bound when its (slot,
// tx_index) is greater than or equal to this position, and as an upper
// bound when it is less than or equal to it.
type AddressTransactionPosition struct {
	Slot    uint64
	TxIndex uint32
}

// AccountTransactionAssociationRow holds one (payment address,
// transaction) association row for a stake credential, backing the
// Blockfrost account transactions endpoint. It is the direct, final page
// of results: ordering, the from/to range filter, and LIMIT/OFFSET are
// all applied in SQL, so building a response from these rows does not
// require any further application-level fan-out or filtering.
type AccountTransactionAssociationRow struct {
	PaymentKey []byte
	TxHash     []byte
	// TxSlot and TxIndex are the transaction's position, used for
	// ordering and from/to range filtering.
	TxSlot  uint64
	TxIndex uint32
	// BlockHash is the hash of the block containing the transaction. The
	// block height is resolved from the block store, which is not part
	// of the metadata SQL schema.
	BlockHash []byte
}

// AccountSums holds aggregated lovelace totals for a stake
// account, summed from persisted withdrawal and MIR state.
type AccountSums struct {
	// WithdrawalsSum is the total of all reward withdrawals
	// made by the account. Withdrawals are coin, so this total
	// is unsigned.
	WithdrawalsSum uint64
	// ReservesSum is the signed total of all MIR deltas for the
	// account sourced from the reserves pot. MIR deltas are
	// delta_coin, so individual rows may be negative and the
	// total is summed as a signed value. Never nil.
	ReservesSum *big.Int
	// TreasurySum is the signed total of all MIR deltas for the
	// account sourced from the treasury pot. See ReservesSum.
	TreasurySum *big.Int
}

// NewAccountSums returns an AccountSums with every total at zero and neither
// signed total nil. Readers return it on their failure paths too, so the
// non-nil guarantee does not depend on the caller checking the error first.
func NewAccountSums() AccountSums {
	return AccountSums{
		ReservesSum: new(big.Int),
		TreasurySum: new(big.Int),
	}
}
