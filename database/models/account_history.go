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
	Deposit uint64
	// TxSlot is the slot of the transaction containing the
	// (de)registration certificate.
	TxSlot uint64
	// BlockHash is the hash of the block containing the
	// (de)registration certificate's transaction. The block
	// height is resolved from the block store, which is not part
	// of the metadata SQL schema.
	BlockHash []byte
}

// AccountSums holds aggregated lovelace totals for a stake
// account, summed from persisted withdrawal and MIR state.
type AccountSums struct {
	// WithdrawalsSum is the total of all reward withdrawals
	// made by the account.
	WithdrawalsSum uint64
	// ReservesSum is the total of all MIR transfers to the
	// account sourced from the reserves pot.
	ReservesSum uint64
	// TreasurySum is the total of all MIR transfers to the
	// account sourced from the treasury pot.
	TreasurySum uint64
}
