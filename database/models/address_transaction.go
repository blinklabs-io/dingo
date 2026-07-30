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

import (
	"fmt"
	"log/slog"

	"gorm.io/gorm"
)

// AddressTransaction maps an address (payment and/or staking key) to a
// transaction that references it as an input/output participant.
type AddressTransaction struct {
	ID uint `gorm:"primaryKey"`
	// PaymentKey backs bare payment-credential lookups (idx_addr_tx_payment,
	// regardless of staking part) and, as idx_addr_tx_stake_position's
	// trailing column, gives a deterministic tie-break between several
	// addresses that share one transaction under the same stake credential.
	PaymentKey    []byte `gorm:"index:idx_addr_tx_payment;size:28;index:idx_addr_tx_stake_position,priority:5"`
	StakingKey    []byte `gorm:"size:28;index:idx_addr_tx_stake_position,priority:2"`
	CredentialTag uint8  `gorm:"not null;default:0;index:idx_addr_tx_stake_position,priority:1"`
	TransactionID uint   `gorm:"index"`
	// Slot keeps its own single-column index (used by
	// DeleteAddressTransactionsAfterSlot's unscoped rollback cleanup, "WHERE
	// slot > ?" with no credential filter) in addition to participating in
	// idx_addr_tx_stake_position.
	Slot uint64 `gorm:"index;index:idx_addr_tx_stake_position,priority:3"`
	// TxIndex has no standalone index: it is only ever queried together with
	// the stake credential and slot, via idx_addr_tx_stake_position.
	TxIndex uint32 `gorm:"index:idx_addr_tx_stake_position,priority:4"`
}

func (AddressTransaction) TableName() string {
	return "address_transaction"
}

// MigrateAddressTransactionStakePositionIndex drops the legacy
// credential_tag/staking_key-only index (idx_addr_tx_staking) once
// idx_addr_tx_stake_position exists to take over its role: its leading two
// columns (credential_tag, staking_key) are an exact prefix match for every
// query idx_addr_tx_staking served (GetAddressesByCredential,
// GetTransactionsByAddress's credential-tag branch,
// GetAddressTransactionsByCredential), so nothing loses index coverage.
// address_transaction gets one row per (payment address, transaction) on
// every applied block, so leaving both indexes in place would cost write
// throughput for a lookup the wider index already covers.
//
// Callers MUST invoke this only after AutoMigrate has created
// idx_addr_tx_stake_position (AutoMigrate creates it on its own, since it
// is a new index name that did not previously exist). This function also
// verifies that precondition itself and is a no-op if the replacement
// index is not present yet, so that a caller mistake — or a future
// refactor that reorders migration steps — cannot leave the table with
// neither index: if a process were to crash between dropping the legacy
// index and AutoMigrate creating the replacement, address_transaction
// would have no index at all supporting credential lookups until the next
// startup retried both steps in the correct order.
func MigrateAddressTransactionStakePositionIndex(
	db *gorm.DB,
	logger *slog.Logger,
) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&AddressTransaction{}) {
		return nil
	}
	if !db.Migrator().HasIndex(&AddressTransaction{}, "idx_addr_tx_staking") {
		return nil
	}
	if !db.Migrator().HasIndex(&AddressTransaction{}, "idx_addr_tx_stake_position") {
		logger.Warn(
			"deferring address_transaction staking index cleanup: " +
				"idx_addr_tx_stake_position does not exist yet",
		)
		return nil
	}
	logger.Info(
		"dropping redundant address_transaction credential/staking index " +
			"now that idx_addr_tx_stake_position covers the same lookups",
	)
	if err := db.Migrator().DropIndex(
		&AddressTransaction{},
		"idx_addr_tx_staking",
	); err != nil {
		return fmt.Errorf("drop address_transaction staking index: %w", err)
	}
	return nil
}
