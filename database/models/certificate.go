// Copyright 2025 Blink Labs Software
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
	"database/sql/driver"
	"fmt"
	"strconv"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// CertificateType represents a certificate type with type safety
//
//nolint:recvcheck
type CertificateType uint

// Certificate type constants using gouroboros constants
const (
	CertificateTypeStakeRegistration CertificateType = CertificateType(
		lcommon.CertificateTypeStakeRegistration,
	)
	CertificateTypeStakeDeregistration CertificateType = CertificateType(
		lcommon.CertificateTypeStakeDeregistration,
	)
	CertificateTypeStakeDelegation CertificateType = CertificateType(
		lcommon.CertificateTypeStakeDelegation,
	)
	CertificateTypePoolRegistration CertificateType = CertificateType(
		lcommon.CertificateTypePoolRegistration,
	)
	CertificateTypePoolRetirement CertificateType = CertificateType(
		lcommon.CertificateTypePoolRetirement,
	)
	CertificateTypeRegistration CertificateType = CertificateType(
		lcommon.CertificateTypeRegistration,
	)
	CertificateTypeDeregistration CertificateType = CertificateType(
		lcommon.CertificateTypeDeregistration,
	)
	CertificateTypeStakeRegistrationDelegation CertificateType = CertificateType(
		lcommon.CertificateTypeStakeRegistrationDelegation,
	)
	CertificateTypeVoteDelegation CertificateType = CertificateType(
		lcommon.CertificateTypeVoteDelegation,
	)
	CertificateTypeStakeVoteDelegation CertificateType = CertificateType(
		lcommon.CertificateTypeStakeVoteDelegation,
	)
	CertificateTypeVoteRegistrationDelegation CertificateType = CertificateType(
		lcommon.CertificateTypeVoteRegistrationDelegation,
	)
	CertificateTypeStakeVoteRegistrationDelegation CertificateType = CertificateType(
		lcommon.CertificateTypeStakeVoteRegistrationDelegation,
	)
	CertificateTypeAuthCommitteeHot CertificateType = CertificateType(
		lcommon.CertificateTypeAuthCommitteeHot,
	)
	CertificateTypeResignCommitteeCold CertificateType = CertificateType(
		lcommon.CertificateTypeResignCommitteeCold,
	)
	CertificateTypeRegistrationDrep CertificateType = CertificateType(
		lcommon.CertificateTypeRegistrationDrep,
	)
	CertificateTypeDeregistrationDrep CertificateType = CertificateType(
		lcommon.CertificateTypeDeregistrationDrep,
	)
	CertificateTypeUpdateDrep CertificateType = CertificateType(
		lcommon.CertificateTypeUpdateDrep,
	)
	CertificateTypeGenesisKeyDelegation CertificateType = CertificateType(
		lcommon.CertificateTypeGenesisKeyDelegation,
	)
	CertificateTypeMoveInstantaneousRewards CertificateType = CertificateType(
		lcommon.CertificateTypeMoveInstantaneousRewards,
	)
	CertificateTypeLeiosEb CertificateType = CertificateType(
		lcommon.CertificateTypeLeiosEb,
	)
)

// maxCertificateTypeValue represents the maximum valid certificate type value
// Based on known Cardano certificate types from gouroboros library
const maxCertificateTypeValue = 1000

// Value implements the driver.Valuer interface for database storage.
// Use a value receiver so both CertificateType and *CertificateType satisfy driver.Valuer.
func (ct CertificateType) Value() (driver.Value, error) {
	return int64(ct), nil // #nosec G115 - certificate types are small integers
}

// Scan implements the sql.Scanner interface for database retrieval.
// Uses pointer receiver because it modifies the receiver.
func (ct *CertificateType) Scan(value interface{}) error {
	if value == nil {
		*ct = 0
		return nil
	}
	var val int64
	switch v := value.(type) {
	case uint:
		if v > 1<<63-1 { // Check for overflow before conversion
			return fmt.Errorf("uint value too large for CertificateType: %d", v)
		}
		val = int64(v)
	case int:
		val = int64(v)
	case int64:
		val = v
	case uint64:
		if v > 1<<63-1 { // Check for overflow before conversion
			return fmt.Errorf("uint64 value too large for CertificateType: %d", v)
		}
		val = int64(v)
	case []byte:
		// Handle byte slice inputs (common for some database drivers)
		var err error
		val, err = strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse []byte as int64 for CertificateType: %w", err)
		}
	case string:
		// Handle string inputs (in case a driver hands back text instead of []byte)
		var err error
		val, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse string as int64 for CertificateType: %w", err)
		}
	default:
		return fmt.Errorf("cannot scan %T into CertificateType", value)
	}
	// Basic range check for certificate types (should be small positive integers)
	if val < 0 || val > maxCertificateTypeValue {
		return fmt.Errorf("invalid certificate type value: %d", val)
	}
	*ct = CertificateType(val)
	return nil
}

// Certificate maps transaction certificates to their specialized table records.
// Provides unified indexing across all certificate types without requiring joins.
//
// All certificate types now have dedicated specialized models. The CertificateID field
// references the ID of the specific certificate record based on CertType.
type Certificate struct {
	BlockHash     []byte          `gorm:"index"`
	ID            uint            `gorm:"primaryKey"`
	TransactionID uint            `gorm:"index;uniqueIndex:uniq_tx_cert;constraint:OnDelete:CASCADE"`
	CertificateID uint            `gorm:"index"` // Polymorphic FK to certificate table based on CertType. Not DB-enforced.
	Slot          uint64          `gorm:"index"`
	CertIndex     uint            `gorm:"column:cert_index;uniqueIndex:uniq_tx_cert"`
	CertType      CertificateType `gorm:"index"`
}

// TableName returns the database table name for the Certificate model.
func (Certificate) TableName() string {
	return "certificates"
}

// Type returns the certificate type.
func (c Certificate) Type() CertificateType {
	return c.CertType
}
