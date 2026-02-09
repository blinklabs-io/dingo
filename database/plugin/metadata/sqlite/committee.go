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

package sqlite

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
)

// GetCommitteeMember retrieves a committee member by cold key.
// Returns the latest authorization (ordered by added_slot DESC).
func (d *MetadataStoreSqlite) GetCommitteeMember(
	coldKey []byte,
	txn types.Txn,
) (*models.AuthCommitteeHot, error) {
	var member models.AuthCommitteeHot
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"cold_credential = ?",
		coldKey,
	).Order("added_slot DESC, certificate_id DESC").First(&member); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &member, nil
}

// GetActiveCommitteeMembers retrieves all active committee members.
// Returns only the latest authorization per cold key, and excludes members
// whose latest resignation is after their latest authorization.
func (d *MetadataStoreSqlite) GetActiveCommitteeMembers(
	txn types.Txn,
) ([]*models.AuthCommitteeHot, error) {
	var members []*models.AuthCommitteeHot
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	// Use a subquery to get only the latest authorization per cold_credential,
	// then filter out members whose latest resignation is after their latest authorization.
	// Uses (added_slot, certificate_id) for deterministic ordering based on global certificate order.
	if result := db.Raw(`
		SELECT a.*
		FROM auth_committee_hot a
		WHERE NOT EXISTS (
			SELECT 1 FROM auth_committee_hot a2
			WHERE a2.cold_credential = a.cold_credential
			AND (a2.added_slot > a.added_slot OR (a2.added_slot = a.added_slot AND a2.certificate_id > a.certificate_id))
		)
		AND NOT EXISTS (
			SELECT 1 FROM resign_committee_cold r
			WHERE r.cold_credential = a.cold_credential
			AND (r.added_slot > a.added_slot OR (r.added_slot = a.added_slot AND r.certificate_id > a.certificate_id))
		)
	`).Scan(&members); result.Error != nil {
		return nil, result.Error
	}
	return members, nil
}

// IsCommitteeMemberResigned checks if a committee member has resigned.
// Returns true only if the latest resignation is after the latest authorization
// (handles resign-then-rejoin scenarios). Uses (added_slot, id) for deterministic
// ordering when events occur in the same block.
func (d *MetadataStoreSqlite) IsCommitteeMemberResigned(
	coldKey []byte,
	txn types.Txn,
) (bool, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return false, err
	}

	// Get the latest authorization for this cold key
	var latestAuth models.AuthCommitteeHot
	if result := db.Where("cold_credential = ?", coldKey).
		Order("added_slot DESC, certificate_id DESC").
		First(&latestAuth); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			// If no authorization exists, the member doesn't exist (not resigned)
			return false, nil
		}
		return false, result.Error
	}

	// Get the latest resignation for this cold key
	var latestResign models.ResignCommitteeCold
	if result := db.Where("cold_credential = ?", coldKey).
		Order("added_slot DESC, certificate_id DESC").
		First(&latestResign); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			// No resignation exists, so not resigned
			return false, nil
		}
		return false, result.Error
	}

	// Resigned if latest resignation is after latest authorization
	// Compare by (added_slot, certificate_id) for deterministic ordering based on global certificate order
	if latestResign.AddedSlot > latestAuth.AddedSlot {
		return true, nil
	}
	if latestResign.AddedSlot == latestAuth.AddedSlot && latestResign.CertificateID > latestAuth.CertificateID {
		return true, nil
	}
	return false, nil
}
