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
	"github.com/blinklabs-io/dingo/database/models"
)

// GetCommitteeMember returns a committee member by cold key
func (d *Database) GetCommitteeMember(
	coldKey []byte,
	txn *Txn,
) (*models.AuthCommitteeHot, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	ret, err := d.metadata.GetCommitteeMember(coldKey, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, models.ErrCommitteeMemberNotFound
	}
	return ret, nil
}

// GetActiveCommitteeMembers returns all active committee members
func (d *Database) GetActiveCommitteeMembers(
	txn *Txn,
) ([]*models.AuthCommitteeHot, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.GetActiveCommitteeMembers(txn.Metadata())
}

// IsCommitteeMemberResigned checks if a committee member has resigned
func (d *Database) IsCommitteeMemberResigned(
	coldKey []byte,
	txn *Txn,
) (bool, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.IsCommitteeMemberResigned(coldKey, txn.Metadata())
}

// GetCommitteeActiveCount returns the number of active (non-resigned)
// committee members.
func (d *Database) GetCommitteeActiveCount(
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.GetCommitteeActiveCount(txn.Metadata())
}
