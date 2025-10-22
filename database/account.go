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

package database

import (
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// GetAccount returns an account by staking key
func (d *Database) GetAccount(
	stakeKey []byte,
	txn *Txn,
) (*models.Account, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	account, err := d.metadata.GetAccount(stakeKey, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, models.ErrAccountNotFound
	}
	return account, nil
}

// SetDeregistration saves a deregistration certificate
func (d *Database) SetDeregistration(
	cert *lcommon.DeregistrationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetDeregistration(
		cert,
		slot,
		txn.Metadata(),
	)
}

// SetRegistration saves a registration certificate
func (d *Database) SetRegistration(
	cert *lcommon.RegistrationCertificate,
	slot, deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetRegistration(
		cert,
		slot,
		deposit,
		txn.Metadata(),
	)
}

// SetStakeDelegation saves a stake delegation certificate
func (d *Database) SetStakeDelegation(
	cert *lcommon.StakeDelegationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeDelegation(
		cert,
		slot,
		txn.Metadata(),
	)
}

// SetStakeDeregistration saves a stake deregistration certificate
func (d *Database) SetStakeDeregistration(
	cert *lcommon.StakeDeregistrationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeDeregistration(
		cert,
		slot,
		txn.Metadata(),
	)
}

// SetStakeRegistration saves a stake registration certificate
func (d *Database) SetStakeRegistration(
	cert *lcommon.StakeRegistrationCertificate,
	slot, deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeRegistration(
		cert,
		slot,
		deposit,
		txn.Metadata(),
	)
}

// SetStakeRegistrationDelegation saves a stake registration delegation certificate
func (d *Database) SetStakeRegistrationDelegation(
	cert *lcommon.StakeRegistrationDelegationCertificate,
	slot, deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeRegistrationDelegation(
		cert,
		slot,
		deposit,
		txn.Metadata(),
	)
}

// SetStakeVoteDelegation saves a stake vote delegation certificate
func (d *Database) SetStakeVoteDelegation(
	cert *lcommon.StakeVoteDelegationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeVoteDelegation(
		cert,
		slot,
		txn.Metadata(),
	)
}

// SetStakeVoteRegistrationDelegation saves a stake vote registration delegation certificate
func (d *Database) SetStakeVoteRegistrationDelegation(
	cert *lcommon.StakeVoteRegistrationDelegationCertificate,
	slot, deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeVoteRegistrationDelegation(
		cert,
		slot,
		deposit,
		txn.Metadata(),
	)
}

// SetVoteDelegation saves a vote delegation certificate
func (d *Database) SetVoteDelegation(
	cert *lcommon.VoteDelegationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetVoteDelegation(
		cert,
		slot,
		txn.Metadata(),
	)
}

// SetVoteRegistrationDelegation saves a vote registration delegation certificate
func (d *Database) SetVoteRegistrationDelegation(
	cert *lcommon.VoteRegistrationDelegationCertificate,
	slot, deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetVoteRegistrationDelegation(
		cert,
		slot,
		deposit,
		txn.Metadata(),
	)
}
