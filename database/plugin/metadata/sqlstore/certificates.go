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

//nolint:sqlclosecheck // Cursors are explicitly closed before issuing dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

var rollbackCertificateTables = []string{
	"stake_registration",
	"stake_delegation",
	"stake_deregistration",
	"stake_registration_delegation",
	"stake_vote_delegation",
	"stake_vote_registration_delegation",
	"vote_delegation",
	"vote_registration_delegation",
	"registration",
	"deregistration",
	"pool_registration",
	"pool_retirement",
	"registration_drep",
	"deregistration_drep",
	"update_drep",
	"auth_committee_hot",
	"resign_committee_cold",
	"move_instantaneous_rewards",
	"genesis_delegation",
}

func (s *Store) DeleteCertificatesAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			if _, err := db.ExecContext(context.Background(), `
DELETE FROM pool_registration_owner
WHERE pool_registration_id IN (
    SELECT id FROM pool_registration WHERE added_slot > ?
)`,
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(context.Background(), `
DELETE FROM pool_registration_relay
WHERE pool_registration_id IN (
    SELECT id FROM pool_registration WHERE added_slot > ?
)`,
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(context.Background(), `
DELETE FROM move_instantaneous_rewards_reward
WHERE mir_id IN (
    SELECT id FROM move_instantaneous_rewards WHERE added_slot > ?
)`,
				slot,
			); err != nil {
				return err
			}
			for _, table := range rollbackCertificateTables {
				if _, err := db.ExecContext(
					context.Background(),
					"DELETE FROM "+table+" WHERE added_slot > ?",
					slot,
				); err != nil {
					return fmt.Errorf("delete %s after slot: %w", table, err)
				}
			}
			_, err := db.ExecContext(
				context.Background(),
				"DELETE FROM certs WHERE slot > ?",
				slot,
			)
			return err
		},
	)
}

func (s *Store) GetMIRCertsInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) ([]models.MIREffect, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("GetMIRCertsInSlotRange: resolve db: %w", err)
	}
	rows, err := db.QueryContext(context.Background(), `
SELECT id, pot, other_pot
FROM move_instantaneous_rewards
WHERE added_slot >= ? AND added_slot < ?
ORDER BY added_slot ASC, id ASC`,
		startSlot,
		endSlot,
	)
	if err != nil {
		return nil, fmt.Errorf("GetMIRCertsInSlotRange: query: %w", err)
	}
	effects := []models.MIREffect{}
	for rows.Next() {
		var effect models.MIREffect
		var otherPot sql.NullString
		if err := rows.Scan(
			&effect.ID,
			&effect.Pot,
			&otherPot,
		); err != nil {
			rows.Close()
			return nil, err
		}
		effect.OtherPot, err = parseNullUint64("MIR other pot", otherPot)
		if err != nil {
			rows.Close()
			return nil, err
		}
		effect.Rewards = []models.MIRReward{}
		effects = append(effects, effect)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range effects {
		rewardRows, err := db.QueryContext(context.Background(), `
SELECT credential, credential_tag, amount
FROM move_instantaneous_rewards_reward
WHERE mir_id = ?
ORDER BY id`,
			effects[i].ID,
		)
		if err != nil {
			return nil, err
		}
		for rewardRows.Next() {
			var reward models.MIRReward
			var amount sql.NullString
			if err := rewardRows.Scan(
				&reward.Credential,
				&reward.CredentialTag,
				&amount,
			); err != nil {
				rewardRows.Close()
				return nil, err
			}
			reward.Amount, err = parseNullUint64("MIR reward", amount)
			if err != nil {
				rewardRows.Close()
				return nil, err
			}
			effects[i].Rewards = append(effects[i].Rewards, reward)
		}
		if err := rewardRows.Close(); err != nil {
			return nil, err
		}
		if err := rewardRows.Err(); err != nil {
			return nil, err
		}
	}
	return effects, nil
}

func (s *Store) GetGenesisDelegationForSlot(
	genesisHash []byte,
	blockSlot uint64,
	txn types.Txn,
) (*models.GenesisDelegation, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	var ret models.GenesisDelegation
	err = db.QueryRowContext(context.Background(), `
SELECT id, genesis_hash, genesis_delegate_hash, vrf_key_hash, added_slot,
       block_index, cert_index, certificate_id
FROM genesis_delegation
WHERE genesis_hash = ? AND added_slot < ?
ORDER BY added_slot DESC, block_index DESC, cert_index DESC, id DESC
LIMIT 1`,
		genesisHash,
		blockSlot,
	).Scan(
		&ret.ID,
		&ret.GenesisHash,
		&ret.GenesisDelegateHash,
		&ret.VrfKeyHash,
		&ret.AddedSlot,
		&ret.BlockIndex,
		&ret.CertIndex,
		&ret.CertificateID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &ret, err
}
