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

package sqlstore

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

func addDrepDelegator(
	ctx context.Context,
	db queryer,
	drepTag uint8,
	drepCredential []byte,
	stakeTag uint8,
	stakeCredential []byte,
	slot uint64,
) error {
	addedSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
INSERT INTO drep_delegator (
    drep_credential_tag, drep_credential, stake_credential_tag,
    stake_credential, added_slot
)
SELECT ?, ?, ?, ?, ?
WHERE EXISTS (
    SELECT 1 FROM drep
    WHERE credential_tag = ? AND credential = ? AND active = TRUE
)
AND NOT EXISTS (
    SELECT 1 FROM drep_delegator
    WHERE drep_credential_tag = ? AND drep_credential = ?
      AND stake_credential_tag = ? AND stake_credential = ?
      AND removed_slot IS NULL
)`,
		drepTag,
		drepCredential,
		stakeTag,
		stakeCredential,
		addedSlot,
		drepTag,
		drepCredential,
		drepTag,
		drepCredential,
		stakeTag,
		stakeCredential,
	)
	return err
}

func removeDrepDelegator(
	ctx context.Context,
	db queryer,
	drepTag uint8,
	drepCredential []byte,
	stakeTag uint8,
	stakeCredential []byte,
	slot uint64,
) error {
	removedSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
UPDATE drep_delegator SET removed_slot = ?
WHERE drep_credential_tag = ? AND drep_credential = ?
  AND stake_credential_tag = ? AND stake_credential = ?
  AND removed_slot IS NULL`,
		removedSlot,
		drepTag,
		drepCredential,
		stakeTag,
		stakeCredential,
	)
	return err
}

func removeAllDrepDelegators(
	ctx context.Context,
	db queryer,
	drepTag uint8,
	drepCredential []byte,
	slot uint64,
) error {
	removedSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, `
SELECT stake_credential_tag, stake_credential
FROM drep_delegator
WHERE drep_credential_tag = ? AND drep_credential = ?
  AND removed_slot IS NULL`,
		drepTag,
		drepCredential,
	)
	if err != nil {
		return err
	}
	type delegator struct {
		tag uint8
		key []byte
	}
	delegators := make([]delegator, 0)
	for rows.Next() {
		var item delegator
		if err := rows.Scan(&item.tag, &item.key); err != nil {
			rows.Close()
			return err
		}
		delegators = append(delegators, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `
UPDATE drep_delegator SET removed_slot = ?
WHERE drep_credential_tag = ? AND drep_credential = ?
  AND removed_slot IS NULL`, removedSlot, drepTag, drepCredential); err != nil {
		return fmt.Errorf("remove reverse DRep delegations: %w", err)
	}
	for _, item := range delegators {
		if _, err := db.ExecContext(ctx, `
UPDATE account SET drep = NULL, drep_type = 0, added_slot = ?
WHERE credential_tag = ? AND staking_key = ?`,
			removedSlot,
			item.tag,
			item.key,
		); err != nil {
			return fmt.Errorf("clear DRep delegator account: %w", err)
		}
	}
	return nil
}

func insertImportedDrepDelegators(
	ctx context.Context,
	db queryer,
	drep *models.Drep,
) error {
	for _, delegator := range drep.Delegators {
		if err := addDrepDelegator(
			ctx,
			db,
			drep.CredentialTag,
			drep.Credential,
			delegator.Tag,
			delegator.Key,
			drep.AddedSlot,
		); err != nil {
			return fmt.Errorf("import DRep delegator: %w", err)
		}
	}
	return nil
}
