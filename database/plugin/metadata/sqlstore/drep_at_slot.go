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

//nolint:rowserrcheck,sqlclosecheck // Cursors are explicitly closed and close errors are propagated before dependent queries.
package sqlstore

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) GetDrepsAtSlot(
	refs []models.StakeCredentialRef,
	slot uint64,
	txn types.Txn,
) ([]*models.Drep, error) {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	var candidates []*models.Drep
	if len(refs) == 0 {
		// Every row, inactive ones included: a DRep deregistered after slot
		// was active at it.
		rows, err := db.QueryContext(ctx, `
SELECT anchor_url, credential, anchor_hash, id, added_slot, credential_tag,
       last_activity_epoch, expiry_epoch, active
FROM drep`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var row sqlitequery.Drep
			if err := rows.Scan(
				&row.AnchorUrl,
				&row.Credential,
				&row.AnchorHash,
				&row.ID,
				&row.AddedSlot,
				&row.CredentialTag,
				&row.LastActivityEpoch,
				&row.ExpiryEpoch,
				&row.Active,
			); err != nil {
				rows.Close()
				return nil, err
			}
			candidates = append(candidates, drepFromSQLite(row))
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	} else {
		for _, ref := range refs {
			drep, err := s.GetDrepByCredential(ref.Tag, ref.Key, true, txn)
			if err != nil {
				if errors.Is(err, models.ErrDrepNotFound) {
					continue
				}
				return nil, err
			}
			if drep != nil {
				candidates = append(candidates, drep)
			}
		}
	}
	// Activity renews expiry without touching drep.added_slot, so a row
	// last written at or before slot is exact there only when its expiry
	// history holds nothing later either.
	renewed := make(map[string]struct{})
	rows, err := db.QueryContext(ctx, `
SELECT DISTINCT credential_tag, credential
FROM drep_expiry_history WHERE added_slot > ?`,
		slotValue,
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tag uint8
		var credential []byte
		if err := rows.Scan(&tag, &credential); err != nil {
			rows.Close()
			return nil, err
		}
		renewed[models.NewStakeCredentialRef(tag, credential).MapKey()] = struct{}{}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ret := make([]*models.Drep, 0, len(candidates))
	for _, drep := range candidates {
		key := models.NewStakeCredentialRef(
			drep.CredentialTag,
			drep.Credential,
		).MapKey()
		if _, ok := renewed[key]; !ok && drep.AddedSlot <= slot {
			if drep.Active {
				ret = append(ret, drep)
			}
			continue
		}
		state, found, err := deriveDrepStateAtSlot(
			ctx,
			db,
			drep.CredentialTag,
			drep.Credential,
			slot,
		)
		if err != nil {
			return nil, err
		}
		if !found || !state.active {
			continue
		}
		atSlot := *drep
		atSlot.Active = true
		atSlot.AnchorURL = state.anchorURL
		atSlot.AnchorHash = state.anchorHash
		atSlot.AddedSlot = state.latestSlot
		// The same rule RestoreDrepStateAtSlot applies when the expiry
		// history has nothing at or before slot.
		if state.hasExpiry || state.registrationSlot != 0 {
			atSlot.LastActivityEpoch = state.lastActivity
			atSlot.ExpiryEpoch = state.expiry
		}
		ret = append(ret, &atSlot)
	}
	return ret, nil
}

func (s *Store) GetDrepRegistrationDepositAtSlot(
	credentialTag uint8,
	credential []byte,
	slot uint64,
	txn types.Txn,
) (*uint64, error) {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	var raw sql.NullString
	err = db.QueryRowContext(ctx, `
SELECT deposit_amount
FROM registration_drep
WHERE credential_tag = ? AND drep_credential = ? AND added_slot <= ?
ORDER BY added_slot DESC
LIMIT 1`,
		credentialTag,
		credential,
		slotValue,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get drep registration deposit at slot: %w", err)
	}
	if !raw.Valid {
		return nil, nil
	}
	deposit, err := parseUint64("drep registration deposit", raw.String)
	if err != nil {
		return nil, err
	}
	return &deposit, nil
}

func (s *Store) GetDRepDelegatorsAtSlot(
	dreps []models.StakeCredentialRef,
	slot uint64,
	txn types.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]struct{}, len(dreps))
	for _, drep := range dreps {
		wanted[drep.MapKey()] = struct{}{}
	}
	ret := make(map[string][]models.StakeCredentialRef)
	add := func(tag uint8, key []byte, drepType uint64, drep []byte) {
		if len(drep) == 0 || drepType > models.DrepTypeScriptHash {
			return
		}
		drepKey := models.NewStakeCredentialRef(uint8(drepType), drep).MapKey()
		if len(wanted) > 0 {
			if _, ok := wanted[drepKey]; !ok {
				return
			}
		}
		ret[drepKey] = append(ret[drepKey], models.NewStakeCredentialRef(tag, key))
	}
	// Rows written at or before slot hold their state there (see
	// GetAccountsByCredentialAtSlot).
	liveQuery := `
SELECT credential_tag, staking_key, drep_type, drep
FROM account
WHERE active = TRUE AND drep IS NOT NULL AND drep_type <= 1
  AND added_slot <= ?`
	liveArgs := [][]any{{slotValue}}
	if len(dreps) > 0 {
		liveQuery += " AND drep_type = ? AND drep = ?"
		liveArgs = liveArgs[:0]
		for _, drep := range dreps {
			liveArgs = append(liveArgs, []any{slotValue, drep.Tag, drep.Key})
		}
	}
	for _, args := range liveArgs {
		rows, err := db.QueryContext(ctx, liveQuery, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tag uint8
			var key, drep []byte
			var drepType uint64
			if err := rows.Scan(&tag, &key, &drepType, &drep); err != nil {
				rows.Close()
				return nil, err
			}
			add(tag, key, drepType, drep)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	rows, err := db.QueryContext(ctx, `
SELECT credential_tag, staking_key, created_slot, drep_type, drep
FROM account WHERE added_slot > ?`,
		slotValue,
	)
	if err != nil {
		return nil, err
	}
	type changedAccount struct {
		tag         uint8
		key         []byte
		createdSlot uint64
		drepType    uint64
		drep        []byte
	}
	var changed []changedAccount
	for rows.Next() {
		var account changedAccount
		var drepType sql.NullInt64
		if err := rows.Scan(
			&account.tag,
			&account.key,
			&account.createdSlot,
			&drepType,
			&account.drep,
		); err != nil {
			rows.Close()
			return nil, err
		}
		if drepType.Int64 < 0 {
			rows.Close()
			return nil, fmt.Errorf(
				"account drep_type %d is negative",
				drepType.Int64,
			)
		}
		account.drepType = uint64(drepType.Int64)
		changed = append(changed, account)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, account := range changed {
		state, err := deriveAccountStateAtSlot(
			ctx,
			db,
			account.tag,
			account.key,
			account.createdSlot,
			slot,
		)
		if err != nil {
			return nil, err
		}
		if state.absent || !state.active {
			continue
		}
		drepType, drep := account.drepType, account.drep
		if state.setDrep {
			drepType, drep = state.drepType, state.drep
		}
		add(account.tag, account.key, drepType, drep)
	}
	for key := range ret {
		slices.SortFunc(ret[key], func(a, b models.StakeCredentialRef) int {
			if a.Tag != b.Tag {
				return int(a.Tag) - int(b.Tag)
			}
			return bytes.Compare(a.Key, b.Key)
		})
	}
	return ret, nil
}
