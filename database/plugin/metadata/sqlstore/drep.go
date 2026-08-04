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

//nolint:gosec,rowserrcheck,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/drepquery"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) CreateDrep(txn types.Txn, drep *models.Drep) error {
	if drep == nil {
		return errors.New("create drep: drep is nil")
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	params, err := drepParams(drep)
	if err != nil {
		return err
	}
	id, err := q.CreateDrep(
		context.Background(),
		sqlitequery.CreateDrepParams(params),
	)
	if err != nil {
		return fmt.Errorf("create drep: %w", err)
	}
	drep.ID = uint(id)
	drep.Active = params.Active.Bool
	return nil
}

func (s *Store) ImportDrep(
	drep *models.Drep,
	registration *models.RegistrationDrep,
	txn types.Txn,
) error {
	if drep == nil {
		return errors.New("import drep: drep is nil")
	}
	if registration == nil {
		return errors.New("import drep: registration is nil")
	}
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			q := s.operationalQueries(db)
			params, err := drepParams(drep)
			if err != nil {
				return err
			}
			id, err := q.ImportDrep(
				context.Background(),
				sqlitequery.ImportDrepParams(params),
			)
			if err != nil {
				return fmt.Errorf("import drep: %w", err)
			}
			drep.ID = uint(id)
			drep.Active = params.Active.Bool

			regParams, err := drepRegistrationParams(registration)
			if err != nil {
				return err
			}
			registrationID, err := q.ImportDrepRegistration(
				context.Background(),
				regParams,
			)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("import drep registration: %w", err)
			}
			registration.ID = uint(registrationID)
			return nil
		},
	)
}

func (s *Store) RestoreDrepStateAtSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			if _, err := db.ExecContext(context.Background(), `
DELETE FROM drep
WHERE added_slot > ?
  AND NOT EXISTS (
      SELECT 1 FROM registration_drep registration
      WHERE registration.credential_tag = drep.credential_tag
        AND registration.drep_credential = drep.credential
        AND registration.added_slot <= ?
  )`,
				slot,
				slot,
			); err != nil {
				return err
			}
			rows, err := db.QueryContext(context.Background(), `
SELECT credential_tag, credential, expiry_epoch, last_activity_epoch
FROM drep WHERE added_slot > ?`,
				slot,
			)
			if err != nil {
				return err
			}
			type restoreRow struct {
				tag          uint8
				credential   []byte
				expiry       uint64
				lastActivity uint64
			}
			items := []restoreRow{}
			for rows.Next() {
				var item restoreRow
				if err := rows.Scan(
					&item.tag,
					&item.credential,
					&item.expiry,
					&item.lastActivity,
				); err != nil {
					rows.Close()
					return err
				}
				items = append(items, item)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}
			for _, item := range items {
				registration, found, err := latestDrepEvent(
					db,
					"registration_drep",
					"drep_credential",
					item.tag,
					item.credential,
					slot,
					true,
				)
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf(
						"DRep %x has no registration at or before slot %d",
						item.credential,
						slot,
					)
				}
				deregistration, hasDeregistration, err := latestDrepEvent(
					db,
					"deregistration_drep",
					"drep_credential",
					item.tag,
					item.credential,
					slot,
					false,
				)
				if err != nil {
					return err
				}
				update, hasUpdate, err := latestDrepEvent(
					db,
					"update_drep",
					"credential",
					item.tag,
					item.credential,
					slot,
					true,
				)
				if err != nil {
					return err
				}
				active := !hasDeregistration ||
					compareCertificatePosition(
						registration.position,
						deregistration.position,
					) > 0
				latest := registration
				if !active {
					latest = deregistration
					latest.anchorURL = ""
					latest.anchorHash = nil
				} else if hasUpdate &&
					compareCertificatePosition(
						update.position,
						latest.position,
					) > 0 {
					latest = update
				}
				expiry := uint64(0)
				lastActivity := uint64(0)
				if registration.position.slot == 0 {
					expiry = item.expiry
					lastActivity = item.lastActivity
				}
				if _, err := db.ExecContext(context.Background(), `
UPDATE drep
SET active = ?, anchor_url = ?, anchor_hash = ?, added_slot = ?,
    last_activity_epoch = ?, expiry_epoch = ?
WHERE credential_tag = ? AND credential = ?`,
					active,
					latest.anchorURL,
					latest.anchorHash,
					latest.position.slot,
					lastActivity,
					expiry,
					item.tag,
					item.credential,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type drepRestoreEvent struct {
	position   accountCertificatePosition
	anchorURL  string
	anchorHash []byte
}

func latestDrepEvent(
	db queryer,
	table string,
	credentialColumn string,
	tag uint8,
	credential []byte,
	slot uint64,
	hasAnchor bool,
) (drepRestoreEvent, bool, error) {
	anchorColumns := "'' AS anchor_url, NULL AS anchor_hash"
	if hasAnchor {
		anchorColumns = "event.anchor_url, event.anchor_hash"
	}
	var event drepRestoreEvent
	err := db.QueryRowContext(context.Background(), `
SELECT event.added_slot, COALESCE(tx.block_index, 0),
       COALESCE(certs.cert_index, 0), `+anchorColumns+`
FROM `+table+` event
LEFT JOIN certs ON certs.id = event.certificate_id
LEFT JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE event.credential_tag = ? AND event.`+credentialColumn+` = ?
  AND event.added_slot <= ?
ORDER BY event.added_slot DESC, COALESCE(tx.block_index, 0) DESC,
         COALESCE(certs.cert_index, 0) DESC, event.id DESC
LIMIT 1`,
		tag,
		credential,
		slot,
	).Scan(
		&event.position.slot,
		&event.position.blockIndex,
		&event.position.certIndex,
		&event.anchorURL,
		&event.anchorHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return drepRestoreEvent{}, false, nil
	}
	return event, err == nil, err
}

func (s *Store) GetDrep(
	credential []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Drep, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	var row sqlitequery.Drep
	if includeInactive {
		row, err = q.GetDrepByHash(context.Background(), credential)
	} else {
		row, err = q.GetActiveDrepByHash(context.Background(), credential)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return drepFromSQLite(row), nil
}

func (s *Store) GetDrepByCredential(
	credentialTag uint8,
	credential []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Drep, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	params := sqlitequery.GetActiveDrepByCredentialParams{
		CredentialTag: int64(credentialTag),
		Credential:    credential,
	}
	var row sqlitequery.Drep
	if includeInactive {
		row, err = q.GetDrepByCredential(
			context.Background(),
			sqlitequery.GetDrepByCredentialParams(params),
		)
	} else {
		row, err = q.GetActiveDrepByCredential(
			context.Background(),
			params,
		)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return drepFromSQLite(row), nil
}

func (s *Store) GetActiveDreps(
	txn types.Txn,
) ([]*models.Drep, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	rows, err := q.GetActiveDreps(context.Background())
	if err != nil {
		return nil, err
	}
	ret := make([]*models.Drep, len(rows))
	for i := range rows {
		ret[i] = drepFromSQLite(rows[i])
	}
	return ret, nil
}

// SetDrep updates all mutable registration state. It remains a concrete store
// helper because existing callers use it even though it is not part of the
// public MetadataStore interface.
func (s *Store) SetDrep(
	credentialTag uint8,
	credential []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return q.SetDrep(context.Background(), sqlitequery.SetDrepParams{
		CredentialTag: int64(credentialTag),
		Credential:    credential,
		AddedSlot:     validInt64(slotValue),
		AnchorUrl:     validString(url),
		AnchorHash:    hash,
		Active:        validBool(active),
	})
}

func (s *Store) InsertDrepIfAbsent(
	credentialTag uint8,
	credential []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return q.InsertDrepIfAbsent(
		context.Background(),
		sqlitequery.InsertDrepIfAbsentParams{
			CredentialTag: int64(credentialTag),
			Credential:    credential,
			AddedSlot:     validInt64(slotValue),
			AnchorUrl:     validString(url),
			AnchorHash:    hash,
			Active:        validBool(active),
		},
	)
}

func (s *Store) GetDRepDelegators(
	credentialTag uint8,
	credential []byte,
	txn types.Txn,
) ([]models.StakeCredentialRef, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	rows, err := q.GetDRepDelegators(
		context.Background(),
		sqlitequery.GetDRepDelegatorsParams{
			Drep:     credential,
			DrepType: validInt64(int64(credentialTag)),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("get drep delegators: %w", err)
	}
	ret := make([]models.StakeCredentialRef, len(rows))
	for i := range rows {
		ret[i] = models.NewStakeCredentialRef(
			uint8(rows[i].CredentialTag),
			rows[i].StakingKey,
		)
	}
	return ret, nil
}

func (s *Store) GetDRepVotingPower(
	credentialTag uint8,
	credential []byte,
	expiryEpoch uint64,
	txn types.Txn,
) (uint64, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	query, args := drepquery.VotingPowerSQL(
		s.dialect.Name(),
		credentialTag,
		credential,
		expiryEpoch,
	)
	var stake int64
	if err := db.QueryRowContext(
		context.Background(),
		s.dialect.Rebind(query),
		args...,
	).Scan(&stake); err != nil {
		return 0, fmt.Errorf("get drep voting power: %w", err)
	}
	return uint64(stake), nil
}

func (s *Store) GetDRepVotingPowerBatch(
	credentials []models.StakeCredentialRef,
	expiryEpoch uint64,
	txn types.Txn,
) (map[string]uint64, error) {
	ret := make(map[string]uint64, len(credentials))
	if len(credentials) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	hashes := make([][]byte, len(credentials))
	requested := make(map[string]struct{}, len(credentials))
	for i := range credentials {
		hashes[i] = credentials[i].Key
		requested[credentials[i].MapKey()] = struct{}{}
	}
	// Each hash occurs in both the inner and outer IN list.
	chunkSize := s.dialect.ParameterLimit() / 2
	if expiryEpoch > 0 {
		chunkSize = (s.dialect.ParameterLimit() - 2) / 2
	}
	for start := 0; start < len(hashes); start += chunkSize {
		end := min(start+chunkSize, len(hashes))
		chunk := hashes[start:end]
		query := expandDrepCollectionQuery(
			drepquery.VotingPowerBatchSQL(s.dialect.Name(), expiryEpoch),
			len(chunk),
		)
		args := drepCollectionArgs(chunk, expiryEpoch)
		rows, err := db.QueryContext(
			context.Background(),
			s.dialect.Rebind(query),
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("get drep voting power batch: %w", err)
		}
		for rows.Next() {
			var credential []byte
			var credentialTag int64
			var stake int64
			if err := rows.Scan(&credential, &credentialTag, &stake); err != nil {
				rows.Close()
				return nil, err
			}
			if credentialTag > 1 {
				continue
			}
			ref := models.NewStakeCredentialRef(
				uint8(credentialTag),
				credential,
			)
			if _, ok := requested[ref.MapKey()]; ok {
				ret[ref.MapKey()] = uint64(stake)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (s *Store) GetDRepVotingPowerByType(
	drepTypes []uint64,
	expiryEpoch uint64,
	txn types.Txn,
) (map[uint64]uint64, error) {
	ret := make(map[uint64]uint64, len(drepTypes))
	if len(drepTypes) == 0 {
		return ret, nil
	}
	if err := models.ValidatePredefinedDrepTypes(drepTypes); err != nil {
		return nil, err
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query := expandDrepCollectionQuery(
		drepquery.VotingPowerByTypeSQL(s.dialect.Name(), expiryEpoch),
		len(drepTypes),
	)
	args := drepCollectionArgs(drepTypes, expiryEpoch)
	rows, err := db.QueryContext(
		context.Background(),
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("get drep voting power by type: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var drepType int64
		var stake int64
		if err := rows.Scan(&drepType, &stake); err != nil {
			return nil, err
		}
		ret[uint64(drepType)] = uint64(stake)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) UpdateDRepActivity(
	credentialTag uint8,
	credential []byte,
	activityEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	activity, err := checkedInt64(activityEpoch)
	if err != nil {
		return err
	}
	expiry, err := checkedInt64(activityEpoch + inactivityPeriod)
	if err != nil {
		return err
	}
	affected, err := q.UpdateDRepActivity(
		context.Background(),
		sqlitequery.UpdateDRepActivityParams{
			LastActivityEpoch: validInt64(activity),
			ExpiryEpoch:       validInt64(expiry),
			CredentialTag:     int64(credentialTag),
			Credential:        credential,
		},
	)
	if err != nil {
		return fmt.Errorf("update drep activity: %w", err)
	}
	if affected == 0 {
		return models.ErrDrepActivityNotUpdated
	}
	return nil
}

func (s *Store) GetExpiredDReps(
	epoch uint64,
	txn types.Txn,
) ([]*models.Drep, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	epochValue, err := checkedInt64(epoch)
	if err != nil {
		return nil, err
	}
	rows, err := q.GetExpiredDReps(
		context.Background(),
		validInt64(epochValue),
	)
	if err != nil {
		return nil, fmt.Errorf("get expired dreps: %w", err)
	}
	ret := make([]*models.Drep, len(rows))
	for i := range rows {
		ret[i] = drepFromSQLite(rows[i])
	}
	return ret, nil
}

func (s *Store) GetDrepLastRegistrationSlot(
	credentialTag uint8,
	credential []byte,
	txn types.Txn,
) (uint64, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q := s.operationalQueries(db)
	slot, err := q.GetDrepLastRegistrationSlot(
		context.Background(),
		sqlitequery.GetDrepLastRegistrationSlotParams{
			CredentialTag:  int64(credentialTag),
			DrepCredential: credential,
		},
	)
	if err != nil {
		return 0, fmt.Errorf("get drep last registration slot: %w", err)
	}
	return uint64(slot), nil
}

func (s *Store) GetDreps(
	txn types.Txn,
) ([]models.DrepListRow, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	const query = `
WITH first_seen AS (
    SELECT cred, tag, MIN(slot) AS slot FROM (
        SELECT drep_credential AS cred, credential_tag AS tag,
               MIN(added_slot) AS slot
        FROM registration_drep GROUP BY drep_credential, credential_tag
        UNION ALL SELECT credential, credential_tag, MIN(added_slot)
        FROM update_drep GROUP BY credential, credential_tag
        UNION ALL SELECT drep, drep_type, MIN(added_slot)
        FROM vote_delegation WHERE drep_type <= 1 GROUP BY drep, drep_type
        UNION ALL SELECT drep, drep_type, MIN(added_slot)
        FROM stake_vote_delegation
        WHERE drep_type <= 1 GROUP BY drep, drep_type
        UNION ALL SELECT drep, drep_type, MIN(added_slot)
        FROM vote_registration_delegation
        WHERE drep_type <= 1 GROUP BY drep, drep_type
        UNION ALL SELECT drep, drep_type, MIN(added_slot)
        FROM stake_vote_registration_delegation
        WHERE drep_type <= 1 GROUP BY drep, drep_type
    ) u GROUP BY cred, tag
),
last_reg AS (
    SELECT drep_credential AS cred, credential_tag AS tag,
           MAX(added_slot) AS slot
    FROM registration_drep
    WHERE certificate_id IS NOT NULL AND certificate_id != 0
    GROUP BY drep_credential, credential_tag
)
SELECT drep.anchor_url, drep.credential, drep.anchor_hash, drep.id,
       drep.added_slot, drep.credential_tag, drep.last_activity_epoch,
       drep.expiry_epoch, drep.active,
       COALESCE(first_seen.slot, drep.added_slot) AS first_seen_slot,
       COALESCE(last_reg.slot, 0) AS last_registration_slot
FROM drep
LEFT JOIN first_seen
  ON first_seen.cred = drep.credential
 AND first_seen.tag = drep.credential_tag
LEFT JOIN last_reg
  ON last_reg.cred = drep.credential
 AND last_reg.tag = drep.credential_tag
ORDER BY COALESCE(first_seen.slot, drep.added_slot), drep.id`
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("get dreps: %w", err)
	}
	defer rows.Close()
	ret := []models.DrepListRow{}
	for rows.Next() {
		var row sqlitequery.Drep
		var firstSeen int64
		var lastRegistration int64
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
			&firstSeen,
			&lastRegistration,
		); err != nil {
			return nil, err
		}
		drep := drepFromSQLite(row)
		ret = append(ret, models.DrepListRow{
			AnchorURL:            drep.AnchorURL,
			Credential:           drep.Credential,
			AnchorHash:           drep.AnchorHash,
			ID:                   drep.ID,
			AddedSlot:            drep.AddedSlot,
			CredentialTag:        drep.CredentialTag,
			LastActivityEpoch:    drep.LastActivityEpoch,
			ExpiryEpoch:          drep.ExpiryEpoch,
			Active:               drep.Active,
			FirstSeenSlot:        uint64(firstSeen),
			LastRegistrationSlot: uint64(lastRegistration),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) GetPredefinedDrepFirstSeenSlots(
	txn types.Txn,
) (map[uint64]uint64, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	const query = `
SELECT drep_type, MIN(slot) AS slot FROM (
    SELECT drep_type, MIN(added_slot) AS slot
    FROM vote_delegation WHERE drep_type >= 2 GROUP BY drep_type
    UNION ALL SELECT drep_type, MIN(added_slot)
    FROM stake_vote_delegation WHERE drep_type >= 2 GROUP BY drep_type
    UNION ALL SELECT drep_type, MIN(added_slot)
    FROM vote_registration_delegation WHERE drep_type >= 2 GROUP BY drep_type
    UNION ALL SELECT drep_type, MIN(added_slot)
    FROM stake_vote_registration_delegation
    WHERE drep_type >= 2 GROUP BY drep_type
) u GROUP BY drep_type`
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("get predefined drep first seen slots: %w", err)
	}
	defer rows.Close()
	ret := make(map[uint64]uint64)
	for rows.Next() {
		var drepType int64
		var slot int64
		if err := rows.Scan(&drepType, &slot); err != nil {
			return nil, err
		}
		ret[uint64(drepType)] = uint64(slot)
	}
	return ret, rows.Err()
}

func (s *Store) DeactivateDreps(
	txn types.Txn,
	credentials []models.StakeCredentialRef,
) error {
	if len(credentials) == 0 {
		return nil
	}
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("DeactivateDreps: resolve db: %w", err)
	}
	chunkSize := s.dialect.ParameterLimit() / 2
	for start := 0; start < len(credentials); start += chunkSize {
		end := min(start+chunkSize, len(credentials))
		predicates := make([]string, 0, end-start)
		args := make([]any, 0, (end-start)*2)
		for _, credential := range credentials[start:end] {
			predicates = append(
				predicates,
				"(credential_tag = ? AND credential = ?)",
			)
			args = append(args, credential.Tag, credential.Key)
		}
		query := "UPDATE drep SET active = FALSE WHERE " +
			strings.Join(predicates, " OR ")
		if _, err := db.ExecContext(
			context.Background(),
			s.dialect.Rebind(query),
			args...,
		); err != nil {
			return fmt.Errorf("deactivate dreps: %w", err)
		}
	}
	return nil
}

func (s *Store) ClearDanglingDRepDelegations(
	atSlot uint64,
	txn types.Txn,
) (int, error) {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return 0, err
	}
	slot, err := checkedInt64(atSlot)
	if err != nil {
		return 0, err
	}
	result, err := db.ExecContext(context.Background(), `
UPDATE account
SET drep = NULL, drep_type = 0, added_slot = ?
WHERE drep IS NOT NULL
  AND drep_type IN (0, 1)
  AND NOT EXISTS (
      SELECT 1 FROM drep
      WHERE drep.credential_tag = account.drep_type
        AND drep.credential = account.drep
        AND drep.active = TRUE
  )`, slot)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	return int(affected), err
}

func expandDrepCollectionQuery(query string, count int) string {
	in := "IN (" + bindPlaceholders(count) + ")"
	return strings.Replace(query, "IN ?", in, 2)
}

func drepCollectionArgs[T any](values []T, expiryEpoch uint64) []any {
	ret := make([]any, 0, len(values)*2+2)
	if expiryEpoch > 0 {
		ret = append(ret, expiryEpoch)
	}
	for i := range values {
		ret = append(ret, values[i])
	}
	if expiryEpoch > 0 {
		ret = append(ret, expiryEpoch)
	}
	for i := range values {
		ret = append(ret, values[i])
	}
	return ret
}

type drepQueryParams struct {
	AnchorUrl         sql.NullString
	Credential        []byte
	AnchorHash        []byte
	AddedSlot         sql.NullInt64
	CredentialTag     int64
	LastActivityEpoch sql.NullInt64
	ExpiryEpoch       sql.NullInt64
	Active            sql.NullBool
}

func drepParams(drep *models.Drep) (drepQueryParams, error) {
	addedSlot, err := checkedInt64(drep.AddedSlot)
	if err != nil {
		return drepQueryParams{}, err
	}
	lastActivity, err := checkedInt64(drep.LastActivityEpoch)
	if err != nil {
		return drepQueryParams{}, err
	}
	expiry, err := checkedInt64(drep.ExpiryEpoch)
	if err != nil {
		return drepQueryParams{}, err
	}
	return drepQueryParams{
		AnchorUrl:         validString(drep.AnchorURL),
		Credential:        drep.Credential,
		AnchorHash:        drep.AnchorHash,
		AddedSlot:         validInt64(addedSlot),
		CredentialTag:     int64(drep.CredentialTag),
		LastActivityEpoch: validInt64(lastActivity),
		ExpiryEpoch:       validInt64(expiry),
		Active:            validBool(drep.Active),
	}, nil
}

func drepRegistrationParams(
	registration *models.RegistrationDrep,
) (sqlitequery.ImportDrepRegistrationParams, error) {
	certificateID, err := checkedInt64(uint64(registration.CertificateID))
	if err != nil {
		return sqlitequery.ImportDrepRegistrationParams{}, err
	}
	addedSlot, err := checkedInt64(registration.AddedSlot)
	if err != nil {
		return sqlitequery.ImportDrepRegistrationParams{}, err
	}
	return sqlitequery.ImportDrepRegistrationParams{
		AnchorUrl:      validString(registration.AnchorURL),
		DrepCredential: registration.DrepCredential,
		AnchorHash:     registration.AnchorHash,
		CertificateID:  validInt64(certificateID),
		CredentialTag:  int64(registration.CredentialTag),
		AddedSlot:      validInt64(addedSlot),
		DepositAmount:  validString(decimalUint64(registration.DepositAmount)),
	}, nil
}

func drepFromSQLite(row sqlitequery.Drep) *models.Drep {
	return &models.Drep{
		AnchorURL:         row.AnchorUrl.String,
		Credential:        row.Credential,
		AnchorHash:        row.AnchorHash,
		ID:                uint(row.ID),
		AddedSlot:         uint64(row.AddedSlot.Int64),
		CredentialTag:     uint8(row.CredentialTag),
		LastActivityEpoch: uint64(row.LastActivityEpoch.Int64),
		ExpiryEpoch:       uint64(row.ExpiryEpoch.Int64),
		Active:            row.Active.Bool,
	}
}

func validInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: true}
}

func validString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: true}
}

func validBool(value bool) sql.NullBool {
	return sql.NullBool{Bool: value, Valid: true}
}
