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
	"math"
	"net"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// poolRegistrationID returns the existing registration for the unique
// (pool, slot) key after an idempotent INSERT ... DO NOTHING. Keeping this
// lookup in one place ensures import and certificate paths preserve the
// first-write-wins behavior consistently across dialects.
func poolRegistrationID(
	ctx context.Context,
	db queryer,
	poolID int64,
	slot uint64,
) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
SELECT id FROM pool_registration WHERE pool_id = ? AND added_slot = ?`,
		poolID,
		slot,
	).Scan(&id)
	return id, err
}

const poolRegistrationInsertSQL = `
INSERT INTO pool_registration (
    margin, metadata_url, vrf_key_hash, pool_key_hash, reward_account,
    reward_account_credential_tag, metadata_hash, pledge, cost,
    certificate_id, pool_id, added_slot, deposit_amount,
    leios_key_public, leios_key_possession_proof
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (pool_id, added_slot) DO NOTHING
RETURNING id`

// insertPoolRegistration is shared by genesis/import and certificate paths so
// conflict semantics cannot diverge. Registrations are first-write-wins for
// the unique (pool, slot) key; a duplicate returns the existing row ID.
func insertPoolRegistration(
	ctx context.Context,
	db queryer,
	values []any,
	poolID int64,
	slot uint64,
) (int64, error) {
	id, err := queryReturnedID(ctx, db, poolRegistrationInsertSQL, values...)
	if errors.Is(err, sql.ErrNoRows) {
		return poolRegistrationID(ctx, db, poolID, slot)
	}
	return id, err
}

func (s *Store) ImportPool(
	pool *models.Pool,
	registration *models.PoolRegistration,
	txn types.Txn,
) error {
	if pool == nil {
		return errors.New("import pool: pool is nil")
	}
	if registration == nil {
		return errors.New("import pool: registration is nil")
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			margin := nullableRat(pool.Margin)
			id, err := queryReturnedID(ctx, db, `
INSERT INTO pool (
    margin, pool_key_hash, vrf_key_hash, reward_account,
    latest_op_cert_sequence, reward_account_credential_tag, pledge, cost,
    leios_key_public, leios_key_possession_proof
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (pool_key_hash) DO UPDATE SET
    vrf_key_hash = excluded.vrf_key_hash,
    pledge = excluded.pledge,
    cost = excluded.cost,
    margin = excluded.margin,
    reward_account = excluded.reward_account,
    reward_account_credential_tag =
        excluded.reward_account_credential_tag,
    leios_key_public = excluded.leios_key_public,
    leios_key_possession_proof = excluded.leios_key_possession_proof
RETURNING id`,
				margin,
				pool.PoolKeyHash,
				pool.VrfKeyHash,
				pool.RewardAccount,
				pool.LatestOpCertSequence,
				pool.RewardAccountCredentialTag,
				decimalUint64(pool.Pledge),
				decimalUint64(pool.Cost),
				nullBytes(pool.LeiosKeyPublic),
				nullBytes(pool.LeiosKeyPossessionProof),
			)
			if err != nil {
				return fmt.Errorf("import pool: %w", err)
			}
			pool.ID = uint(id)
			registration.PoolID = pool.ID
			registrationID, err := insertPoolRegistration(ctx, db, []any{
				nullableRat(registration.Margin),
				registration.MetadataUrl,
				registration.VrfKeyHash,
				registration.PoolKeyHash,
				registration.RewardAccount,
				registration.RewardAccountCredentialTag,
				registration.MetadataHash,
				decimalUint64(registration.Pledge),
				decimalUint64(registration.Cost),
				registration.CertificateID,
				registration.PoolID,
				registration.AddedSlot,
				decimalUint64(registration.DepositAmount),
				nullBytes(registration.LeiosKeyPublic),
				nullBytes(registration.LeiosKeyPossessionProof),
			}, int64(registration.PoolID), registration.AddedSlot)
			if err != nil {
				return fmt.Errorf("import pool registration: %w", err)
			}
			registration.ID = uint(registrationID)
			if _, err := db.ExecContext(ctx, `
DELETE FROM pool_registration_owner WHERE pool_registration_id = ?`,
				registrationID,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM pool_registration_relay WHERE pool_registration_id = ?`,
				registrationID,
			); err != nil {
				return err
			}
			for i := range registration.Owners {
				owner := &registration.Owners[i]
				owner.PoolID = pool.ID
				owner.PoolRegistrationID = registration.ID
				ownerID, err := queryReturnedID(ctx, db, `
INSERT INTO pool_registration_owner (
    key_hash, pool_registration_id, pool_id
) VALUES (?, ?, ?) RETURNING id`,
					owner.KeyHash,
					owner.PoolRegistrationID,
					owner.PoolID,
				)
				if err != nil {
					return err
				}
				owner.ID = uint(ownerID)
			}
			for i := range registration.Relays {
				relay := &registration.Relays[i]
				relay.PoolID = pool.ID
				relay.PoolRegistrationID = registration.ID
				relayID, err := queryReturnedID(ctx, db, `
INSERT INTO pool_registration_relay (
    ipv4, ipv6, hostname, pool_registration_id, pool_id, port
) VALUES (?, ?, ?, ?, ?, ?) RETURNING id`,
					netIPValue(relay.Ipv4),
					netIPValue(relay.Ipv6),
					relay.Hostname,
					relay.PoolRegistrationID,
					relay.PoolID,
					relay.Port,
				)
				if err != nil {
					return err
				}
				relay.ID = uint(relayID)
			}
			return nil
		},
	)
}

func (s *Store) GetPool(
	poolKeyHash lcommon.PoolKeyHash,
	includeInactive bool,
	txn types.Txn,
) (*models.Pool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	pool, err := queryPool(ctx, db, "pool_key_hash = ?", poolKeyHash.Bytes())
	if err != nil || pool == nil {
		return pool, err
	}
	if err := s.loadPoolAssociations(ctx, db, pool, true); err != nil {
		return nil, err
	}
	if !includeInactive {
		return s.activePoolOrNil(ctx, db, pool)
	}
	return pool, nil
}

func (s *Store) GetPoolByVrfKeyHash(
	vrfKeyHash []byte,
	txn types.Txn,
) (*models.Pool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	pool, err := queryPool(ctx, db, "vrf_key_hash = ?", vrfKeyHash)
	if err != nil || pool == nil {
		return pool, err
	}
	if err := s.loadPoolAssociations(ctx, db, pool, true); err != nil {
		return nil, err
	}
	// This method backs LedgerView.IsVrfKeyInUse, whose contract is to report
	// only currently registered pools. Retired registrations remain in the
	// history but must not reserve their old VRF key indefinitely.
	return s.activePoolOrNil(ctx, db, pool)
}

// activePoolOrNil applies the same current-registration/retirement semantics
// used by GetPool(..., false) to other lookups that expose active pools.
func (s *Store) activePoolOrNil(
	ctx context.Context,
	db queryer,
	pool *models.Pool,
) (*models.Pool, error) {
	if len(pool.Registration) == 0 {
		return nil, nil
	}
	if len(pool.Retirement) == 0 {
		return pool, nil
	}
	current, ok, err := currentEpoch(ctx, db)
	if err != nil {
		return nil, err
	}
	if ok && pool.Retirement[0].Epoch <= current {
		retired, err := latestPoolEventIsRetirement(ctx, db, s.dialect, pool.ID)
		if err != nil {
			return nil, err
		}
		if retired {
			return nil, nil
		}
	}
	return pool, nil
}

func (s *Store) GetPools(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn types.Txn,
) ([]models.Pool, error) {
	ret := []models.Pool{}
	if len(poolKeyHashes) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	// Deduplicated before chunking, which is what keeps chunking invisible to
	// the caller. A single `IN (...)` has set semantics -- naming a value twice
	// still matches its row once -- but two mentions landing in different
	// chunks match in both, and the chunks are concatenated. The caller would
	// then get a pool twice from a request that, unchunked, returned it once.
	//
	// The repeat does more damage than it looks. loadPoolsAssociations keys its
	// index by pool ID and so keeps only the last position for a repeated pool,
	// leaving the earlier copy with empty registrations and retirements. Callers
	// deciding on len(pool.Registration), as registeredPoolVrfKeyHash does,
	// would then get an answer that depends on which copy they reached.
	hashes := make([]any, 0, len(poolKeyHashes))
	seen := make(map[string]struct{}, len(poolKeyHashes))
	for i := range poolKeyHashes {
		raw := poolKeyHashes[i].Bytes()
		if _, ok := seen[string(raw)]; ok {
			continue
		}
		seen[string(raw)] = struct{}{}
		hashes = append(hashes, raw)
	}
	// Chunked over the same bound loadPoolsAssociations below uses. Callers
	// name every pool on the chain -- the pool distribution behind
	// leadership-schedule and the peer snapshot both do -- so the list length
	// is the chain's pool count rather than anything this code picks.
	//
	// ParameterLimit is the conservative figure the store contracts to, not
	// each driver's true ceiling: SQLite has allowed 32766 bound parameters
	// since 3.32 and PostgreSQL/MySQL allow 65535, so at Cardano's few
	// thousand registered pools an unchunked list happens to fit today. It
	// stays chunked because that headroom is a property of the deployment
	// rather than of this query, and exceeding it fails the read outright
	// instead of degrading.
	for start := 0; start < len(hashes); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(hashes))
		rows, err := db.QueryContext(ctx, `
SELECT margin, pool_key_hash, vrf_key_hash, reward_account,
       latest_op_cert_sequence, reward_account_credential_tag, id,
       pledge, cost, leios_key_public, leios_key_possession_proof
FROM pool
WHERE pool_key_hash IN (`+bindPlaceholders(end-start)+`)`,
			hashes[start:end]...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			pool, err := scanPool(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			ret = append(ret, *pool)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	if err := s.loadPoolsAssociations(ctx, db, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) UpdatePoolOpCertSequence(
	poolKeyHash lcommon.PoolKeyHash,
	sequence uint64,
	slot uint64,
	txn types.Txn,
) error {
	sequenceValue, err := checkedInt64(sequence)
	if err != nil {
		return err
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(ctx, `
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, ?)
ON CONFLICT (pool_key_hash, slot) DO UPDATE
SET sequence = excluded.sequence`,
				poolKeyHash.Bytes(),
				slotValue,
				sequenceValue,
			); err != nil {
				return err
			}
			_, err := db.ExecContext(ctx, `
UPDATE pool SET latest_op_cert_sequence = ?
WHERE pool_key_hash = ?
  AND latest_op_cert_sequence < ?`,
				sequenceValue,
				poolKeyHash.Bytes(),
				sequenceValue,
			)
			return err
		},
	)
}

func (s *Store) LatestPoolOpCertSequence(
	poolKeyHash lcommon.PoolKeyHash,
	txn types.Txn,
) (uint64, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, false, err
	}
	var sequence int64
	var count int64
	err = db.QueryRowContext(ctx, `
SELECT COALESCE(MAX(sequence), 0), COUNT(*)
FROM pool_opcert_sequence
WHERE pool_key_hash = ?`,
		poolKeyHash.Bytes(),
	).Scan(&sequence, &count)
	return uint64(sequence), count > 0, err
}

// LatestPoolOpCertSequenceAfter returns the highest sequence recorded for a
// pool after afterSlot. A Mithril-restored ledger uses this to distinguish
// replayed counter history from rows imported at its trust boundary.
func (s *Store) LatestPoolOpCertSequenceAfter(
	poolKeyHash lcommon.PoolKeyHash,
	afterSlot uint64,
	txn types.Txn,
) (uint64, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, false, err
	}
	var sequence int64
	var count int64
	err = db.QueryRowContext(ctx, `
SELECT COALESCE(MAX(sequence), 0), COUNT(*)
FROM pool_opcert_sequence
WHERE pool_key_hash = ? AND slot > ?`,
		poolKeyHash.Bytes(),
		afterSlot,
	).Scan(&sequence, &count)
	return uint64(sequence), count > 0, err
}

func (s *Store) LatestPoolOpCertSequenceAtOrBefore(
	poolKeyHash lcommon.PoolKeyHash,
	slot uint64,
	txn types.Txn,
) (uint64, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, false, err
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return 0, false, err
	}
	var sequence int64
	var count int64
	err = db.QueryRowContext(ctx, `
SELECT COALESCE(MAX(sequence), 0), COUNT(*)
FROM pool_opcert_sequence
WHERE pool_key_hash = ? AND slot <= ?`,
		poolKeyHash.Bytes(),
		slotValue,
	).Scan(&sequence, &count)
	return uint64(sequence), count > 0, err
}

// LatestPoolOpCertSequencesSQL is the statement LatestPoolOpCertSequences
// issues.
//
// Exported so a test can pin its query plan against the statement the store
// actually runs. The index this reads is only worth its write cost while the
// planner chooses it, and a test EXPLAINing a hand-copied statement would keep
// passing against the copy after the store's own SQL moved off the index.
const LatestPoolOpCertSequencesSQL = `
SELECT pool_key_hash, MAX(sequence)
FROM pool_opcert_sequence
GROUP BY pool_key_hash`

// LatestPoolOpCertSequences returns the highest observed op-cert sequence for
// every pool that has issued a block, keyed by pool key hash.
//
// The issuer table records one row per (pool, slot), so the highest sequence
// is an aggregate rather than the newest row: a pool that rotated to a lower
// issue number after a higher one has still had the higher number accepted,
// and that is the number the chain enforces.
func (s *Store) LatestPoolOpCertSequences(
	txn types.Txn,
) (map[string]uint64, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(
		ctx,
		LatestPoolOpCertSequencesSQL,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := map[string]uint64{}
	for rows.Next() {
		var poolKeyHash []byte
		var sequence int64
		if err := rows.Scan(&poolKeyHash, &sequence); err != nil {
			return nil, err
		}
		ret[string(poolKeyHash)] = uint64(sequence)
	}
	return ret, rows.Err()
}

// mithrilTrustBoundarySyncKey mirrors database.mithrilLedgerSlotSyncKey and
// ledgerstate's writer of the same key. It is duplicated here for the reason
// the database package duplicates it: nothing below the ledger may import the
// package that writes it.
const mithrilTrustBoundarySyncKey = "mithril_ledger_slot"

// firstMintedBlockSlot raises startSlot past the Mithril trust boundary when
// one is recorded, so a slot range asked about minted blocks never reaches
// rows that are not blocks.
//
// pool_opcert_sequence carries two kinds of row. A block-apply writes one per
// block minted (ledger.processBlockTransactions), which is what every block
// count here means. A Mithril restore also writes one row per pool in the
// certified HeaderState counter map, all at the snapshot's anchor slot
// (ledgerstate.importOpCertCounters), so post-boundary validation has an
// authoritative baseline to enforce counter monotonicity against. Those rows
// record a counter the node was told about, not a block it saw: a bootstrap
// applies no blocks at or below the anchor, and one slot cannot hold a block
// from every pool in the set in any case.
//
// Counting them as blocks credits every pool holding a certified counter with
// a block it never minted and inflates the epoch's block total by the size of
// the pool set, which reaches pool reward performance
// (ledger/reward_calculation.go), the reward_pool_input rows seeded at the
// epoch boundary (ledger/snapshot/rotation.go), and Blockfrost's
// blocks_minted. The boundary is the same discriminator
// LedgerState.latestOpCertCounterForValidation already uses to tell an
// observed counter from an imported one.
//
// A failed or malformed read is returned as an error rather than treated as
// "no boundary": these callers distribute rewards from the result, and a
// silent fallback would restore the very inflation this removes at exactly
// the moment the boundary could not be confirmed.
//
// The bool reports whether any slot at all can be past the boundary. It is
// false only when the recorded boundary is the largest representable slot,
// where boundary+1 would wrap to zero and re-admit every row.
func (s *Store) firstMintedBlockSlot(
	db queryer,
	ctx context.Context,
	startSlot uint64,
) (uint64, bool, error) {
	value, err := s.operationalQueries(db).
		GetSyncState(ctx, mithrilTrustBoundarySyncKey)
	if errors.Is(err, sql.ErrNoRows) {
		return startSlot, true, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read Mithril trust boundary: %w", err)
	}
	if value == "" {
		// sqlc's GetSyncState returns sql.ErrNoRows for an absent key, which
		// the branch above already takes, so an empty string here is a row
		// that exists and holds nothing. That is a malformed boundary, not
		// the absence of one, and it gets the same treatment as an
		// unparseable value rather than silently re-admitting every imported
		// counter row.
		return 0, false, fmt.Errorf(
			"parse Mithril trust boundary %q: empty value", value,
		)
	}
	boundary, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf(
			"parse Mithril trust boundary %q: %w",
			value,
			err,
		)
	}
	if boundary < startSlot {
		return startSlot, true, nil
	}
	if boundary == math.MaxUint64 {
		return 0, false, nil
	}
	return boundary + 1, true, nil
}

func (s *Store) GetPoolBlockIssuersInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) ([]models.PoolOpCertSequence, error) {
	if endSlot < startSlot {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	startSlot, anyMinted, err := s.firstMintedBlockSlot(db, ctx, startSlot)
	if err != nil {
		return nil, err
	}
	if !anyMinted || endSlot < startSlot {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx, `
SELECT pool_key_hash, id, slot, sequence
FROM pool_opcert_sequence
WHERE slot >= ? AND slot <= ?
ORDER BY slot ASC, pool_key_hash ASC`,
		startSlot,
		endSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []models.PoolOpCertSequence{}
	for rows.Next() {
		var row models.PoolOpCertSequence
		if err := rows.Scan(
			&row.PoolKeyHash,
			&row.ID,
			&row.Slot,
			&row.Sequence,
		); err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func (s *Store) CountPoolBlocksInSlotRange(
	poolKeyHashes []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) (map[string]uint64, uint64, error) {
	counts := make(map[string]uint64, len(poolKeyHashes))
	for _, poolKeyHash := range poolKeyHashes {
		counts[string(poolKeyHash.Bytes())] = 0
	}
	if endSlot < startSlot {
		return counts, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	startSlot, anyMinted, err := s.firstMintedBlockSlot(db, ctx, startSlot)
	if err != nil {
		return nil, 0, err
	}
	if !anyMinted || endSlot < startSlot {
		return counts, 0, nil
	}
	var total int64
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM pool_opcert_sequence
WHERE slot >= ? AND slot <= ?`,
		startSlot,
		endSlot,
	).Scan(&total); err != nil {
		return nil, 0, err
	}
	if len(poolKeyHashes) == 0 {
		return counts, uint64(total), nil
	}
	args := make([]any, 0, len(poolKeyHashes)+2)
	args = append(args, startSlot, endSlot)
	for _, poolKeyHash := range poolKeyHashes {
		args = append(args, poolKeyHash.Bytes())
	}
	rows, err := db.QueryContext(ctx, `
SELECT pool_key_hash, COUNT(*)
FROM pool_opcert_sequence
WHERE slot >= ? AND slot <= ?
  AND pool_key_hash IN (`+bindPlaceholders(len(poolKeyHashes))+`)
GROUP BY pool_key_hash`,
		args...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		var count int64
		if err := rows.Scan(&hash, &count); err != nil {
			return nil, 0, err
		}
		counts[string(hash)] = uint64(count)
	}
	return counts, uint64(total), rows.Err()
}

func (s *Store) RetirePools(
	txn types.Txn,
	poolKeyHashes [][]byte,
	epoch uint64,
	addedSlot uint64,
) error {
	if len(poolKeyHashes) == 0 {
		return nil
	}
	epochValue, err := checkedInt64(epoch)
	if err != nil {
		return err
	}
	slotValue, err := checkedInt64(addedSlot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			for start := 0; start < len(poolKeyHashes); start += 400 {
				end := min(start+400, len(poolKeyHashes))
				chunk := poolKeyHashes[start:end]
				args := make([]any, len(chunk))
				for i := range chunk {
					args[i] = chunk[i]
				}
				rows, err := db.QueryContext(ctx, `
SELECT id, pool_key_hash FROM pool
WHERE pool_key_hash IN (`+bindPlaceholders(len(chunk))+`)`,
					args...,
				)
				if err != nil {
					return err
				}
				for rows.Next() {
					var poolID int64
					var hash []byte
					if err := rows.Scan(&poolID, &hash); err != nil {
						rows.Close()
						return err
					}
					if _, err := db.ExecContext(ctx, `
INSERT INTO pool_retirement (
    pool_key_hash, certificate_id, pool_id, epoch, added_slot
)
SELECT ?, 0, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM pool_retirement
    WHERE certificate_id = 0 AND pool_id = ?
      AND epoch = ? AND added_slot = ?
)`,
						hash,
						poolID,
						epochValue,
						slotValue,
						poolID,
						epochValue,
						slotValue,
					); err != nil {
						rows.Close()
						return err
					}
				}
				if err := rows.Close(); err != nil {
					return err
				}
				if err := rows.Err(); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func (s *Store) GetRetiringPools(
	currentEpoch uint64,
	txn types.Txn,
) ([]models.PoolRetiringRow, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
WITH latest_reg AS (
    SELECT pr.pool_key_hash, pr.added_slot,
           CASE WHEN pr.certificate_id = 0 THEN 1 ELSE 0 END synth,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_key_hash
               ORDER BY pr.added_slot DESC,
                        CASE WHEN pr.certificate_id = 0 THEN 1 ELSE 0 END DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_registration pr
    LEFT JOIN certs c ON c.id = pr.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
),
latest_ret AS (
    SELECT pt.pool_key_hash, pt.epoch, pt.added_slot,
           CASE WHEN pt.certificate_id = 0 THEN 1 ELSE 0 END synth,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY pt.pool_key_hash
               ORDER BY pt.added_slot DESC,
                        CASE WHEN pt.certificate_id = 0 THEN 1 ELSE 0 END DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_retirement pt
    LEFT JOIN certs c ON c.id = pt.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
)
SELECT r.pool_key_hash, r.epoch
FROM latest_ret r
LEFT JOIN latest_reg g
  ON g.pool_key_hash = r.pool_key_hash AND g.rn = 1
WHERE r.rn = 1 AND r.epoch > ?
  AND (
      g.pool_key_hash IS NULL
      OR (r.added_slot, r.synth, r.block_index, r.cert_index)
         > (g.added_slot, g.synth, g.block_index, g.cert_index)
  )
ORDER BY r.epoch, r.added_slot, r.block_index, r.cert_index`,
		currentEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("get retiring pools: %w", err)
	}
	defer rows.Close()
	ret := []models.PoolRetiringRow{}
	for rows.Next() {
		var row models.PoolRetiringRow
		if err := rows.Scan(&row.PoolKeyHash, &row.Epoch); err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func (s *Store) GetActivePoolKeyHashes(
	txn types.Txn,
) ([][]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("GetActivePoolKeyHashes: resolve db: %w", err)
	}
	slot, err := currentTipSlot(ctx, db)
	if err != nil {
		return nil, err
	}
	if slot == 0 {
		var exists bool
		if err := db.QueryRowContext(
			ctx,
			"SELECT EXISTS(SELECT 1 FROM tip WHERE id = 1)",
		).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return [][]byte{}, nil
		}
	}
	return s.GetActivePoolKeyHashesAtSlot(slot, txn)
}

func (s *Store) GetActivePoolKeyHashesOrdered(
	txn types.Txn,
) ([][]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesOrdered: resolve db: %w",
			err,
		)
	}
	slot, err := currentTipSlot(ctx, db)
	if err != nil {
		return nil, err
	}
	var epochID, startSlot, length int64
	err = db.QueryRowContext(ctx, `
SELECT epoch_id, start_slot, length_in_slots
FROM epoch
WHERE start_slot <= ?
ORDER BY start_slot DESC
LIMIT 1`,
		slot,
	).Scan(&epochID, &startSlot, &length)
	if errors.Is(err, sql.ErrNoRows) ||
		(err == nil && slot >= uint64(startSlot+length)) {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesOrdered: %w",
			types.ErrNoEpochData,
		)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesOrdered: get epoch at slot: %w",
			err,
		)
	}
	rows, err := db.QueryContext(ctx, `
WITH reg_ranked AS (
    SELECT pr.pool_id, pr.added_slot,
           COALESCE(t.block_index, 0) AS blk_idx,
           COALESCE(c.cert_index, 0) AS cert_idx,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_id
               ORDER BY pr.added_slot DESC, COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) AS rn_latest,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_id
               ORDER BY pr.added_slot ASC, COALESCE(t.block_index, 0) ASC,
                        COALESCE(c.cert_index, 0) ASC
           ) AS rn_first
    FROM pool_registration pr
    LEFT JOIN certs c ON c.id = pr.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE pr.added_slot <= ?
),
latest_ret AS (
    SELECT rt.pool_id, rt.added_slot, rt.epoch,
           CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END synthetic_ret,
           COALESCE(t.block_index, 0) AS blk_idx,
           COALESCE(c.cert_index, 0) AS cert_idx,
           ROW_NUMBER() OVER (
               PARTITION BY rt.pool_id
               ORDER BY rt.added_slot DESC,
                        CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) AS rn
    FROM pool_retirement rt
    LEFT JOIN certs c ON c.id = rt.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE rt.added_slot <= ?
)
SELECT p.pool_key_hash
FROM pool p
JOIN reg_ranked lr ON lr.pool_id = p.id AND lr.rn_latest = 1
JOIN reg_ranked fr ON fr.pool_id = p.id AND fr.rn_first = 1
LEFT JOIN latest_ret lrt ON lrt.pool_id = p.id AND lrt.rn = 1
WHERE lrt.pool_id IS NULL
   OR lrt.added_slot < lr.added_slot
   OR (lrt.added_slot = lr.added_slot AND lrt.synthetic_ret = 0
       AND lrt.blk_idx < lr.blk_idx)
   OR (lrt.added_slot = lr.added_slot AND lrt.synthetic_ret = 0
       AND lrt.blk_idx = lr.blk_idx AND lrt.cert_idx < lr.cert_idx)
   OR lrt.epoch > ?
ORDER BY fr.added_slot ASC, fr.blk_idx ASC, fr.cert_idx ASC,
         p.pool_key_hash ASC`,
		slot,
		slot,
		epochID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesOrdered: query pools: %w",
			err,
		)
	}
	defer rows.Close()
	ret := make([][]byte, 0)
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		ret = append(ret, hash)
	}
	return ret, rows.Err()
}

func (s *Store) GetPoolCertificateHistory(
	pkh lcommon.PoolKeyHash,
	txn types.Txn,
) ([][]byte, [][]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	query := func(table string) ([][]byte, error) {
		rows, err := db.QueryContext(ctx, `
SELECT tx.hash
FROM `+table+` item
JOIN certs c ON c.id = item.certificate_id
JOIN "transaction" tx ON tx.id = c.transaction_id
WHERE item.pool_key_hash = ?
ORDER BY item.added_slot ASC, tx.block_index ASC, c.cert_index ASC`,
			pkh.Bytes(),
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		ret := make([][]byte, 0)
		for rows.Next() {
			var hash []byte
			if err := rows.Scan(&hash); err != nil {
				return nil, err
			}
			ret = append(ret, hash)
		}
		return ret, rows.Err()
	}
	registrations, err := query("pool_registration")
	if err != nil {
		return nil, nil, fmt.Errorf(
			"GetPoolCertificateHistory: query registrations: %w",
			err,
		)
	}
	retirements, err := query("pool_retirement")
	if err != nil {
		return nil, nil, fmt.Errorf(
			"GetPoolCertificateHistory: query retirements: %w",
			err,
		)
	}
	return registrations, retirements, nil
}

func (s *Store) GetActivePoolKeyHashesAtSlot(
	slot uint64,
	txn types.Txn,
) ([][]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: resolve db: %w",
			err,
		)
	}
	var epochID sql.NullInt64
	var startSlot sql.NullInt64
	var length sql.NullInt64
	err = db.QueryRowContext(ctx, `
SELECT epoch_id, start_slot, length_in_slots
FROM epoch
WHERE start_slot <= ?
ORDER BY start_slot DESC
LIMIT 1`,
		slot,
	).Scan(&epochID, &startSlot, &length)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: %w",
			types.ErrNoEpochData,
		)
	}
	if err != nil {
		return nil, err
	}
	if !epochID.Valid || !startSlot.Valid || !length.Valid ||
		slot >= uint64(startSlot.Int64+length.Int64) {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: %w",
			types.ErrNoEpochData,
		)
	}
	rows, err := db.QueryContext(ctx, `
WITH latest_reg AS (
    SELECT pr.pool_id, pr.added_slot,
           COALESCE(t.block_index, 0) blk_idx,
           COALESCE(c.cert_index, 0) cert_idx,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_id
               ORDER BY pr.added_slot DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_registration pr
    LEFT JOIN certs c ON c.id = pr.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE pr.added_slot <= ?
),
latest_ret AS (
    SELECT rt.pool_id, rt.added_slot, rt.epoch,
           CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END synthetic_ret,
           COALESCE(t.block_index, 0) blk_idx,
           COALESCE(c.cert_index, 0) cert_idx,
           ROW_NUMBER() OVER (
               PARTITION BY rt.pool_id
               ORDER BY rt.added_slot DESC,
                        CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_retirement rt
    LEFT JOIN certs c ON c.id = rt.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE rt.added_slot <= ?
)
SELECT p.pool_key_hash
FROM pool p
JOIN latest_reg lr ON lr.pool_id = p.id AND lr.rn = 1
LEFT JOIN latest_ret lrt ON lrt.pool_id = p.id AND lrt.rn = 1
WHERE lrt.pool_id IS NULL
   OR lrt.added_slot < lr.added_slot
   OR (lrt.added_slot = lr.added_slot AND lrt.synthetic_ret = 0
       AND lrt.blk_idx < lr.blk_idx)
   OR (lrt.added_slot = lr.added_slot AND lrt.synthetic_ret = 0
       AND lrt.blk_idx = lr.blk_idx AND lrt.cert_idx < lr.cert_idx)
   OR lrt.epoch > ?`,
		slot,
		slot,
		epochID.Int64,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := [][]byte{}
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		ret = append(ret, hash)
	}
	return ret, rows.Err()
}

func (s *Store) GetStakeByPool(
	poolKeyHash []byte,
	txn types.Txn,
) (uint64, uint64, error) {
	stakes, delegators, err := s.GetStakeByPools(
		[][]byte{poolKeyHash},
		txn,
	)
	if err != nil {
		return 0, 0, err
	}
	return stakes[string(poolKeyHash)], delegators[string(poolKeyHash)], nil
}

func (s *Store) GetStakeByPools(
	poolKeyHashes [][]byte,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	if len(poolKeyHashes) == 0 {
		stakes, delegators := emptyPoolStakeMaps(poolKeyHashes)
		return stakes, delegators, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	stakes, delegators := emptyPoolStakeMaps(poolKeyHashes)
	complete, err := s.getStakeByPoolsFromLive(
		ctx,
		db,
		poolKeyHashes,
		stakes,
		delegators,
	)
	if err == nil && complete {
		return stakes, delegators, nil
	}
	if err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "no such table") {
		return nil, nil, err
	}
	return s.getStakeByPoolsDirect(poolKeyHashes, txn)
}

func emptyPoolStakeMaps(
	poolKeyHashes [][]byte,
) (map[string]uint64, map[string]uint64) {
	stakes := make(map[string]uint64, len(poolKeyHashes))
	delegators := make(map[string]uint64, len(poolKeyHashes))
	for _, hash := range poolKeyHashes {
		stakes[string(hash)] = 0
		delegators[string(hash)] = 0
	}
	return stakes, delegators
}

func (s *Store) getStakeByPoolsFromLive(
	ctx context.Context,
	db queryer,
	poolKeyHashes [][]byte,
	stakes, delegators map[string]uint64,
) (bool, error) {
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		chunk := poolKeyHashes[start:end]
		args := make([]any, 0, len(chunk)+1)
		args = append(args, models.RewardStakeCalculationVersion)
		for _, hash := range chunk {
			args = append(args, hash)
		}
		rows, err := db.QueryContext(ctx, `
SELECT account.pool, reward_live_stake.utxo_stake
FROM account
LEFT JOIN reward_live_stake
  ON reward_live_stake.credential_tag = account.credential_tag
 AND reward_live_stake.staking_key = account.staking_key
 AND reward_live_stake.calculation_version = ?
WHERE account.active = TRUE AND account.pool IN (`+bindPlaceholders(len(chunk))+`)`, args...)
		if err != nil {
			return false, err
		}
		complete := true
		for rows.Next() {
			var hash []byte
			var raw sql.NullString
			if err := rows.Scan(&hash, &raw); err != nil {
				rows.Close()
				return false, err
			}
			key := string(hash)
			delegators[key]++
			if !raw.Valid || raw.String == "" {
				complete = false
				continue
			}
			amount, err := parseUint64("pool stake amount", raw.String)
			if err != nil {
				rows.Close()
				return false, err
			}
			if ^uint64(0)-stakes[key] < amount {
				rows.Close()
				return false, fmt.Errorf("pool stake overflow for %x", hash)
			}
			stakes[key] += amount
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return false, err
		}
		if err := rows.Close(); err != nil {
			return false, err
		}
		if !complete {
			return false, nil
		}
	}
	return true, nil
}

func (s *Store) getStakeByPoolsDirect(
	poolKeyHashes [][]byte,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	stakes := make(map[string]uint64, len(poolKeyHashes))
	delegators := make(map[string]uint64, len(poolKeyHashes))
	for _, hash := range poolKeyHashes {
		stakes[string(hash)] = 0
		delegators[string(hash)] = 0
	}
	if len(poolKeyHashes) == 0 {
		return stakes, delegators, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		chunk := poolKeyHashes[start:end]
		args := make([]any, len(chunk))
		for i := range chunk {
			args[i] = chunk[i]
		}
		rows, err := db.QueryContext(ctx, `
SELECT pool, COUNT(*)
FROM account
WHERE active = TRUE AND pool IN (`+bindPlaceholders(len(chunk))+`)
GROUP BY pool`,
			args...,
		)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			var hash []byte
			var count int64
			if err := rows.Scan(&hash, &count); err != nil {
				rows.Close()
				return nil, nil, err
			}
			delegators[string(hash)] = uint64(count)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, nil, err
		}
		rows, err = db.QueryContext(ctx, `
		SELECT account.pool, utxo.amount
		FROM account
JOIN utxo
  ON utxo.credential_tag = account.credential_tag
 AND utxo.staking_key = account.staking_key
WHERE account.active = TRUE AND utxo.deleted_slot = 0
  AND account.pool IN (`+bindPlaceholders(len(chunk))+`)`,
			args...,
		)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			var hash []byte
			var raw sql.NullString
			if err := rows.Scan(&hash, &raw); err != nil {
				rows.Close()
				return nil, nil, err
			}
			if !raw.Valid || raw.String == "" {
				continue
			}
			amount, err := parseUint64("pool stake amount", raw.String)
			if err != nil {
				rows.Close()
				return nil, nil, err
			}
			key := string(hash)
			if ^uint64(0)-stakes[key] < amount {
				rows.Close()
				return nil, nil, fmt.Errorf("pool stake overflow for %x", hash)
			}
			stakes[key] += amount
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, nil, err
		}
	}
	return stakes, delegators, nil
}

// GetPoolRegistrationsAtSlot returns the latest registration for every
// requested pool at or before slot. Certificate position breaks same-slot
// ties, with the row ID as a deterministic final fallback.
func (s *Store) GetPoolRegistrationsAtSlot(
	poolKeyHashes []lcommon.PoolKeyHash,
	slot uint64,
	txn types.Txn,
) ([]models.PoolRegistration, error) {
	ret := []models.PoolRegistration{}
	if len(poolKeyHashes) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		args := make([]any, 0, end-start+1)
		for _, poolKeyHash := range poolKeyHashes[start:end] {
			args = append(args, poolKeyHash.Bytes())
		}
		args = append(args, slot)
		rows, err := db.QueryContext(ctx, `
WITH ranked AS (
    SELECT pr.id,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_key_hash
               ORDER BY pr.added_slot DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC,
                        pr.id DESC
           ) rn
    FROM pool_registration pr
    LEFT JOIN certs c ON c.id = pr.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE pr.pool_key_hash IN (`+
			bindPlaceholders(end-start)+`) AND pr.added_slot <= ?
)
SELECT pr.margin, pr.metadata_url, pr.vrf_key_hash, pr.pool_key_hash,
       pr.reward_account, pr.reward_account_credential_tag, pr.metadata_hash,
       pr.pledge, pr.cost, pr.certificate_id, pr.id, pr.pool_id,
       pr.added_slot, pr.deposit_amount, pr.leios_key_public,
       pr.leios_key_possession_proof
FROM ranked r
JOIN pool_registration pr ON pr.id = r.id
WHERE r.rn = 1`,
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"GetPoolRegistrationsAtSlot: query registrations: %w",
				err,
			)
		}
		batch := make([]*models.PoolRegistration, 0)
		for rows.Next() {
			registration, err := scanPoolRegistration(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			batch = append(batch, registration)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if err := loadPoolRegistrationChildrenBatch(ctx, db, batch); err != nil {
			return nil, err
		}
		for _, registration := range batch {
			ret = append(ret, *registration)
		}
	}
	return ret, nil
}

func (s *Store) GetPoolRegistrationsEffectiveForEpoch(
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
	txn types.Txn,
) ([]models.PoolRegistration, error) {
	ret := []models.PoolRegistration{}
	if len(poolKeyHashes) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	preEpochRegistrationIDs := []uint{}
	freshPools := [][]byte{}
	for start := 0; start < len(poolKeyHashes); start += 200 {
		end := min(start+200, len(poolKeyHashes))
		hashes := make([][]byte, end-start)
		args := make([]any, 0, 2*len(hashes)+3)
		for i, poolKeyHash := range poolKeyHashes[start:end] {
			hashes[i] = poolKeyHash.Bytes()
			args = append(args, hashes[i])
		}
		args = append(args, epochStartSlot)
		for _, hash := range hashes {
			args = append(args, hash)
		}
		args = append(args, epochStartSlot, endedEpoch)
		rows, err := db.QueryContext(ctx, `
WITH events AS (
    SELECT registration.pool_key_hash, registration.id registration_id,
           0 is_retirement, registration.added_slot,
           COALESCE(tx.block_index, 0) block_index,
           COALESCE(certs.cert_index, 0) cert_index
    FROM pool_registration registration
    LEFT JOIN certs ON certs.id = registration.certificate_id
    LEFT JOIN "transaction" tx ON tx.id = certs.transaction_id
    WHERE registration.pool_key_hash IN (`+
			bindPlaceholders(len(hashes))+`)
      AND registration.added_slot < ?
    UNION ALL
    SELECT retirement.pool_key_hash, 0, 1, retirement.added_slot,
           COALESCE(tx.block_index, 0),
           COALESCE(certs.cert_index, 0)
    FROM pool_retirement retirement
    LEFT JOIN certs ON certs.id = retirement.certificate_id
    LEFT JOIN "transaction" tx ON tx.id = certs.transaction_id
    WHERE retirement.pool_key_hash IN (`+
			bindPlaceholders(len(hashes))+`)
      AND retirement.added_slot < ? AND retirement.epoch <= ?
),
ranked AS (
    SELECT pool_key_hash, registration_id, is_retirement,
           ROW_NUMBER() OVER (
               PARTITION BY pool_key_hash
               ORDER BY added_slot DESC, block_index DESC, cert_index DESC,
                        is_retirement DESC, registration_id DESC
           ) rn
    FROM events
)
SELECT pool_key_hash, registration_id, is_retirement
FROM ranked WHERE rn = 1`,
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"query pre-epoch pool cert events: %w",
				err,
			)
		}
		seen := make(map[string]struct{}, len(hashes))
		for rows.Next() {
			var hash []byte
			var registrationID uint
			var isRetirement bool
			if err := rows.Scan(
				&hash,
				&registrationID,
				&isRetirement,
			); err != nil {
				rows.Close()
				return nil, err
			}
			seen[string(hash)] = struct{}{}
			if !isRetirement && registrationID != 0 {
				preEpochRegistrationIDs = append(
					preEpochRegistrationIDs,
					registrationID,
				)
			} else {
				freshPools = append(freshPools, hash)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("query pool certificate events: %w", err)
		}
		for _, hash := range hashes {
			if _, ok := seen[string(hash)]; !ok {
				freshPools = append(freshPools, hash)
			}
		}
	}
	registrationIDs := append([]uint{}, preEpochRegistrationIDs...)
	for start := 0; start < len(freshPools); start += 400 {
		end := min(start+400, len(freshPools))
		args := make([]any, 0, end-start+2)
		for _, hash := range freshPools[start:end] {
			args = append(args, hash)
		}
		args = append(args, epochStartSlot, snapshotSlot)
		rows, err := db.QueryContext(ctx, `
WITH ranked AS (
    SELECT registration.id,
           ROW_NUMBER() OVER (
               PARTITION BY registration.pool_key_hash
               ORDER BY registration.added_slot ASC,
                        COALESCE(tx.block_index, 0) ASC,
                        COALESCE(certs.cert_index, 0) ASC,
                        registration.id ASC
           ) rn
    FROM pool_registration registration
    LEFT JOIN certs ON certs.id = registration.certificate_id
    LEFT JOIN "transaction" tx ON tx.id = certs.transaction_id
    WHERE registration.pool_key_hash IN (`+
			bindPlaceholders(end-start)+`)
      AND registration.added_slot >= ?
      AND registration.added_slot <= ?
)
SELECT id FROM ranked WHERE rn = 1`,
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"query in-epoch fresh pool registrations: %w",
				err,
			)
		}
		for rows.Next() {
			var id uint
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			registrationIDs = append(registrationIDs, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf(
				"query in-epoch fresh pool registrations: %w",
				err,
			)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	for start := 0; start < len(registrationIDs); start += 400 {
		end := min(start+400, len(registrationIDs))
		args := make([]any, end-start)
		for i, id := range registrationIDs[start:end] {
			args[i] = id
		}
		rows, err := db.QueryContext(ctx, `
SELECT p.margin, p.metadata_url, p.vrf_key_hash, p.pool_key_hash, p.reward_account,
       p.reward_account_credential_tag, p.metadata_hash, p.pledge, p.cost,
       p.certificate_id, p.id, p.pool_id, p.added_slot, p.deposit_amount,
       p.leios_key_public, p.leios_key_possession_proof
FROM pool_registration p
WHERE p.id IN (`+bindPlaceholders(len(args))+`)`,
			args...,
		)
		if err != nil {
			return nil, err
		}
		batch := make([]*models.PoolRegistration, 0)
		for rows.Next() {
			registration, err := scanPoolRegistration(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			batch = append(batch, registration)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if err := loadPoolRegistrationChildrenBatch(ctx, db, batch); err != nil {
			return nil, err
		}
		for _, registration := range batch {
			ret = append(ret, *registration)
		}
	}
	return ret, nil
}

// GetPoolRegistrations reconstructs the ledger certificates for a pool.
func (s *Store) GetPoolRegistrations(
	poolKeyHash lcommon.PoolKeyHash,
	txn types.Txn,
) ([]lcommon.PoolRegistrationCertificate, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
	SELECT p.margin, p.metadata_url, p.vrf_key_hash, p.pool_key_hash, p.reward_account,
	       p.reward_account_credential_tag, p.metadata_hash, p.pledge, p.cost,
	       p.certificate_id, p.id, p.pool_id, p.added_slot, p.deposit_amount,
	       p.leios_key_public, p.leios_key_possession_proof
FROM pool_registration p
WHERE p.pool_key_hash = ?
ORDER BY p.id DESC`,
		poolKeyHash.Bytes(),
	)
	if err != nil {
		return nil, err
	}
	registrations := []*models.PoolRegistration{}
	for rows.Next() {
		registration, err := scanPoolRegistration(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := loadPoolRegistrationChildrenBatch(ctx, db, registrations); err != nil {
		return nil, err
	}
	ret := make([]lcommon.PoolRegistrationCertificate, 0, len(registrations))
	for _, registration := range registrations {
		if registration.Margin == nil || registration.Margin.Rat == nil {
			return nil, fmt.Errorf(
				"pool registration margin is nil (id=%d)",
				registration.ID,
			)
		}
		certificate := lcommon.PoolRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypePoolRegistration),
			Operator: lcommon.PoolKeyHash(
				lcommon.NewBlake2b224(registration.PoolKeyHash),
			),
			VrfKeyHash: lcommon.VrfKeyHash(
				lcommon.NewBlake2b256(registration.VrfKeyHash),
			),
			Pledge: uint64(registration.Pledge),
			Cost:   uint64(registration.Cost),
			Margin: lcommon.GenesisRat{Rat: registration.Margin.Rat},
			RewardAccount: lcommon.AddrKeyHash(
				lcommon.NewBlake2b224(registration.RewardAccount),
			),
		}
		if len(registration.LeiosKeyPublic) > 0 {
			certificate.LeiosKey = &lcommon.LeiosKey{
				PublicKey:       registration.LeiosKeyPublic,
				PossessionProof: registration.LeiosKeyPossessionProof,
			}
		}
		for _, owner := range registration.Owners {
			certificate.PoolOwners = append(
				certificate.PoolOwners,
				lcommon.AddrKeyHash(lcommon.NewBlake2b224(owner.KeyHash)),
			)
		}
		for _, relay := range registration.Relays {
			converted, err := ledgerPoolRelay(relay)
			if err != nil {
				return nil, err
			}
			certificate.Relays = append(certificate.Relays, converted)
		}
		if registration.MetadataUrl != "" {
			certificate.PoolMetadata = &lcommon.PoolMetadata{
				Url: registration.MetadataUrl,
				Hash: lcommon.PoolMetadataHash(
					lcommon.NewBlake2b256(registration.MetadataHash),
				),
			}
		}
		ret = append(ret, certificate)
	}
	return ret, nil
}

// GetActivePoolRelays returns relays from each active pool's latest
// registration. The query uses the same chain-position ordering as active-pool
// selection and avoids loading historical registrations into memory.
func (s *Store) GetActivePoolRelays(
	txn types.Txn,
) ([]models.PoolRegistrationRelay, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("GetActivePoolRelays: resolve db: %w", err)
	}
	current, ok, err := currentEpoch(ctx, db)
	if err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolRelays: get current epoch: %w",
			err,
		)
	}
	if !ok {
		return []models.PoolRegistrationRelay{}, nil
	}
	rows, err := db.QueryContext(ctx, `
WITH latest_reg AS (
    SELECT pr.id, pr.pool_id, pr.added_slot,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_id
               ORDER BY pr.added_slot DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC,
                        pr.id DESC
           ) rn
    FROM pool_registration pr
    LEFT JOIN certs c ON c.id = pr.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
),
latest_ret AS (
    SELECT rt.pool_id, rt.epoch, rt.added_slot,
           CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END synthetic,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY rt.pool_id
               ORDER BY rt.added_slot DESC,
                        CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC,
                        rt.id DESC
           ) rn
    FROM pool_retirement rt
    LEFT JOIN certs c ON c.id = rt.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
)
SELECT relay.ipv4, relay.ipv6, relay.hostname, relay.id,
       relay.pool_registration_id, relay.pool_id, relay.port
FROM latest_reg reg
LEFT JOIN latest_ret ret ON ret.pool_id = reg.pool_id AND ret.rn = 1
JOIN pool_registration_relay relay ON relay.pool_registration_id = reg.id
WHERE reg.rn = 1
  AND (
      ret.pool_id IS NULL
      OR ret.added_slot < reg.added_slot
      OR (ret.added_slot = reg.added_slot AND ret.synthetic = 0
          AND ret.block_index < reg.block_index)
      OR (ret.added_slot = reg.added_slot AND ret.synthetic = 0
          AND ret.block_index = reg.block_index
          AND ret.cert_index < reg.cert_index)
      OR ret.epoch > ?
  )
ORDER BY relay.id`,
		current,
	)
	if err != nil {
		return nil, fmt.Errorf("GetActivePoolRelays: query pools: %w", err)
	}
	defer rows.Close()
	ret := []models.PoolRegistrationRelay{}
	for rows.Next() {
		var relay models.PoolRegistrationRelay
		var ipv4 []byte
		var ipv6 []byte
		if err := rows.Scan(
			&ipv4,
			&ipv6,
			&relay.Hostname,
			&relay.ID,
			&relay.PoolRegistrationID,
			&relay.PoolID,
			&relay.Port,
		); err != nil {
			return nil, err
		}
		relay.Ipv4 = netIPPointer(ipv4)
		relay.Ipv6 = netIPPointer(ipv6)
		ret = append(ret, relay)
	}
	return ret, rows.Err()
}

func (s *Store) GetPoolsRetiringAtEpoch(
	epoch uint64,
	boundarySlot uint64,
	txn types.Txn,
) ([]models.PoolRetirementRefund, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetPoolsRetiringAtEpoch: resolve db: %w",
			err,
		)
	}
	rows, err := db.QueryContext(ctx, `
WITH latest_reg AS (
    SELECT pr.pool_id, pr.added_slot, pr.reward_account,
           pr.reward_account_credential_tag, pr.deposit_amount,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_id
               ORDER BY pr.added_slot DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_registration pr
    LEFT JOIN certs c ON c.id = pr.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE pr.added_slot < ?
),
latest_ret AS (
    SELECT rt.pool_id, rt.added_slot, rt.epoch,
           CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END synthetic_ret,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY rt.pool_id
               ORDER BY rt.added_slot DESC,
                        CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_retirement rt
    LEFT JOIN certs c ON c.id = rt.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE rt.added_slot < ?
)
SELECT p.pool_key_hash, reg.reward_account,
       reg.reward_account_credential_tag, reg.deposit_amount
FROM pool p
JOIN latest_reg reg ON reg.pool_id = p.id AND reg.rn = 1
JOIN latest_ret ret ON ret.pool_id = p.id AND ret.rn = 1
WHERE ret.epoch = ?
  AND NOT (
      ret.added_slot < reg.added_slot
      OR (ret.added_slot = reg.added_slot AND ret.synthetic_ret = 0
          AND ret.block_index < reg.block_index)
      OR (ret.added_slot = reg.added_slot AND ret.synthetic_ret = 0
          AND ret.block_index = reg.block_index
          AND ret.cert_index < reg.cert_index)
  )`,
		boundarySlot,
		boundarySlot,
		epoch,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"GetPoolsRetiringAtEpoch: query pools: %w",
			err,
		)
	}
	defer rows.Close()
	ret := []models.PoolRetirementRefund{}
	for rows.Next() {
		var refund models.PoolRetirementRefund
		var deposit sql.NullString
		if err := rows.Scan(
			&refund.PoolKeyHash,
			&refund.RewardAccount,
			&refund.RewardAccountCredentialTag,
			&deposit,
		); err != nil {
			return nil, err
		}
		value, err := parseNullUint64("pool retirement deposit", deposit)
		if err != nil {
			return nil, err
		}
		refund.DepositAmount = types.Uint64(value)
		ret = append(ret, refund)
	}
	return ret, rows.Err()
}

// GetPoolKeyHashesRetiredByEpoch is GetPoolsRetiringAtEpoch's "at or before"
// sibling: same latest-certificate resolution and same cancellation rule, but
// it matches every retirement effective up to and including epoch rather than
// only the one landing on it, and returns bare key hashes because no deposit
// refund is being applied. See MetadataStore's doc comment for why the parity
// checker needs the wider comparison (dingo #3925).
//
// "Same resolution" includes the synthetic-retirement key every
// latest-retirement query in the tree shares. A reconcile retirement
// (certificate_id = 0) has no certs/transaction join, so its COALESCE'd
// block_index/cert_index are both zero: without ranking it first and exempting
// it from the same-slot cancellation clauses it would lose the tie-break to any
// certificate-backed registration in its own slot, and the pool would be
// reported as still active. ledgerstate's snapshot import writes exactly that
// shape — ImportPool followed by RetirePools at one slot — so this is the
// ordinary bootstrap case, not an edge case. DingoDB.GetPoolsRetiredByEpoch
// carries the same three elements, and koiosparity's
// implementations-agree test runs both against one database to pin them
// against drift.
func (s *Store) GetPoolKeyHashesRetiredByEpoch(
	epoch uint64,
	boundarySlot uint64,
	txn types.Txn,
) ([][]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetPoolKeyHashesRetiredByEpoch: resolve db: %w",
			err,
		)
	}
	rows, err := db.QueryContext(ctx, `
WITH latest_reg AS (
    SELECT pr.pool_id, pr.added_slot,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY pr.pool_id
               ORDER BY pr.added_slot DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_registration pr
    LEFT JOIN certs c ON c.id = pr.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE pr.added_slot < ?
),
latest_ret AS (
    SELECT rt.pool_id, rt.added_slot, rt.epoch,
           CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END synthetic_ret,
           COALESCE(t.block_index, 0) block_index,
           COALESCE(c.cert_index, 0) cert_index,
           ROW_NUMBER() OVER (
               PARTITION BY rt.pool_id
               ORDER BY rt.added_slot DESC,
                        CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END DESC,
                        COALESCE(t.block_index, 0) DESC,
                        COALESCE(c.cert_index, 0) DESC
           ) rn
    FROM pool_retirement rt
    LEFT JOIN certs c ON c.id = rt.certificate_id
    LEFT JOIN "transaction" t ON t.id = c.transaction_id
    WHERE rt.added_slot < ?
)
SELECT p.pool_key_hash
FROM pool p
JOIN latest_reg reg ON reg.pool_id = p.id AND reg.rn = 1
JOIN latest_ret ret ON ret.pool_id = p.id AND ret.rn = 1
WHERE ret.epoch <= ?
  AND NOT (
      ret.added_slot < reg.added_slot
      OR (ret.added_slot = reg.added_slot AND ret.synthetic_ret = 0
          AND ret.block_index < reg.block_index)
      OR (ret.added_slot = reg.added_slot AND ret.synthetic_ret = 0
          AND ret.block_index = reg.block_index
          AND ret.cert_index < reg.cert_index)
  )`,
		boundarySlot,
		boundarySlot,
		epoch,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"GetPoolKeyHashesRetiredByEpoch: query pools: %w",
			err,
		)
	}
	defer rows.Close()
	ret := [][]byte{}
	for rows.Next() {
		var poolKeyHash []byte
		if err := rows.Scan(&poolKeyHash); err != nil {
			return nil, err
		}
		ret = append(ret, poolKeyHash)
	}
	return ret, rows.Err()
}

func (s *Store) RestorePoolStateAtSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(
				ctx,
				"DELETE FROM pool_opcert_sequence WHERE slot > ?",
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM pool
WHERE NOT EXISTS (
    SELECT 1 FROM pool_registration registration
    WHERE registration.pool_id = pool.id
      AND registration.added_slot <= ?
)`,
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `
WITH ranked AS (
    SELECT registration.pool_id, registration.pledge, registration.cost,
           registration.margin, registration.vrf_key_hash,
           registration.reward_account,
           registration.reward_account_credential_tag,
           registration.leios_key_public,
           registration.leios_key_possession_proof,
           ROW_NUMBER() OVER (
               PARTITION BY registration.pool_id
               ORDER BY registration.added_slot DESC,
                        COALESCE(tx.block_index, 0) DESC,
                        COALESCE(certs.cert_index, 0) DESC,
                        registration.id DESC
           ) rn
    FROM pool_registration registration
    LEFT JOIN certs ON certs.id = registration.certificate_id
    LEFT JOIN "transaction" tx ON tx.id = certs.transaction_id
    WHERE registration.added_slot <= ?
)
UPDATE pool
SET pledge = (SELECT pledge FROM ranked
              WHERE ranked.pool_id = pool.id AND rn = 1),
    cost = (SELECT cost FROM ranked
            WHERE ranked.pool_id = pool.id AND rn = 1),
    margin = (SELECT margin FROM ranked
              WHERE ranked.pool_id = pool.id AND rn = 1),
    vrf_key_hash = (SELECT vrf_key_hash FROM ranked
                    WHERE ranked.pool_id = pool.id AND rn = 1),
    reward_account = (SELECT reward_account FROM ranked
                      WHERE ranked.pool_id = pool.id AND rn = 1),
    reward_account_credential_tag = (
        SELECT reward_account_credential_tag FROM ranked
        WHERE ranked.pool_id = pool.id AND rn = 1
    ),
    leios_key_public = (
        SELECT leios_key_public FROM ranked
        WHERE ranked.pool_id = pool.id AND rn = 1
    ),
    leios_key_possession_proof = (
        SELECT leios_key_possession_proof FROM ranked
        WHERE ranked.pool_id = pool.id AND rn = 1
    )
WHERE EXISTS (
    SELECT 1 FROM ranked WHERE ranked.pool_id = pool.id AND rn = 1
)`,
				slot,
			); err != nil {
				return err
			}
			_, err := db.ExecContext(ctx, `
UPDATE pool
SET latest_op_cert_sequence = COALESCE((
    SELECT MAX(sequence) FROM pool_opcert_sequence sequence
    WHERE sequence.pool_key_hash = pool.pool_key_hash
), 0)`)
			return err
		},
	)
}

func ledgerPoolRelay(
	relay models.PoolRegistrationRelay,
) (lcommon.PoolRelay, error) {
	ret := lcommon.PoolRelay{}
	if relay.Port != 0 {
		if relay.Port > math.MaxUint32 {
			return ret, fmt.Errorf(
				"pool relay port out of range: %d",
				relay.Port,
			)
		}
		port := uint32(relay.Port)
		ret.Port = &port
		if relay.Hostname != "" {
			hostname := relay.Hostname
			ret.Type = lcommon.PoolRelayTypeSingleHostName
			ret.Hostname = &hostname
		} else {
			ret.Type = lcommon.PoolRelayTypeSingleHostAddress
			ret.Ipv4 = relay.Ipv4
			ret.Ipv6 = relay.Ipv6
		}
		return ret, nil
	}
	if relay.Ipv4 != nil || relay.Ipv6 != nil {
		ret.Type = lcommon.PoolRelayTypeSingleHostAddress
		ret.Ipv4 = relay.Ipv4
		ret.Ipv6 = relay.Ipv6
	} else if relay.Hostname != "" {
		hostname := relay.Hostname
		ret.Type = lcommon.PoolRelayTypeMultiHostName
		ret.Hostname = &hostname
	}
	return ret, nil
}

func queryPool(
	ctx context.Context,
	db queryer,
	predicate string,
	args ...any,
) (*models.Pool, error) {
	row := db.QueryRowContext(ctx, `
SELECT margin, pool_key_hash, vrf_key_hash, reward_account,
       latest_op_cert_sequence, reward_account_credential_tag, id,
       pledge, cost, leios_key_public, leios_key_possession_proof
FROM pool WHERE `+predicate+` LIMIT 1`,
		args...,
	)
	pool, err := scanPool(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return pool, err
}

type rowScanner interface {
	Scan(...any) error
}

func scanPool(row rowScanner) (*models.Pool, error) {
	var pool models.Pool
	var margin sql.NullString
	var latest sql.NullInt64
	var credentialTag int64
	var pledge sql.NullString
	var cost sql.NullString
	err := row.Scan(
		&margin,
		&pool.PoolKeyHash,
		&pool.VrfKeyHash,
		&pool.RewardAccount,
		&latest,
		&credentialTag,
		&pool.ID,
		&pledge,
		&cost,
		&pool.LeiosKeyPublic,
		&pool.LeiosKeyPossessionProof,
	)
	if err != nil {
		return nil, err
	}
	pool.Margin, err = parseRat(margin)
	if err != nil {
		return nil, err
	}
	pledgeValue, err := parseNullUint64("pool pledge", pledge)
	if err != nil {
		return nil, err
	}
	costValue, err := parseNullUint64("pool cost", cost)
	if err != nil {
		return nil, err
	}
	pool.LatestOpCertSequence = uint64(latest.Int64)
	pool.RewardAccountCredentialTag = uint8(credentialTag)
	pool.Pledge = types.Uint64(pledgeValue)
	pool.Cost = types.Uint64(costValue)
	return &pool, nil
}

func (s *Store) loadPoolAssociations(
	ctx context.Context,
	db queryer,
	pool *models.Pool,
	latestOnly bool,
) error {
	pool.Registration = []models.PoolRegistration{}
	pool.Retirement = []models.PoolRetirement{}
	query := `
SELECT p.margin, p.metadata_url, p.vrf_key_hash, p.pool_key_hash, p.reward_account,
       p.reward_account_credential_tag, p.metadata_hash, p.pledge, p.cost,
       p.certificate_id, p.id, p.pool_id, p.added_slot, p.deposit_amount,
       p.leios_key_public, p.leios_key_possession_proof
FROM pool_registration p
LEFT JOIN certs c ON c.id = p.certificate_id
LEFT JOIN ` + s.dialect.QuoteIdentifier("transaction") + ` tx ON tx.id = c.transaction_id
WHERE p.pool_id = ?
ORDER BY p.added_slot DESC, COALESCE(tx.block_index, 0) DESC,
         COALESCE(c.cert_index, 0) DESC, p.id DESC`
	if latestOnly {
		query += " LIMIT 1"
	}
	rows, err := db.QueryContext(ctx, query, pool.ID)
	if err != nil {
		return fmt.Errorf("load pool registrations query: %w", err)
	}
	// Collect every registration row and fully close this cursor before
	// issuing any nested owner/relay query on the same queryer. On SQLite,
	// concurrently open cursors on one connection are fine, but MySQL and
	// PostgreSQL connections are strictly request/response: issuing a new
	// query while this outer result set is still open corrupts the
	// connection (observed as go-sql-driver/mysql's "busy buffer" /
	// "unexpected sequence nr", and equivalent protocol-desync failures on
	// PostgreSQL) once that connection is returned to the pool and reused
	// -- see the conformance suite's real-backend investigation. Load
	// children only after this cursor is closed, matching the safe
	// collect-then-batch-load pattern loadPoolsAssociations already uses.
	registrations := make([]*models.PoolRegistration, 0)
	for rows.Next() {
		registration, err := scanPoolRegistration(rows)
		if err != nil {
			rows.Close()
			return err
		}
		registrations = append(registrations, registration)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, registration := range registrations {
		if err := loadPoolRegistrationChildren(ctx, db, registration); err != nil {
			return err
		}
		pool.Registration = append(pool.Registration, *registration)
	}
	rows, err = db.QueryContext(ctx, `
SELECT r.pool_key_hash, r.certificate_id, r.id, r.pool_id, r.epoch, r.added_slot
FROM pool_retirement r
LEFT JOIN certs c ON c.id = r.certificate_id
LEFT JOIN `+s.dialect.QuoteIdentifier("transaction")+` tx ON tx.id = c.transaction_id
WHERE r.pool_id = ?
ORDER BY r.added_slot DESC, COALESCE(tx.block_index, 0) DESC,
         COALESCE(c.cert_index, 0) DESC,
         CASE WHEN r.certificate_id = 0 THEN 1 ELSE 0 END DESC, r.id DESC`,
		pool.ID,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var retirement models.PoolRetirement
		var certificateID sql.NullInt64
		var epoch sql.NullInt64
		var addedSlot sql.NullInt64
		if err := rows.Scan(
			&retirement.PoolKeyHash,
			&certificateID,
			&retirement.ID,
			&retirement.PoolID,
			&epoch,
			&addedSlot,
		); err != nil {
			rows.Close()
			return err
		}
		retirement.CertificateID = uint(certificateID.Int64)
		retirement.Epoch = uint64(epoch.Int64)
		retirement.AddedSlot = uint64(addedSlot.Int64)
		pool.Retirement = append(pool.Retirement, retirement)
		if latestOnly {
			break
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return rows.Err()
}

// loadPoolsAssociations hydrates all registrations, owners, and relays for a
// pool slice with bounded set queries.  GetPools is used by pool-list APIs and
// ledger snapshots, where the old per-pool loader issued three round trips for
// every result.  Keep the association order identical to loadPoolAssociations
// while reducing the query count to one query per association table (per
// parameter-limit chunk).
func (s *Store) loadPoolsAssociations(
	ctx context.Context,
	db queryer,
	pools []models.Pool,
) error {
	if len(pools) == 0 {
		return nil
	}
	poolIDs := make([]any, len(pools))
	poolByID := make(map[uint]int, len(pools))
	for i := range pools {
		pools[i].Registration = []models.PoolRegistration{}
		pools[i].Retirement = []models.PoolRetirement{}
		poolIDs[i] = pools[i].ID
		poolByID[pools[i].ID] = i
	}
	for start := 0; start < len(poolIDs); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(poolIDs))
		query := `
SELECT p.margin, p.metadata_url, p.vrf_key_hash, p.pool_key_hash, p.reward_account,
       p.reward_account_credential_tag, p.metadata_hash, p.pledge, p.cost,
       p.certificate_id, p.id, p.pool_id, p.added_slot, p.deposit_amount,
       p.leios_key_public, p.leios_key_possession_proof
FROM pool_registration p
LEFT JOIN certs c ON c.id = p.certificate_id
LEFT JOIN ` + s.dialect.QuoteIdentifier("transaction") + ` tx ON tx.id = c.transaction_id
WHERE p.pool_id IN (` + bindPlaceholders(end-start) + `)
ORDER BY p.pool_id, p.added_slot DESC, COALESCE(tx.block_index, 0) DESC,
         COALESCE(c.cert_index, 0) DESC, p.id DESC`
		rows, err := db.QueryContext(
			ctx,
			s.dialect.Rebind(query),
			poolIDs[start:end]...,
		)
		if err != nil {
			return fmt.Errorf("load pool registrations query: %w", err)
		}
		for rows.Next() {
			registration, err := scanPoolRegistration(rows)
			if err != nil {
				rows.Close()
				return err
			}
			poolIndex, ok := poolByID[registration.PoolID]
			if !ok {
				rows.Close()
				return fmt.Errorf(
					"pool registration %d references unknown pool %d",
					registration.ID,
					registration.PoolID,
				)
			}
			pools[poolIndex].Registration = append(
				pools[poolIndex].Registration,
				*registration,
			)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	registrationCount := 0
	for poolIndex := range pools {
		registrationCount += len(pools[poolIndex].Registration)
	}
	registrations := make([]*models.PoolRegistration, 0, registrationCount)
	for poolIndex := range pools {
		for registrationIndex := range pools[poolIndex].Registration {
			registrations = append(
				registrations,
				&pools[poolIndex].Registration[registrationIndex],
			)
		}
	}
	if err := loadPoolRegistrationChildrenBatch(ctx, db, registrations); err != nil {
		return err
	}
	return s.loadPoolRetirementsBatch(ctx, db, pools)
}

func (s *Store) loadPoolRetirementsBatch(
	ctx context.Context,
	db queryer,
	pools []models.Pool,
) error {
	poolIDs := make([]any, len(pools))
	poolByID := make(map[uint]int, len(pools))
	for i := range pools {
		poolIDs[i] = pools[i].ID
		poolByID[pools[i].ID] = i
	}
	for start := 0; start < len(poolIDs); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(poolIDs))
		query := `
SELECT r.pool_key_hash, r.certificate_id, r.id, r.pool_id, r.epoch, r.added_slot
FROM pool_retirement r
LEFT JOIN certs c ON c.id = r.certificate_id
LEFT JOIN ` + s.dialect.QuoteIdentifier("transaction") + ` tx ON tx.id = c.transaction_id
WHERE r.pool_id IN (` + bindPlaceholders(end-start) + `)
ORDER BY r.pool_id, r.added_slot DESC, COALESCE(tx.block_index, 0) DESC,
         COALESCE(c.cert_index, 0) DESC,
         CASE WHEN r.certificate_id = 0 THEN 1 ELSE 0 END DESC, r.id DESC`
		rows, err := db.QueryContext(
			ctx,
			s.dialect.Rebind(query),
			poolIDs[start:end]...)
		if err != nil {
			return fmt.Errorf("load pool retirements: %w", err)
		}
		for rows.Next() {
			var retirement models.PoolRetirement
			var certificateID, epoch, addedSlot sql.NullInt64
			if err := rows.Scan(&retirement.PoolKeyHash, &certificateID, &retirement.ID, &retirement.PoolID, &epoch, &addedSlot); err != nil {
				rows.Close()
				return err
			}
			retirement.CertificateID = uint(certificateID.Int64)
			retirement.Epoch = uint64(epoch.Int64)
			retirement.AddedSlot = uint64(addedSlot.Int64)
			index, ok := poolByID[retirement.PoolID]
			if !ok {
				rows.Close()
				return fmt.Errorf(
					"pool retirement %d references unknown pool %d",
					retirement.ID,
					retirement.PoolID,
				)
			}
			pools[index].Retirement = append(
				pools[index].Retirement,
				retirement,
			)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func scanPoolRegistration(
	row rowScanner,
) (*models.PoolRegistration, error) {
	var registration models.PoolRegistration
	var margin sql.NullString
	var metadataURL sql.NullString
	var credentialTag int64
	var pledge sql.NullString
	var cost sql.NullString
	var deposit sql.NullString
	var certificateID sql.NullInt64
	var addedSlot sql.NullInt64
	err := row.Scan(
		&margin,
		&metadataURL,
		&registration.VrfKeyHash,
		&registration.PoolKeyHash,
		&registration.RewardAccount,
		&credentialTag,
		&registration.MetadataHash,
		&pledge,
		&cost,
		&certificateID,
		&registration.ID,
		&registration.PoolID,
		&addedSlot,
		&deposit,
		&registration.LeiosKeyPublic,
		&registration.LeiosKeyPossessionProof,
	)
	if err != nil {
		return nil, err
	}
	registration.Margin, err = parseRat(margin)
	if err != nil {
		return nil, err
	}
	pledgeValue, err := parseNullUint64("pool registration pledge", pledge)
	if err != nil {
		return nil, err
	}
	costValue, err := parseNullUint64("pool registration cost", cost)
	if err != nil {
		return nil, err
	}
	depositValue, err := parseNullUint64(
		"pool registration deposit",
		deposit,
	)
	if err != nil {
		return nil, err
	}
	registration.MetadataUrl = metadataURL.String
	registration.CertificateID = uint(certificateID.Int64)
	registration.AddedSlot = uint64(addedSlot.Int64)
	registration.RewardAccountCredentialTag = uint8(credentialTag)
	registration.Pledge = types.Uint64(pledgeValue)
	registration.Cost = types.Uint64(costValue)
	registration.DepositAmount = types.Uint64(depositValue)
	registration.Owners = []models.PoolRegistrationOwner{}
	registration.Relays = []models.PoolRegistrationRelay{}
	return &registration, nil
}

func loadPoolRegistrationChildren(
	ctx context.Context,
	db queryer,
	registration *models.PoolRegistration,
) error {
	rows, err := db.QueryContext(ctx, `
SELECT key_hash, id, pool_registration_id, pool_id
FROM pool_registration_owner
WHERE pool_registration_id = ?`,
		registration.ID,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var owner models.PoolRegistrationOwner
		if err := rows.Scan(
			&owner.KeyHash,
			&owner.ID,
			&owner.PoolRegistrationID,
			&owner.PoolID,
		); err != nil {
			rows.Close()
			return err
		}
		registration.Owners = append(registration.Owners, owner)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows, err = db.QueryContext(ctx, `
SELECT ipv4, ipv6, hostname, id, pool_registration_id, pool_id, port
FROM pool_registration_relay
WHERE pool_registration_id = ?`,
		registration.ID,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var relay models.PoolRegistrationRelay
		var ipv4 []byte
		var ipv6 []byte
		if err := rows.Scan(
			&ipv4,
			&ipv6,
			&relay.Hostname,
			&relay.ID,
			&relay.PoolRegistrationID,
			&relay.PoolID,
			&relay.Port,
		); err != nil {
			rows.Close()
			return err
		}
		relay.Ipv4 = netIPPointer(ipv4)
		relay.Ipv6 = netIPPointer(ipv6)
		registration.Relays = append(registration.Relays, relay)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return rows.Err()
}

// loadPoolRegistrationChildrenBatch hydrates all owner and relay rows for a
// registration slice with two queries per parameter-limited chunk. Keeping
// the grouping in memory avoids the per-registration N+1 query pattern used
// by the legacy helper while preserving child insertion order.
func loadPoolRegistrationChildrenBatch(
	ctx context.Context,
	db queryer,
	registrations []*models.PoolRegistration,
) error {
	if len(registrations) == 0 {
		return nil
	}
	byID := make(map[uint]*models.PoolRegistration, len(registrations))
	ids := make([]any, len(registrations))
	for i, registration := range registrations {
		registration.Owners = []models.PoolRegistrationOwner{}
		registration.Relays = []models.PoolRegistrationRelay{}
		byID[registration.ID] = registration
		ids[i] = registration.ID
	}
	for start := 0; start < len(ids); start += 400 {
		end := min(start+400, len(ids))
		rows, err := db.QueryContext(ctx, `
SELECT key_hash, id, pool_registration_id, pool_id
FROM pool_registration_owner
WHERE pool_registration_id IN (`+bindPlaceholders(end-start)+`)
ORDER BY id`, ids[start:end]...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var owner models.PoolRegistrationOwner
			if err := rows.Scan(&owner.KeyHash, &owner.ID, &owner.PoolRegistrationID, &owner.PoolID); err != nil {
				rows.Close()
				return err
			}
			if registration := byID[owner.PoolRegistrationID]; registration != nil {
				registration.Owners = append(registration.Owners, owner)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		rows, err = db.QueryContext(ctx, `
SELECT ipv4, ipv6, hostname, id, pool_registration_id, pool_id, port
FROM pool_registration_relay
WHERE pool_registration_id IN (`+bindPlaceholders(end-start)+`)
ORDER BY id`, ids[start:end]...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var relay models.PoolRegistrationRelay
			var ipv4, ipv6 []byte
			if err := rows.Scan(&ipv4, &ipv6, &relay.Hostname, &relay.ID, &relay.PoolRegistrationID, &relay.PoolID, &relay.Port); err != nil {
				rows.Close()
				return err
			}
			relay.Ipv4 = netIPPointer(ipv4)
			relay.Ipv6 = netIPPointer(ipv6)
			if registration := byID[relay.PoolRegistrationID]; registration != nil {
				registration.Relays = append(registration.Relays, relay)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func queryReturnedID(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, query, args...).Scan(&id)
	return id, err
}

// latestPoolEventIsRetirement compares registration and retirement events at
// their complete chain position. Slot alone is insufficient because multiple
// certificates may occur in one slot; the transaction block index and
// certificate index provide the remaining ordering, with retirement events
// winning ties through the synthetic is_retirement component.
func latestPoolEventIsRetirement(
	ctx context.Context,
	db queryer,
	dialect Dialect,
	poolID uint,
) (bool, error) {
	var retirement bool
	err := db.QueryRowContext(ctx, `
WITH events AS (
    SELECT 0 AS is_retirement, p.added_slot,
           COALESCE(tx.block_index, 0) AS block_index,
           COALESCE(c.cert_index, 0) AS cert_index,
           p.id AS event_id
    FROM pool_registration p
    LEFT JOIN certs c ON c.id = p.certificate_id
    LEFT JOIN `+dialect.QuoteIdentifier("transaction")+` tx ON tx.id = c.transaction_id
    WHERE p.pool_id = ?
    UNION ALL
    SELECT 1 AS is_retirement, r.added_slot,
           COALESCE(tx.block_index, 0), COALESCE(c.cert_index, 0), r.id
    FROM pool_retirement r
    LEFT JOIN certs c ON c.id = r.certificate_id
    LEFT JOIN `+dialect.QuoteIdentifier("transaction")+` tx ON tx.id = c.transaction_id
    WHERE r.pool_id = ?
)
SELECT is_retirement FROM events
ORDER BY added_slot DESC, block_index DESC, cert_index DESC,
         is_retirement DESC, event_id DESC
LIMIT 1`, poolID, poolID).Scan(&retirement)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return retirement, err
}

func parseRat(value sql.NullString) (*types.Rat, error) {
	return parseNullableRat(value)
}

func netIPValue(value *net.IP) any {
	if value == nil {
		return nil
	}
	return []byte(*value)
}

func netIPPointer(value []byte) *net.IP {
	if len(value) == 0 {
		return nil
	}
	ip := net.IP(value)
	return &ip
}

func currentEpoch(ctx context.Context, db queryer) (uint64, bool, error) {
	var epoch sql.NullInt64
	err := db.QueryRowContext(ctx, `
SELECT epoch_id FROM epoch
WHERE start_slot <= (SELECT slot FROM tip WHERE id = 1)
ORDER BY start_slot DESC
LIMIT 1`).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(epoch.Int64), true, nil
}
