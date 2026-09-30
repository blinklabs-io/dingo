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
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// installPoolAuditTrigger attaches an AFTER UPDATE OF <columns> trigger to
// the pool table that records every row id the update statement actually
// names in its SET list -- SQLite fires an "UPDATE OF column-list" trigger
// whenever the statement's SET clause mentions one of the named columns, even
// when the assigned value is unchanged, which is exactly the "this row was
// rewritten" behavior RestorePoolStateAtSlot's scoping is supposed to avoid
// for an unaffected pool. It must be installed only after every fixture
// write that itself updates pool (ImportPool's ON CONFLICT DO UPDATE path,
// UpdatePoolOpCertSequence's second statement) has already run, or setup
// itself pollutes the audit table. Returns a reader that drains the distinct
// touched pool ids in ascending order.
func installPoolAuditTrigger(
	t *testing.T,
	store *Store,
	name string,
	columns ...string,
) func() []int64 {
	t.Helper()
	ctx := context.Background()
	auditTable := "test_pool_audit_" + name
	_, err := store.writeDB.ExecContext(
		ctx,
		"CREATE TABLE "+auditTable+" (pool_id INTEGER)",
	)
	require.NoError(t, err)
	_, err = store.writeDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TRIGGER trg_%s AFTER UPDATE OF %s ON pool "+
			"BEGIN INSERT INTO %s (pool_id) VALUES (NEW.id); END",
		name, strings.Join(columns, ", "), auditTable,
	))
	require.NoError(t, err)
	return func() []int64 {
		rows, err := store.writeDB.QueryContext(
			ctx,
			"SELECT DISTINCT pool_id FROM "+auditTable+" ORDER BY pool_id",
		)
		require.NoError(t, err)
		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		return ids
	}
}

// poolIDForHash reads back the surrogate id ImportPool assigned to hash, so
// a test can name the exact row an audit trigger is expected (or not
// expected) to record.
func poolIDForHash(t *testing.T, store *Store, hash []byte) int64 {
	t.Helper()
	var id int64
	err := store.writeDB.QueryRowContext(
		context.Background(),
		"SELECT id FROM pool WHERE pool_key_hash = ?",
		hash,
	).Scan(&id)
	require.NoError(t, err)
	return id
}

// TestRestorePoolStateAtSlotScopesDenormalizedUpdateToAffectedPools is the
// scope regression test for the O(pool_count) UPDATE RestorePoolStateAtSlot
// used to issue on every rollback: it registers five pools that are never
// re-registered past the truncate target (nothing to revert) alongside one
// pool that is, and asserts the denormalized-field UPDATE's SET clause names
// only that one pool's row -- not merely that the final values come out
// right, which the pre-existing TestTruncateAfterSlotRestoresPoolDenormalizedFields
// (database/truncate_test.go) already covers.
//
// It also checks every one of the eight denormalized columns
// restorePoolDenormalizedFields reverts, with distinct before/after values
// for each, not just pledge/cost/VRF/reward-account: a swapped assignment in
// the rewritten single-join SET list (for example margin sourced from the
// wrong CTE column) would go uncovered by checking only a subset.
func TestRestorePoolStateAtSlotScopesDenormalizedUpdateToAffectedPools(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	const targetSlot = 1500

	seedUnaffectedPool := func(marker byte) []byte {
		hash := bytes.Repeat([]byte{marker}, 28)
		pool := &models.Pool{
			PoolKeyHash:   hash,
			Pledge:        100,
			Cost:          200,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			VrfKeyHash:    bytes.Repeat([]byte{0xa0}, 32),
			RewardAccount: bytes.Repeat([]byte{0xb0}, 28),
		}
		reg := &models.PoolRegistration{
			PoolKeyHash:   hash,
			AddedSlot:     1000,
			Pledge:        pool.Pledge,
			Cost:          pool.Cost,
			Margin:        pool.Margin,
			VrfKeyHash:    pool.VrfKeyHash,
			RewardAccount: pool.RewardAccount,
		}
		require.NoError(t, store.ImportPool(pool, reg, nil))
		return hash
	}
	for _, marker := range []byte{0x10, 0x11, 0x12, 0x13, 0x14} {
		seedUnaffectedPool(marker)
	}

	// The one affected pool: an initial registration below the target,
	// discarded by a later re-registration above it that changes every
	// denormalized field.
	affectedHash := bytes.Repeat([]byte{0x99}, 28)
	before := &models.Pool{
		PoolKeyHash:                affectedHash,
		Pledge:                     100,
		Cost:                       200,
		Margin:                     &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:                 bytes.Repeat([]byte{0xa1}, 32),
		RewardAccount:              bytes.Repeat([]byte{0xb1}, 28),
		RewardAccountCredentialTag: 0,
		LeiosKeyPublic:             bytes.Repeat([]byte{0xc1}, 96),
		LeiosKeyPossessionProof:    bytes.Repeat([]byte{0xd1}, 48),
	}
	beforeReg := &models.PoolRegistration{
		PoolKeyHash:                affectedHash,
		AddedSlot:                  1000,
		Pledge:                     before.Pledge,
		Cost:                       before.Cost,
		Margin:                     before.Margin,
		VrfKeyHash:                 before.VrfKeyHash,
		RewardAccount:              before.RewardAccount,
		RewardAccountCredentialTag: before.RewardAccountCredentialTag,
		LeiosKeyPublic:             before.LeiosKeyPublic,
		LeiosKeyPossessionProof:    before.LeiosKeyPossessionProof,
	}
	require.NoError(t, store.ImportPool(before, beforeReg, nil))
	affectedID := poolIDForHash(t, store, affectedHash)

	after := &models.Pool{
		PoolKeyHash:                affectedHash,
		Pledge:                     999,
		Cost:                       888,
		Margin:                     &types.Rat{Rat: big.NewRat(2, 100)},
		VrfKeyHash:                 bytes.Repeat([]byte{0xa2}, 32),
		RewardAccount:              bytes.Repeat([]byte{0xb2}, 28),
		RewardAccountCredentialTag: 1,
		LeiosKeyPublic:             bytes.Repeat([]byte{0xc2}, 96),
		LeiosKeyPossessionProof:    bytes.Repeat([]byte{0xd2}, 48),
	}
	afterReg := &models.PoolRegistration{
		PoolKeyHash:                affectedHash,
		AddedSlot:                  2000,
		Pledge:                     after.Pledge,
		Cost:                       after.Cost,
		Margin:                     after.Margin,
		VrfKeyHash:                 after.VrfKeyHash,
		RewardAccount:              after.RewardAccount,
		RewardAccountCredentialTag: after.RewardAccountCredentialTag,
		LeiosKeyPublic:             after.LeiosKeyPublic,
		LeiosKeyPossessionProof:    after.LeiosKeyPossessionProof,
	}
	require.NoError(t, store.ImportPool(after, afterReg, nil))

	// Installed only now: both ImportPool calls above included a real
	// UPDATE (the ON CONFLICT DO UPDATE path), which would otherwise land
	// in the audit table before RestorePoolStateAtSlot ever runs.
	readAudit := installPoolAuditTrigger(
		t, store, "denorm",
		"pledge", "cost", "margin", "vrf_key_hash", "reward_account",
		"reward_account_credential_tag", "leios_key_public",
		"leios_key_possession_proof",
	)

	require.NoError(t, store.RestorePoolStateAtSlot(targetSlot, nil))

	require.Equal(
		t,
		[]int64{affectedID},
		readAudit(),
		"only the pool with a discarded post-target registration may have "+
			"its denormalized fields rewritten by RestorePoolStateAtSlot",
	)

	restored, err := store.GetPool(lcommon.PoolKeyHash(affectedHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), uint64(restored.Pledge))
	require.Equal(t, uint64(200), uint64(restored.Cost))
	require.Equal(t, big.NewRat(1, 100).String(), restored.Margin.String())
	require.Equal(t, before.VrfKeyHash, restored.VrfKeyHash)
	require.Equal(t, before.RewardAccount, restored.RewardAccount)
	require.Equal(
		t,
		before.RewardAccountCredentialTag,
		restored.RewardAccountCredentialTag,
	)
	require.Equal(t, before.LeiosKeyPublic, restored.LeiosKeyPublic)
	require.Equal(
		t,
		before.LeiosKeyPossessionProof,
		restored.LeiosKeyPossessionProof,
	)
}

// seedPoolWithSingleRegistration creates a minimally valid pool with one
// registration at addedSlot, for tests that only care about
// pool_opcert_sequence scoping and not about the pool's own denormalized
// fields.
func seedPoolWithSingleRegistration(
	t *testing.T,
	store *Store,
	hash []byte,
	addedSlot uint64,
) {
	t.Helper()
	pool := &models.Pool{
		PoolKeyHash:   hash,
		Pledge:        1,
		Cost:          1,
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:    bytes.Repeat([]byte{0xaa}, 32),
		RewardAccount: bytes.Repeat([]byte{0xbb}, 28),
	}
	reg := &models.PoolRegistration{
		PoolKeyHash:   hash,
		AddedSlot:     addedSlot,
		Pledge:        pool.Pledge,
		Cost:          pool.Cost,
		Margin:        pool.Margin,
		VrfKeyHash:    pool.VrfKeyHash,
		RewardAccount: pool.RewardAccount,
	}
	require.NoError(t, store.ImportPool(pool, reg, nil))
}

// TestRestorePoolStateAtSlotScopesOpCertSequenceToAffectedPools is the scope
// regression test for latest_op_cert_sequence's matching bug: it covers a
// pool whose pool_opcert_sequence rows are entirely below the truncate
// target (never touched), one that reverts to a surviving earlier sequence,
// and one whose only sequence row is discarded entirely (falls back to 0 via
// COALESCE) -- asserting in each case which pool ids the UPDATE's SET clause
// actually named, not just the pools' final values.
func TestRestorePoolStateAtSlotScopesOpCertSequenceToAffectedPools(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	const targetSlot = 1500

	untouchedHash := bytes.Repeat([]byte{0x21}, 28)
	seedPoolWithSingleRegistration(t, store, untouchedHash, 1000)
	require.NoError(t, store.UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash(untouchedHash), 3, 500, nil,
	))

	revertHash := bytes.Repeat([]byte{0x22}, 28)
	seedPoolWithSingleRegistration(t, store, revertHash, 1000)
	require.NoError(t, store.UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash(revertHash), 1, 500, nil,
	))
	require.NoError(t, store.UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash(revertHash), 5, 2000, nil,
	))

	zeroHash := bytes.Repeat([]byte{0x23}, 28)
	seedPoolWithSingleRegistration(t, store, zeroHash, 1000)
	require.NoError(t, store.UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash(zeroHash), 9, 2000, nil,
	))

	revertID := poolIDForHash(t, store, revertHash)
	zeroID := poolIDForHash(t, store, zeroHash)

	// Installed only now: UpdatePoolOpCertSequence's own second statement is
	// a real UPDATE of latest_op_cert_sequence, which would otherwise be
	// recorded by the trigger during setup above.
	readAudit := installPoolAuditTrigger(
		t, store, "opcert", "latest_op_cert_sequence",
	)

	require.NoError(t, store.RestorePoolStateAtSlot(targetSlot, nil))

	require.ElementsMatch(
		t,
		[]int64{revertID, zeroID},
		readAudit(),
		"only pools whose pool_opcert_sequence rows were actually deleted "+
			"by the truncate may have latest_op_cert_sequence rewritten",
	)

	revertPool, err := store.GetPool(lcommon.PoolKeyHash(revertHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), revertPool.LatestOpCertSequence,
		"must revert to the surviving pre-truncate sequence")

	zeroPool, err := store.GetPool(lcommon.PoolKeyHash(zeroHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), zeroPool.LatestOpCertSequence,
		"must fall back to 0 via COALESCE when every sequence row is discarded")

	untouchedPool, err := store.GetPool(
		lcommon.PoolKeyHash(untouchedHash), true, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), untouchedPool.LatestOpCertSequence,
		"a pool with no discarded op-cert rows must be left exactly as it was")
}
