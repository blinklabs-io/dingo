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
	"database/sql"
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

// The deposit-held tests drive the production storage path -- SetTransaction
// with real certificate values, then GetPoolsRetiringAtEpoch -- rather than
// seeding pool_registration rows directly, because the carry-forward decision
// lives in the certificate write path and only that path exercises it.

const depositHeldEpochLength = 1_000

// newDepositHeldStore builds a store on the full migrated schema, which the
// certificate write path needs: the carry-forward reads pool_registration,
// pool_retirement, certs, "transaction" and epoch.
func newDepositHeldStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// depositHeldEpochs records epoch rows covering [0, count*length) so the write
// path can place a slot in an epoch and decide whether a pending retirement has
// already been reaped.
func depositHeldEpochs(t *testing.T, store *Store, count uint64) {
	t.Helper()
	for epoch := range count {
		require.NoError(t, store.SetEpoch(
			epoch*depositHeldEpochLength,
			epoch,
			nil, nil, nil, nil,
			6,
			1,
			depositHeldEpochLength,
			nil,
		))
	}
}

func depositHeldPoolKey(seed byte) lcommon.PoolKeyHash {
	return lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{seed}, 28)),
	)
}

func depositHeldRegistration(
	pool lcommon.PoolKeyHash,
) *lcommon.PoolRegistrationCertificate {
	return &lcommon.PoolRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypePoolRegistration),
		Operator: pool,
		VrfKeyHash: lcommon.VrfKeyHash(
			lcommon.NewBlake2b256(bytes.Repeat([]byte{0x5b}, 32)),
		),
		Pledge: 1_000_000,
		Cost:   340_000_000,
		Margin: gcbor.Rat{Rat: big.NewRat(1, 100)},
		RewardAccount: lcommon.AddrKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{0x5c}, 28)),
		),
		PoolOwners: []lcommon.AddrKeyHash{
			lcommon.AddrKeyHash(
				lcommon.NewBlake2b224(bytes.Repeat([]byte{0x5d}, 28)),
			),
		},
	}
}

func depositHeldRetirement(
	pool lcommon.PoolKeyHash,
	epoch uint64,
) *lcommon.PoolRetirementCertificate {
	return &lcommon.PoolRetirementCertificate{
		CertType:    uint(lcommon.CertificateTypePoolRetirement),
		PoolKeyHash: pool,
		Epoch:       epoch,
	}
}

// writeDepositHeldCert applies one certificate through the production
// transaction write path at the given chain position.
func writeDepositHeldCert(
	t *testing.T,
	store *Store,
	slot uint64,
	blockIndex uint32,
	cert lcommon.Certificate,
	deposit uint64,
) {
	t.Helper()
	hash := make([]byte, 32)
	hash[0] = byte(slot)
	hash[1] = byte(slot >> 8)
	hash[2] = byte(blockIndex)
	tx := mockledger.NewTransactionBuilder().WithCertificates(cert)
	tx.WithId(hash)
	tx.WithValid(true)
	require.NoError(t, store.SetTransaction(
		tx,
		ocommon.Point{Slot: slot, Hash: hash},
		blockIndex,
		map[int]uint64{0: deposit},
		false,
		nil,
	))
}

// depositHeldColumn reads the persisted held amount for one registration,
// asserting the column itself rather than only the refund derived from it.
func depositHeldColumn(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	slot uint64,
) string {
	t.Helper()
	var held string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT deposit_held FROM pool_registration
WHERE pool_key_hash = ? AND added_slot = ?`,
		pool.Bytes(),
		slot,
	).Scan(&held))
	return held
}

func depositHeldRefund(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	epoch uint64,
	boundarySlot uint64,
) uint64 {
	t.Helper()
	refunds, err := store.GetPoolsRetiringAtEpoch(epoch, boundarySlot, nil)
	require.NoError(t, err)
	for _, refund := range refunds {
		if bytes.Equal(refund.PoolKeyHash, pool.Bytes()) {
			return uint64(refund.DepositHeld)
		}
	}
	t.Fatalf(
		"no retirement refund for pool %x at epoch %d",
		pool.Bytes(),
		epoch,
	)
	return 0
}

// A poolDeposit increase between a pool's first registration and a
// re-registration must not increase the refund: the re-registration charges no
// new deposit, so the pool still holds the original, smaller amount.
func TestPoolDepositHeldIncreasedParameterRefundsOriginal(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pool := depositHeldPoolKey(0xa1)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	writeDepositHeldCert(t, store, 1_100, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 1_200, 0, depositHeldRetirement(pool, 3), 0)

	require.Equal(t, "500", depositHeldColumn(t, store, pool, 100))
	require.Equal(t, "500", depositHeldColumn(t, store, pool, 1_100))
	require.Equal(
		t,
		uint64(500),
		depositHeldRefund(t, store, pool, 3, 3_000),
	)
}

// The mirror case: a poolDeposit decrease must not shrink the refund below what
// the pool actually paid.
func TestPoolDepositHeldDecreasedParameterRefundsOriginal(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pool := depositHeldPoolKey(0xa2)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 1_100, 0, depositHeldRegistration(pool), 200)
	writeDepositHeldCert(t, store, 1_200, 0, depositHeldRetirement(pool, 3), 0)

	require.Equal(t, "800", depositHeldColumn(t, store, pool, 1_100))
	require.Equal(
		t,
		uint64(800),
		depositHeldRefund(t, store, pool, 3, 3_000),
	)
}

// Repeated registrations carry the first registration's amount forward through
// every later one, including a re-registration that cancels a pending
// retirement. Each registration is at its own slot because two registrations of
// one pool cannot both persist in one block: the insert resolves the unique
// (pool_id, added_slot) key first-write-wins, so the block_index tie-break
// between pool certificates is exercised by
// TestPoolDepositHeldSameBlockRetirementsOrderByBlockIndex instead.
func TestPoolDepositHeldRepeatedRegistrationsCarryForward(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 6)
	pool := depositHeldPoolKey(0xa3)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	writeDepositHeldCert(t, store, 1_100, 0, depositHeldRegistration(pool), 700)
	writeDepositHeldCert(t, store, 2_100, 0, depositHeldRegistration(pool), 900)
	// A retirement naming a future epoch, cancelled by the next registration.
	writeDepositHeldCert(t, store, 2_200, 0, depositHeldRetirement(pool, 5), 0)
	writeDepositHeldCert(
		t,
		store,
		2_300,
		0,
		depositHeldRegistration(pool),
		1_100,
	)
	writeDepositHeldCert(t, store, 2_400, 0, depositHeldRetirement(pool, 5), 0)

	for _, slot := range []uint64{100, 1_100, 2_100, 2_300} {
		require.Equal(
			t,
			"500",
			depositHeldColumn(t, store, pool, slot),
			"registration at slot %d must hold the first deposit",
			slot,
		)
	}
	require.Equal(
		t,
		uint64(500),
		depositHeldRefund(t, store, pool, 5, 5_000),
	)
}

// Carry-forward stops at a reap. Once the retirement epoch has been crossed the
// earlier deposit has already been refunded, so a later registration is a first
// registration again and holds the amount in force at its own slot.
func TestPoolDepositHeldReRegistrationAfterReapChargesAgain(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pool := depositHeldPoolKey(0xa4)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	// Retires at the boundary into epoch 1, i.e. slot 1000.
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(pool, 1), 0)
	// Registers again inside epoch 1, after the reap.
	writeDepositHeldCert(t, store, 1_500, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 1_600, 0, depositHeldRetirement(pool, 3), 0)

	require.Equal(t, "500", depositHeldColumn(t, store, pool, 100))
	require.Equal(t, "800", depositHeldColumn(t, store, pool, 1_500))
	require.Equal(
		t,
		uint64(800),
		depositHeldRefund(t, store, pool, 3, 3_000),
	)
}

// A registration while the retirement is still pending cancels it and keeps the
// earlier deposit, even though the retirement certificate is the pool's most
// recent event before it.
func TestPoolDepositHeldPendingRetirementKeepsDeposit(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 6)
	pool := depositHeldPoolKey(0xa5)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(pool, 3), 0)
	// Still epoch 0: the reap has not happened, so no deposit is charged.
	writeDepositHeldCert(t, store, 300, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 400, 0, depositHeldRetirement(pool, 4), 0)

	require.Equal(t, "500", depositHeldColumn(t, store, pool, 300))
	require.Equal(
		t,
		uint64(500),
		depositHeldRefund(t, store, pool, 4, 4_000),
	)
}

// Rollback and replay: deleting the certificates after a slot drops the
// re-registration, and replaying it re-derives the same held amount from the
// rows that survived, so the refund after a rollback matches the refund before
// it.
func TestPoolDepositHeldSurvivesRollbackAndReplay(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pool := depositHeldPoolKey(0xa6)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	writeDepositHeldCert(t, store, 1_100, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 1_200, 0, depositHeldRetirement(pool, 3), 0)
	require.Equal(
		t,
		uint64(500),
		depositHeldRefund(t, store, pool, 3, 3_000),
	)

	require.NoError(t, store.DeleteCertificatesAfterSlot(1_000, nil))
	var remaining int
	require.NoError(t, store.writeDB.QueryRow(`
SELECT COUNT(*) FROM pool_registration WHERE pool_key_hash = ?`,
		pool.Bytes(),
	).Scan(&remaining))
	require.Equal(t, 1, remaining, "rollback dropped the re-registration")
	require.Equal(t, "500", depositHeldColumn(t, store, pool, 100))

	// Replay the rolled-away certificates.
	writeDepositHeldCert(t, store, 1_100, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 1_200, 0, depositHeldRetirement(pool, 3), 0)

	require.Equal(t, "500", depositHeldColumn(t, store, pool, 1_100))
	require.Equal(
		t,
		uint64(500),
		depositHeldRefund(t, store, pool, 3, 3_000),
	)
}

// A registration row written before the deposit-held column existed -- a
// migrated database whose backfill has not run, or an external writer -- still
// yields the pre-change refund instead of collapsing to zero, both when it is
// the registration being refunded and when a later re-registration carries its
// amount forward.
func TestPoolDepositHeldFallsBackToRecordedDeposit(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pool := depositHeldPoolKey(0xa7)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	_, err := store.writeDB.Exec(`
UPDATE pool_registration SET deposit_held = NULL WHERE pool_key_hash = ?`,
		pool.Bytes(),
	)
	require.NoError(t, err)

	writeDepositHeldCert(t, store, 1_100, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 1_200, 0, depositHeldRetirement(pool, 3), 0)

	require.Equal(t, "500", depositHeldColumn(t, store, pool, 1_100))
	require.Equal(
		t,
		uint64(500),
		depositHeldRefund(t, store, pool, 3, 3_000),
	)
}

// Without epoch rows the write path cannot place the reap, and falls back to
// charging the registration -- the amount the pre-change refund would have
// used, so a database with no epoch bookkeeping cannot diverge from a node that
// never had this column.
func TestPoolDepositHeldWithoutEpochDataChargesRegistration(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	pool := depositHeldPoolKey(0xa8)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(pool, 1), 0)
	writeDepositHeldCert(t, store, 1_500, 0, depositHeldRegistration(pool), 800)

	require.Equal(t, "800", depositHeldColumn(t, store, pool, 1_500))
}

// A Mithril reconcile or bootstrap import writes a pool with a synthesized
// registration and a synthetic retirement tombstone (certificate_id = 0) at the
// same slot. Neither row has a certs join, so their positions tie exactly; the
// tombstone has to win that tie for a later certificate registration to be
// recognized as a first registration and charge a deposit rather than inherit
// the import's zero.
func TestPoolDepositHeldSyntheticRetirementChargesReRegistration(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pool := depositHeldPoolKey(0xa9)

	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: pool.Bytes()},
		&models.PoolRegistration{
			PoolKeyHash: pool.Bytes(),
			AddedSlot:   100,
		},
		nil,
	))
	require.NoError(t, store.RetirePools(nil, [][]byte{pool.Bytes()}, 0, 100))

	writeDepositHeldCert(t, store, 1_500, 0, depositHeldRegistration(pool), 800)
	writeDepositHeldCert(t, store, 1_600, 0, depositHeldRetirement(pool, 3), 0)

	require.Equal(t, "800", depositHeldColumn(t, store, pool, 1_500))
	require.Equal(
		t,
		uint64(800),
		depositHeldRefund(t, store, pool, 3, 3_000),
	)
}

// pool_retirement has no unique (pool_id, added_slot) key, so two retirement
// certificates for one pool can share a block and only block_index orders them:
// the later transaction's certificate is the effective retirement. That
// tie-break is load-bearing for the held amount here, because
// the superseded certificate names an epoch the chain has already reached, so
// reading it instead would place the re-registration after a reap and charge a
// new deposit.
func TestPoolDepositHeldSameBlockRetirementsOrderByBlockIndex(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 6)
	pool := depositHeldPoolKey(0xaa)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	// Both retirements sit in the block at slot 200, in different
	// transactions, and the second one supersedes the first.
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(pool, 1), 0)
	writeDepositHeldCert(t, store, 200, 1, depositHeldRetirement(pool, 4), 0)
	// Slot 2_500 is in epoch 2: the effective retirement at epoch 4 is still
	// pending, so this re-registration cancels it and keeps the earlier
	// deposit. The superseded epoch-1 retirement was reaped long ago, so
	// losing the tie-break would charge 900 here.
	writeDepositHeldCert(t, store, 2_500, 0, depositHeldRegistration(pool), 900)
	writeDepositHeldCert(t, store, 2_600, 0, depositHeldRetirement(pool, 5), 0)

	require.Equal(t, "500", depositHeldColumn(t, store, pool, 2_500))

	// The refund query resolves the same tie: the pool never retires at epoch
	// 1, and its epoch-5 retirement refunds the held amount.
	refunds, err := store.GetPoolsRetiringAtEpoch(1, 1_000, nil)
	require.NoError(t, err)
	for _, refund := range refunds {
		require.NotEqual(t, pool.Bytes(), refund.PoolKeyHash)
	}
	require.Equal(
		t,
		uint64(500),
		depositHeldRefund(t, store, pool, 5, 5_000),
	)
}
