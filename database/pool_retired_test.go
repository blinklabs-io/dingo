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
	"bytes"
	"database/sql"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// retiredPoolFixture seeds one pool with an explicit certificate history so
// GetPoolKeyHashesRetiredByEpoch's ordering rules can be exercised directly.
// Every registration and retirement gets a real transaction/certs row, so
// added_slot ties are broken by block_index then cert_index exactly the way
// a same-block registration and retirement would be on chain.
type retiredPoolFixture struct {
	keyHash []byte
	regs    []retiredPoolCert
	rets    []retiredPoolCert
}

type retiredPoolCert struct {
	slot       uint64
	blockIndex uint64
	certIndex  uint64
	epoch      uint64 // retirements only
	// synthetic writes the row the way Store.RetirePools does, with
	// certificate_id = 0 and no certs/transaction row behind it. That is how
	// a ledger-state snapshot import and a reconcile tombstone a pool, so
	// such a row has no block_index/cert_index of its own to order by.
	synthetic bool // retirements only
}

// retiredPoolCertSeq keeps transaction hashes unique across a single test:
// "transaction".hash carries a unique index, so two certificates seeded at
// the same slot need distinct hashes.
type retiredPoolSeeder struct {
	raw *sql.DB
	seq uint64
}

func (s *retiredPoolSeeder) insertCert(
	t *testing.T,
	slot, blockIndex, certIndex uint64,
) int64 {
	t.Helper()
	s.seq++
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash, s.seq)
	res, err := s.raw.Exec(
		`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, ?, ?)`,
		hash, slot, blockIndex,
	)
	require.NoError(t, err)
	txID, err := res.LastInsertId()
	require.NoError(t, err)
	res, err = s.raw.Exec(
		`INSERT INTO certs (transaction_id, slot, cert_index) VALUES (?, ?, ?)`,
		txID, slot, certIndex,
	)
	require.NoError(t, err)
	certID, err := res.LastInsertId()
	require.NoError(t, err)
	return certID
}

func (s *retiredPoolSeeder) seed(t *testing.T, f retiredPoolFixture) {
	t.Helper()
	res, err := s.raw.Exec(
		`INSERT INTO pool (pool_key_hash) VALUES (?)`,
		f.keyHash,
	)
	require.NoError(t, err)
	poolID, err := res.LastInsertId()
	require.NoError(t, err)
	for _, reg := range f.regs {
		certID := s.insertCert(t, reg.slot, reg.blockIndex, reg.certIndex)
		_, err := s.raw.Exec(`
INSERT INTO pool_registration (
    pool_id, pool_key_hash, certificate_id, added_slot, deposit_amount
) VALUES (?, ?, ?, ?, '500')`,
			poolID, f.keyHash, certID, reg.slot,
		)
		require.NoError(t, err)
	}
	for _, ret := range f.rets {
		var certID int64
		if !ret.synthetic {
			certID = s.insertCert(t, ret.slot, ret.blockIndex, ret.certIndex)
		}
		_, err := s.raw.Exec(`
INSERT INTO pool_retirement (
    pool_id, pool_key_hash, certificate_id, epoch, added_slot
) VALUES (?, ?, ?, ?, ?)`,
			poolID, f.keyHash, certID, ret.epoch, ret.slot,
		)
		require.NoError(t, err)
	}
}

func retiredPoolKeyHash(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, 28)
}

// TestGetPoolKeyHashesRetiredByEpoch pins the predicate the Koios parity
// checker needs: the pool's latest certificate as of the boundary slot is a
// retirement effective at or *before* the queried epoch.
//
// The two halves that make it different from GetPoolsRetiringAtEpoch are
// covered explicitly. "At or before" (poolRetiredEarlier) is what lets a
// trailing observer classify a pool that left several epochs ago, which
// GetPoolsRetiringAtEpoch's `ret.epoch = ?` would miss. The cancellation
// rules (poolReregistered, poolReregisteredSameSlot) are what stop "a
// retirement certificate exists" from being mistaken for the predicate: a
// later registration puts the pool back, and on this chain seven pools have
// a registration filed after a retirement certificate.
func TestGetPoolKeyHashesRetiredByEpoch(t *testing.T) {
	const (
		queryEpoch   = uint64(7)
		boundarySlot = uint64(1_000)
	)
	var (
		poolRetiredEarlier         = retiredPoolKeyHash(0xA1)
		poolRetiredAtQueryEpoch    = retiredPoolKeyHash(0xA2)
		poolRetiringLater          = retiredPoolKeyHash(0xA3)
		poolReregistered           = retiredPoolKeyHash(0xA4)
		poolReregisteredSameSlot   = retiredPoolKeyHash(0xA5)
		poolRetiredSameSlotAfter   = retiredPoolKeyHash(0xA6)
		poolRetiredAfterBoundary   = retiredPoolKeyHash(0xA7)
		poolNeverRetired           = retiredPoolKeyHash(0xA8)
		poolRetiredThenReregRetire = retiredPoolKeyHash(0xA9)
		poolSyntheticSameSlotAsReg = retiredPoolKeyHash(0xAA)
		poolSyntheticOverCertRet   = retiredPoolKeyHash(0xAB)
	)

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	seeder := &retiredPoolSeeder{raw: rawSQLiteMetadataFixture(t, db)}

	for _, f := range []retiredPoolFixture{
		// Retired effective epoch 5, still departed at epoch 7 — the
		// observed dingo #3925 case, where the pool retired at 243 was
		// still misclassified at param epochs 244 and 245.
		{
			keyHash: poolRetiredEarlier,
			regs:    []retiredPoolCert{{slot: 100}},
			rets:    []retiredPoolCert{{slot: 200, epoch: 5}},
		},
		// Retirement effective exactly at the queried epoch is inclusive.
		{
			keyHash: poolRetiredAtQueryEpoch,
			regs:    []retiredPoolCert{{slot: 100}},
			rets:    []retiredPoolCert{{slot: 200, epoch: queryEpoch}},
		},
		// Certificate filed, but the retirement has not taken effect yet.
		{
			keyHash: poolRetiringLater,
			regs:    []retiredPoolCert{{slot: 100}},
			rets:    []retiredPoolCert{{slot: 200, epoch: 9}},
		},
		// A later registration cancels the pending retirement.
		{
			keyHash: poolReregistered,
			regs:    []retiredPoolCert{{slot: 100}, {slot: 300}},
			rets:    []retiredPoolCert{{slot: 200, epoch: 5}},
		},
		// Same slot, registration ordered after the retirement by
		// cert_index — still a cancellation.
		{
			keyHash: poolReregisteredSameSlot,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, certIndex: 2},
			},
			rets: []retiredPoolCert{{slot: 200, certIndex: 1, epoch: 5}},
		},
		// Same slot, retirement ordered after the registration — the
		// retirement stands.
		{
			keyHash: poolRetiredSameSlotAfter,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, certIndex: 1},
			},
			rets: []retiredPoolCert{{slot: 200, certIndex: 2, epoch: 5}},
		},
		// The retirement certificate is not yet visible at the boundary.
		{
			keyHash: poolRetiredAfterBoundary,
			regs:    []retiredPoolCert{{slot: 100}},
			rets:    []retiredPoolCert{{slot: boundarySlot, epoch: 5}},
		},
		{
			keyHash: poolNeverRetired,
			regs:    []retiredPoolCert{{slot: 100}},
		},
		// Retired, re-registered, then filed a fresh retirement that has
		// taken effect: the latest certificate is the second retirement.
		{
			keyHash: poolRetiredThenReregRetire,
			regs:    []retiredPoolCert{{slot: 100}, {slot: 300}},
			rets: []retiredPoolCert{
				{slot: 200, epoch: 5},
				{slot: 400, epoch: 6},
			},
		},
		// A reconcile retirement sharing its slot with a certificate-backed
		// registration. It carries no certs row, so its COALESCE'd
		// block_index/cert_index are both zero and it would lose the
		// same-slot tie-break to the registration's cert_index 1 — the
		// synthetic_ret guard is what stops that being read as a
		// cancellation. This is the shape ledgerstate's snapshot import
		// writes: ImportPool then RetirePools at one slot.
		{
			keyHash: poolSyntheticSameSlotAsReg,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, certIndex: 1},
			},
			rets: []retiredPoolCert{{slot: 200, epoch: 5, synthetic: true}},
		},
		// A reconcile retirement sharing its slot with a certificate-backed
		// retirement that has not taken effect yet. The synthetic row is the
		// ledger state's answer, so it must rank first despite its zero
		// indices; picking the cert row instead would report epoch 9 and
		// leave the pool active.
		{
			keyHash: poolSyntheticOverCertRet,
			regs:    []retiredPoolCert{{slot: 100}},
			rets: []retiredPoolCert{
				{slot: 200, certIndex: 3, epoch: 9},
				{slot: 200, epoch: 5, synthetic: true},
			},
		},
	} {
		seeder.seed(t, f)
	}

	txn := db.Transaction(false)
	defer txn.Release()
	got, err := db.Metadata().GetPoolKeyHashesRetiredByEpoch(
		queryEpoch,
		boundarySlot,
		txn.Metadata(),
	)
	require.NoError(t, err)

	retired := make(map[string]struct{}, len(got))
	for _, keyHash := range got {
		retired[string(keyHash)] = struct{}{}
	}
	for _, tc := range []struct {
		name    string
		keyHash []byte
		want    bool
	}{
		{"retired at an earlier epoch", poolRetiredEarlier, true},
		{"retired at the queried epoch", poolRetiredAtQueryEpoch, true},
		{"retirement takes effect later", poolRetiringLater, false},
		{"re-registered after retiring", poolReregistered, false},
		{
			"re-registered in the retirement's own slot",
			poolReregisteredSameSlot,
			false,
		},
		{
			"retired in the registration's own slot",
			poolRetiredSameSlotAfter,
			true,
		},
		{
			"retirement filed at or after the boundary",
			poolRetiredAfterBoundary,
			false,
		},
		{"never retired", poolNeverRetired, false},
		{
			"retired again after re-registering",
			poolRetiredThenReregRetire,
			true,
		},
		{
			"reconcile-retired in a registration's own slot",
			poolSyntheticSameSlotAsReg,
			true,
		},
		{
			"reconcile retirement outranks a same-slot certificate",
			poolSyntheticOverCertRet,
			true,
		},
	} {
		_, ok := retired[string(tc.keyHash)]
		require.Equalf(
			t,
			tc.want,
			ok,
			"pool %x (%s) departure at epoch %d",
			tc.keyHash,
			tc.name,
			queryEpoch,
		)
	}
	require.Len(t, retired, 6, "no other pool may be reported as retired")
}

// TestGetPoolsRetiringAtEpochSameSlotResolution pins how the POOLREAP
// deposit-refund query resolves a pool's latest retirement when certificates
// share an added_slot, which nothing in the tree covered before.
//
// Three keys decide it, in order. synthetic_ret ranks reconcile retirements
// (`certificate_id = 0`, written by Store.RetirePools from ledgerstate's
// snapshot import and reconcile paths) ahead of certificate-backed rows and
// exempts them from the cancellation clauses: such a row has no
// certs/transaction join, so its COALESCE'd block_index and cert_index are
// both zero — the lowest possible same-slot rank — and without the key it
// would lose every tie-break to a certificate-backed row, which is the
// opposite of what the ledger state it encodes says. block_index then orders
// by transaction within the block, and cert_index by certificate within the
// transaction.
//
// GetActivePoolKeyHashesAtSlot has ranked synthetic rows first since it was
// written; this query, GetPoolKeyHashesRetiredByEpoch and DingoDB's copy now
// agree with it, so the active pool set, the POOLREAP refund and both Koios
// parity routes cannot resolve the same pool's latest retirement differently.
//
// poolSyntheticAtBoundry pins the other half: a reconcile row still cannot
// drive a refund it should not, because RetirePools stamps the catch-up tip as
// epoch/added_slot and the `added_slot < boundarySlot` cut excludes it. That
// exclusion is now the only thing keeping reconcile rows out of boundary
// refund processing, so it is asserted rather than assumed.
func TestGetPoolsRetiringAtEpochSameSlotResolution(t *testing.T) {
	const boundarySlot = uint64(1_000)
	var (
		poolSyntheticSameSlot  = retiredPoolKeyHash(0xB1)
		poolSyntheticOverCert  = retiredPoolKeyHash(0xB2)
		poolCertBackedOnly     = retiredPoolKeyHash(0xB3)
		poolSyntheticAtBoundry = retiredPoolKeyHash(0xB4)
		poolRetiredLaterTx     = retiredPoolKeyHash(0xB5)
		poolReregisteredTx     = retiredPoolKeyHash(0xB6)
		poolRetiredLaterCert   = retiredPoolKeyHash(0xB7)
		poolReregisteredCert   = retiredPoolKeyHash(0xB8)
		poolRetiredTwiceInSlot = retiredPoolKeyHash(0xB9)
	)

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	seeder := &retiredPoolSeeder{raw: rawSQLiteMetadataFixture(t, db)}

	for _, f := range []retiredPoolFixture{
		// The import shape: a registration and a reconcile retirement in one
		// slot, with the registration certificate-backed at cert_index 1.
		{
			keyHash: poolSyntheticSameSlot,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, certIndex: 1},
			},
			rets: []retiredPoolCert{{slot: 200, epoch: 5, synthetic: true}},
		},
		// A reconcile retirement and a certificate-backed retirement in one
		// slot, naming different effective epochs.
		{
			keyHash: poolSyntheticOverCert,
			regs:    []retiredPoolCert{{slot: 100}},
			rets: []retiredPoolCert{
				{slot: 200, certIndex: 3, epoch: 9},
				{slot: 200, epoch: 5, synthetic: true},
			},
		},
		// Control: ordinary certificate-backed retirement effective 5.
		{
			keyHash: poolCertBackedOnly,
			regs:    []retiredPoolCert{{slot: 100}},
			rets:    []retiredPoolCert{{slot: 200, certIndex: 1, epoch: 5}},
		},
		// A reconcile retirement filed at the boundary itself is not visible
		// to it, which is what keeps reconcile rows out of refunds.
		{
			keyHash: poolSyntheticAtBoundry,
			regs:    []retiredPoolCert{{slot: 100}},
			rets: []retiredPoolCert{
				{slot: boundarySlot, epoch: 5, synthetic: true},
			},
		},
		// Same slot, different transactions: the retirement is in the later
		// transaction, so it stands. cert_index cannot decide this pair —
		// both are 0 — so only the block_index comparison can.
		{
			keyHash: poolRetiredLaterTx,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, blockIndex: 1},
			},
			rets: []retiredPoolCert{{slot: 200, blockIndex: 2, epoch: 5}},
		},
		// The mirror image: the registration is in the later transaction, so
		// it cancels. These two pin both directions of block_index.
		{
			keyHash: poolReregisteredTx,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, blockIndex: 2},
			},
			rets: []retiredPoolCert{{slot: 200, blockIndex: 1, epoch: 5}},
		},
		// Same transaction, retirement at the later cert_index: it stands.
		{
			keyHash: poolRetiredLaterCert,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, certIndex: 1},
			},
			rets: []retiredPoolCert{{slot: 200, certIndex: 2, epoch: 5}},
		},
		// Same transaction, registration at the later cert_index: it cancels.
		{
			keyHash: poolReregisteredCert,
			regs: []retiredPoolCert{
				{slot: 100},
				{slot: 200, certIndex: 2},
			},
			rets: []retiredPoolCert{{slot: 200, certIndex: 1, epoch: 5}},
		},
		// Two retirement certificates in one transaction naming different
		// effective epochs. latest_ret must pick the higher cert_index, so
		// the pool reaps at 5 rather than 9. The epoch-9 row is seeded first
		// so insertion order disagrees with cert_index order, which is what
		// makes latest_ret's own ORDER BY key load-bearing rather than
		// incidentally satisfied by the scan order.
		{
			keyHash: poolRetiredTwiceInSlot,
			regs:    []retiredPoolCert{{slot: 100}},
			rets: []retiredPoolCert{
				{slot: 200, certIndex: 1, epoch: 9},
				{slot: 200, certIndex: 2, epoch: 5},
			},
		},
	} {
		seeder.seed(t, f)
	}

	for _, tc := range []struct {
		name  string
		epoch uint64
		want  [][]byte
	}{
		{
			name:  "the same-slot winners reap at their own epoch",
			epoch: 5,
			want: [][]byte{
				poolSyntheticSameSlot,
				poolSyntheticOverCert,
				poolCertBackedOnly,
				poolRetiredLaterTx,
				poolRetiredLaterCert,
				poolRetiredTwiceInSlot,
			},
		},
		{
			// Every retirement that lost its tie-break named epoch 9, so an
			// inverted ordering key would move pools into this set and out
			// of the one above.
			name:  "the outranked certificates' epoch reaps nothing",
			epoch: 9,
			want:  nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := db.Transaction(false)
			defer txn.Release()
			refunds, err := db.GetPoolsRetiringAtEpoch(
				tc.epoch,
				boundarySlot,
				txn,
			)
			require.NoError(t, err)
			got := make(map[string]struct{}, len(refunds))
			for _, refund := range refunds {
				got[string(refund.PoolKeyHash)] = struct{}{}
			}
			want := make(map[string]struct{}, len(tc.want))
			for _, keyHash := range tc.want {
				want[string(keyHash)] = struct{}{}
			}
			require.Equal(t, want, got)
		})
	}
}
