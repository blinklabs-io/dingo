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
		certID := s.insertCert(t, ret.slot, ret.blockIndex, ret.certIndex)
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
	require.Len(t, retired, 4, "no other pool may be reported as retired")
}
