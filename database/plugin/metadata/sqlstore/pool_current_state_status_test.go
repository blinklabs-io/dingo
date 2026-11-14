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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// poolStatusOracle is what LedgerView.PoolCurrentState computed before it read
// pool_current_state: GetPool(includeInactive=false) for the active check, the
// history-ranked epoch query for the registration, and the (AddedSlot,
// CertificateID) comparison of the newest retirement against the newest
// registration for the pending retirement. It is kept as the oracle for
// GetPoolStatus only.
type poolStatusOracle struct {
	exists       bool
	active       bool
	regID        uint
	pendingEpoch *uint64
	// chainOrder is false when the oracle's comparison of the newest
	// registration and retirement does not follow chain position: they share
	// an added slot and a synthetic retirement or a certificate id out of
	// position order decides it.
	chainOrder bool
	// leios is true when the newest registration or retirement carries a
	// Leios execution context, which the oracle's added-slot order ignores.
	leios bool
}

func (w *poolCurrentWorkload) position(certID uint) (block, cert int64) {
	w.t.Helper()
	err := w.store.writeDB.QueryRow(w.store.dialect.Rebind(`
SELECT COALESCE(tx.block_index, 0), COALESCE(c.cert_index, 0)
FROM certs c LEFT JOIN applied_transaction tx ON tx.tx_id = c.transaction_id
WHERE c.id = ?`), certID).Scan(&block, &cert)
	if err != nil {
		return 0, 0
	}
	return block, cert
}

func (w *poolCurrentWorkload) hasLeiosContext(certID uint) bool {
	w.t.Helper()
	var n int
	require.NoError(w.t, w.store.writeDB.QueryRow(w.store.dialect.Rebind(`
SELECT COUNT(*) FROM leios_transaction_context lc
JOIN certs c ON c.transaction_id = lc.transaction_id
WHERE c.id = ?`), certID).Scan(&n))
	return n > 0
}

func (w *poolCurrentWorkload) oracle(
	pkh lcommon.PoolKeyHash,
	epochStart, committeeEpoch uint64,
) poolStatusOracle {
	w.t.Helper()
	var ret poolStatusOracle
	stored, err := w.store.GetPool(pkh, true, nil)
	require.NoError(w.t, err)
	ret.exists = stored != nil
	pool, err := w.store.GetPool(pkh, false, nil)
	require.NoError(w.t, err)
	ret.chainOrder = true
	if pool == nil {
		return ret
	}
	ret.active = true
	regs, err := w.store.getPoolRegistrationsEffectiveForEpochFromHistory(
		[]lcommon.PoolKeyHash{pkh}, epochStart, committeeEpoch,
		math.MaxInt64, nil,
	)
	require.NoError(w.t, err)
	if len(regs) > 0 {
		ret.regID = regs[0].ID
	}
	reg := pool.Registration[0]
	if w.hasLeiosContext(reg.CertificateID) {
		ret.leios = true
	}
	if len(pool.Retirement) == 0 {
		return ret
	}
	r := pool.Retirement[0]
	if w.hasLeiosContext(r.CertificateID) {
		ret.leios = true
	}
	if reg.AddedSlot == r.AddedSlot {
		regBlock, regCert := w.position(reg.CertificateID)
		retBlock, retCert := w.position(r.CertificateID)
		positionRegWins := regBlock > retBlock ||
			(regBlock == retBlock && regCert > retCert)
		idRegWins := reg.CertificateID > r.CertificateID
		if r.CertificateID == 0 || reg.CertificateID == 0 ||
			positionRegWins != idRegWins {
			ret.chainOrder = false
		}
	}
	registrationSupersedes := reg.AddedSlot > r.AddedSlot ||
		(reg.AddedSlot == r.AddedSlot &&
			reg.CertificateID > r.CertificateID)
	if !registrationSupersedes {
		epoch := r.Epoch
		ret.pendingEpoch = &epoch
	}
	return ret
}

// checkStatus compares GetPoolStatus and the registration it leads to with the
// oracle for every pool, and the at-slot query with the history ranking. It
// returns how many pools the oracle comparison covered.
func (w *poolCurrentWorkload) checkStatus(step int, op string) (compared int) {
	w.t.Helper()
	slots := []uint64{0, 1, w.slot, w.slot + 5000, w.rng.Uint64N(w.slot + 400)}
	if len(w.placed) > 0 {
		key := w.placed[w.rng.IntN(len(w.placed))]
		slots = append(slots, key[0], key[0]+1)
	}
	for _, slot := range slots {
		fast, err := w.store.GetPoolRegistrationsAtSlot(w.pools, slot, nil)
		require.NoError(w.t, err)
		history, err := w.store.getPoolRegistrationsAtSlotFromHistory(
			w.pools, slot, nil,
		)
		require.NoError(w.t, err)
		require.Equal(
			w.t, registrationIDs(history), registrationIDs(fast),
			"step %d (%s): registrations at slot %d", step, op, slot,
		)
	}
	epochStart := w.slot + 1
	committeeEpoch := epochStart / 1000
	for _, pkh := range w.pools {
		want := w.oracle(pkh, epochStart, committeeEpoch)
		status, err := w.store.GetPoolStatus(pkh, nil)
		require.NoError(w.t, err)
		require.Equal(
			w.t, want.exists, status.Exists,
			"step %d (%s): exists", step, op,
		)
		if !want.chainOrder || want.leios {
			continue
		}
		compared++
		require.Equal(
			w.t, want.active, status.Active,
			"step %d (%s): active", step, op,
		)
		require.Equal(
			w.t, want.pendingEpoch, status.RetirementEpoch,
			"step %d (%s): pending retirement", step, op,
		)
	}
	return compared
}

func registrationIDs(regs []models.PoolRegistration) map[string]uint {
	out := map[string]uint{}
	for _, reg := range regs {
		out[string(reg.PoolKeyHash)] = reg.ID
	}
	return out
}

func (w *poolCurrentWorkload) status(pkh lcommon.PoolKeyHash) models.PoolStatus {
	w.t.Helper()
	status, err := w.store.GetPoolStatus(pkh, nil)
	require.NoError(w.t, err)
	return status
}

func (w *poolCurrentWorkload) applyCerts(
	slot uint64,
	blockIndex uint32,
	certs ...lcommon.Certificate,
) {
	w.t.Helper()
	hash := poolCurrentTxHash(slot, blockIndex)
	tx := mockledger.NewTransactionBuilder().WithCertificates(certs...)
	tx.WithId(hash)
	tx.WithValid(true)
	deposits := map[int]uint64{}
	for i := range certs {
		deposits[i] = 500
	}
	require.NoError(w.t, w.store.SetTransaction(
		tx, ocommon.Point{Slot: slot, Hash: hash}, blockIndex, deposits,
		false, nil, 0,
	))
}

// The pending retirement is the newest retirement certificate in chain order.
// A later retirement replaces an earlier one whichever epoch it names, in a
// later block and within one block, and a later registration cancels it, as
// the POOL rule's insert into and delete from psRetiring do.
func TestPoolStatusPendingRetirementFollowsChainOrder(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xb1)
	pool := w.pools[0]
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	require.Equal(t, models.PoolStatus{Exists: true, Active: true}, w.status(pool))

	// Different blocks: epoch 10 then epoch 5 reports 5, then epoch 8 reports 8.
	w.apply(2_000, 0, depositHeldRetirement(pool, 10), nil)
	require.Equal(t, new(uint64(10)), w.status(pool).RetirementEpoch)
	w.apply(2_100, 0, depositHeldRetirement(pool, 5), nil)
	require.Equal(t, new(uint64(5)), w.status(pool).RetirementEpoch)
	w.apply(2_200, 0, depositHeldRetirement(pool, 8), nil)
	require.Equal(t, new(uint64(8)), w.status(pool).RetirementEpoch)

	// One block, two transactions: the later one wins.
	w.apply(3_000, 0, depositHeldRetirement(pool, 9), nil)
	w.apply(3_000, 1, depositHeldRetirement(pool, 4), nil)
	require.Equal(t, new(uint64(4)), w.status(pool).RetirementEpoch)
	w.apply(3_100, 0, depositHeldRetirement(pool, 4), nil)
	w.apply(3_100, 1, depositHeldRetirement(pool, 9), nil)
	require.Equal(t, new(uint64(9)), w.status(pool).RetirementEpoch)

	// One transaction, two certificates: the later one wins.
	w.applyCerts(3_200, 0,
		depositHeldRetirement(pool, 12), depositHeldRetirement(pool, 6))
	require.Equal(t, new(uint64(6)), w.status(pool).RetirementEpoch)
	w.applyCerts(3_300, 0,
		depositHeldRetirement(pool, 6), depositHeldRetirement(pool, 12))
	require.Equal(t, new(uint64(12)), w.status(pool).RetirementEpoch)

	// A registration after it cancels the retirement; one before it does not.
	w.applyCerts(3_400, 0,
		depositHeldRetirement(pool, 7), depositHeldRegistration(pool))
	require.Nil(t, w.status(pool).RetirementEpoch)
	w.applyCerts(3_500, 0,
		depositHeldRegistration(pool), depositHeldRetirement(pool, 7))
	require.Equal(t, new(uint64(7)), w.status(pool).RetirementEpoch)
	w.slot = 3_500
	w.check(0, "chain order")
	w.checkStatus(0, "chain order")
}

// A retirement whose epoch has been reached retires the pool unless a later
// registration follows it, and a pool with no registration is not active.
func TestPoolStatusRetiredAndReRegistered(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xb2)
	pool := w.pools[0]
	require.Equal(t, models.PoolStatus{}, w.status(pool))
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	w.apply(1_500, 0, depositHeldRetirement(pool, 3), nil)
	require.Equal(t, new(uint64(3)), w.status(pool).RetirementEpoch)
	require.NoError(t, w.store.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 3_100, Hash: make([]byte, 32)},
	}, nil))
	require.Equal(t, models.PoolStatus{Exists: true}, w.status(pool))
	w.apply(3_500, 0, depositHeldRegistration(pool), nil)
	require.NoError(t, w.store.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 3_600, Hash: make([]byte, 32)},
	}, nil))
	require.Equal(t, models.PoolStatus{Exists: true, Active: true}, w.status(pool))
}

// A synthetic retirement outranks a registration it shares a slot with, for
// the active check and the pending retirement alike.
func TestPoolStatusSyntheticRetirementWinsSlotTie(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xb3)
	pool := w.pools[0]
	w.apply(1_000, 2, depositHeldRegistration(pool), nil)
	require.NoError(t, w.store.RetirePools(
		nil, [][]byte{pool.Bytes()}, 7, 1_000,
	))
	require.Equal(t, new(uint64(7)), w.status(pool).RetirementEpoch)
	w.apply(1_001, 0, depositHeldRegistration(pool), nil)
	require.Nil(t, w.status(pool).RetirementEpoch)
}

// A pool whose newest certificate is older than the horizon keeps its status
// and answers from its row at every later epoch.
func TestPoolStatusOfPoolOlderThanHorizon(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xb4)
	pool := w.pools[0]
	w.apply(500, 0, depositHeldRegistration(pool), nil)
	w.apply(900, 0, depositHeldRetirement(pool, 40), nil)
	for epoch := uint64(12); epoch < 30; epoch++ {
		require.NoError(t, w.store.SetEpoch(
			epoch*depositHeldEpochLength, epoch,
			nil, nil, nil, nil, 6, 1, depositHeldEpochLength, nil,
		))
	}
	w.slot = 29_000
	require.Equal(t, new(uint64(40)), w.status(pool).RetirementEpoch)
	fast0 := w.store.poolCurrentFastPools.Load()
	history0 := w.store.poolCurrentHistoryPools.Load()
	regs, err := w.store.GetPoolRegistrationsEffectiveForEpoch(
		w.pools, 29_000, 28, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, int64(1), w.store.poolCurrentFastPools.Load()-fast0)
	require.Zero(t, w.store.poolCurrentHistoryPools.Load()-history0)
	w.checkStatus(0, "older than the horizon")
}

// GetPoolStatus and the at-slot lookup read the stored rows, not the history
// they derive from: with the retirement and certificate history removed under
// them by raw SQL, they still answer from the rows.
func TestPoolStatusAndAtSlotReadNoHistory(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xb5)
	pool := w.pools[0]
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	w.apply(2_000, 0, depositHeldRetirement(pool, 9), nil)
	for _, query := range []string{
		"DELETE FROM pool_retirement",
		"DELETE FROM applied_transaction",
		"DELETE FROM certs",
	} {
		_, err := w.store.writeDB.Exec(query)
		require.NoError(t, err)
	}
	require.Equal(t, new(uint64(9)), w.status(pool).RetirementEpoch)
	regs, err := w.store.GetPoolRegistrationsAtSlot(w.pools, 5_000, nil)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, uint64(1_000), regs[0].AddedSlot)

	rows, err := w.store.writeDB.Query(
		"EXPLAIN QUERY PLAN "+poolStatusSQL, pool.Bytes(),
	)
	require.NoError(t, err)
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan += detail + "\n"
	}
	require.NoError(t, rows.Err())
	for _, table := range []string{
		"pool_registration", "pool_retirement", "certs",
		"applied_transaction", "leios_transaction_context",
	} {
		require.NotContains(t, plan, table)
	}
	require.NotContains(t, plan, "SCAN")
}
