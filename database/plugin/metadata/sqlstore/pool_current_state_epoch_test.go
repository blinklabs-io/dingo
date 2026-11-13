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
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// answer is what GetPoolRegistrationsEffectiveForEpoch returns for each pool,
// as the added slot of the selected registration, 0 for none. It also checks
// the result against the history ranking and returns how many pools the call
// answered from pool_current_state and how many it ranked from history.
func (w *poolCurrentWorkload) answer(
	start, ended, snapshot uint64,
) (slots map[string]uint64, fast, history int64) {
	w.t.Helper()
	fast0 := w.store.poolCurrentFastPools.Load()
	history0 := w.store.poolCurrentHistoryPools.Load()
	regs, err := w.store.GetPoolRegistrationsEffectiveForEpoch(
		w.pools, start, ended, snapshot, nil,
	)
	require.NoError(w.t, err)
	fast = w.store.poolCurrentFastPools.Load() - fast0
	history = w.store.poolCurrentHistoryPools.Load() - history0
	require.Equal(
		w.t,
		w.effective(true, start, ended, snapshot),
		w.effective(false, start, ended, snapshot),
	)
	slots = map[string]uint64{}
	for _, reg := range regs {
		slots[string(reg.PoolKeyHash)] = reg.AddedSlot
	}
	return slots, fast, history
}

// A pool whose newest events lie inside or after the requested epoch is
// answered from its row, which holds the registration in force at the epoch
// start next to the pending registration and retirement.
func TestPoolCurrentStateAnswersPendingEventsFromRow(t *testing.T) {
	t.Parallel()
	store := newPoolCurrentStore(t)
	depositHeldEpochs(t, store, 12)
	w := &poolCurrentWorkload{
		t:     t,
		store: store,
		used:  map[[2]uint64]struct{}{},
	}
	for i := range 6 {
		w.pools = append(w.pools, depositHeldPoolKey(byte(0xa0+i)))
	}
	reRegistered, retiring, cancelled, reRegisteredAfterRetire, burst, scheduled :=
		w.pools[0], w.pools[1], w.pools[2], w.pools[3], w.pools[4], w.pools[5]
	// Re-registration in the epoch being captured: the earlier one stays in
	// force until the boundary.
	w.apply(1_500, 0, depositHeldRegistration(reRegistered), nil)
	w.apply(4_500, 0, depositHeldRegistration(reRegistered), nil)
	// Retirement scheduled for a later epoch does not retire the pool yet.
	w.apply(1_500, 1, depositHeldRegistration(retiring), nil)
	w.apply(4_200, 0, depositHeldRetirement(retiring, 5), nil)
	// A re-registration after a retirement cancels it.
	w.apply(1_000, 0, depositHeldRegistration(cancelled), nil)
	w.apply(2_000, 0, depositHeldRetirement(cancelled, 3), nil)
	w.apply(2_500, 0, depositHeldRegistration(cancelled), nil)
	// A pool whose retirement is already in force registers again inside the
	// epoch: the new registration is the one captured.
	w.apply(1_000, 1, depositHeldRegistration(reRegisteredAfterRetire), nil)
	w.apply(1_500, 2, depositHeldRetirement(reRegisteredAfterRetire, 2), nil)
	w.apply(4_200, 1, depositHeldRegistration(reRegisteredAfterRetire), nil)
	// A retirement before the epoch start whose target epoch has not ended
	// leaves the pool registered.
	w.apply(1_000, 3, depositHeldRegistration(scheduled), nil)
	w.apply(2_000, 1, depositHeldRetirement(scheduled, 6), nil)
	// Several registrations inside the epoch: the one before it is in force.
	w.apply(1_000, 2, depositHeldRegistration(burst), nil)
	for i, slot := range []uint64{4_100, 4_300, 4_700} {
		w.apply(slot, uint32(i), depositHeldRegistration(burst), nil)
	}

	slots, fast, history := w.answer(4_000, 3, 4_999)
	require.Equal(t, int64(6), fast)
	require.Zero(t, history, "pools with pending events fell back to history")
	require.Equal(t, map[string]uint64{
		string(reRegistered.Bytes()):            1_500,
		string(retiring.Bytes()):                1_500,
		string(cancelled.Bytes()):               2_500,
		string(reRegisteredAfterRetire.Bytes()): 4_200,
		string(burst.Bytes()):                   1_000,
		string(scheduled.Bytes()):               1_000,
	}, slots)

	// A registration after the snapshot slot is not part of the capture.
	slots, _, _ = w.answer(4_000, 3, 4_100)
	require.NotContains(t, slots, string(reRegisteredAfterRetire.Bytes()))
	require.Equal(t, uint64(1_000), slots[string(burst.Bytes())])

	// Once the retirement's epoch has ended it takes effect.
	slots, fast, history = w.answer(5_000, 5, 5_999)
	require.Equal(t, int64(6), fast)
	require.Zero(t, history)
	require.NotContains(t, slots, string(retiring.Bytes()))
	require.Equal(t, uint64(1_000), slots[string(scheduled.Bytes())])
	slots, _, _ = w.answer(7_000, 6, 7_999)
	require.NotContains(t, slots, string(scheduled.Bytes()))
	require.Equal(t, uint64(4_500), slots[string(reRegistered.Bytes())])
	require.Equal(t, uint64(4_700), slots[string(burst.Bytes())])
}

// A pool's rows hold the events at or after the horizon and the newest one
// below it; an epoch that began before the horizon is ranked from history.
func TestPoolCurrentStateHorizonBoundsRowsAndFallsBack(t *testing.T) {
	t.Parallel()
	store := newPoolCurrentStore(t)
	depositHeldEpochs(t, store, 12)
	w := &poolCurrentWorkload{
		t:     t,
		store: store,
		used:  map[[2]uint64]struct{}{},
		pools: []lcommon.PoolKeyHash{depositHeldPoolKey(0xa8)},
	}
	for epoch := range uint64(12) {
		w.apply(epoch*1_000+500, 0, depositHeldRegistration(w.pools[0]), nil)
	}
	var stored int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM pool_current_event WHERE kind = 0",
	).Scan(&stored))
	// Epochs 3 to 11 are at or after the horizon (epoch 11 less eight), and
	// the registration of epoch 2 is the newest below it.
	require.Equal(t, 10, stored)
	var oldest, horizon uint64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT MIN(effective_slot), MAX(horizon_slot) FROM pool_current_event, pool_current_state",
	).Scan(&oldest, &horizon))
	require.Equal(t, uint64(2_500), oldest)
	require.Equal(t, uint64(3_000), horizon)

	slots, fast, history := w.answer(3_000, 2, math.MaxInt64)
	require.Equal(t, uint64(2_500), slots[string(w.pools[0].Bytes())])
	require.Equal(t, int64(1), fast)
	require.Zero(t, history)
	slots, fast, history = w.answer(11_000, 10, math.MaxInt64)
	require.Equal(t, uint64(10_500), slots[string(w.pools[0].Bytes())])
	require.Equal(t, int64(1), fast)
	require.Zero(t, history)

	slots, fast, history = w.answer(2_000, 1, math.MaxInt64)
	require.Equal(t, uint64(1_500), slots[string(w.pools[0].Bytes())])
	require.Zero(t, fast)
	require.Equal(t, int64(1), history)
}

// After the chain rolls back past a re-registration and past a retirement,
// each pool's rows and answers are those of a chain that never had them.
func TestPoolCurrentStateRollbackPastReRegistrationAndRetirement(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xa9)
	pool := w.pools[0]
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	w.apply(2_000, 0, depositHeldRetirement(pool, 3), nil)
	w.apply(3_000, 0, depositHeldRegistration(pool), nil)
	w.slot = 3_000
	w.check(0, "applied")
	slots, fast, history := w.answer(4_000, 3, 4_999)
	require.Equal(t, uint64(3_000), slots[string(pool.Bytes())])
	require.Equal(t, int64(1), fast)
	require.Zero(t, history)

	require.NoError(t, store(w).DeleteCertificatesAfterSlot(2_500, nil))
	require.NoError(t, store(w).DeleteTransactionsAfterSlot(2_500, nil))
	w.check(1, "rollback past the re-registration")
	slots, _, _ = w.answer(4_000, 3, 4_999)
	require.NotContains(t, slots, string(pool.Bytes()),
		"the retirement is in force again")

	require.NoError(t, store(w).DeleteCertificatesAfterSlot(1_500, nil))
	require.NoError(t, store(w).DeleteTransactionsAfterSlot(1_500, nil))
	w.check(2, "rollback past the retirement")
	slots, _, _ = w.answer(4_000, 3, 4_999)
	require.Equal(t, uint64(1_000), slots[string(pool.Bytes())])

	require.NoError(t, store(w).DeleteCertificatesAfterSlot(500, nil))
	require.NoError(t, store(w).DeleteTransactionsAfterSlot(500, nil))
	w.check(3, "rollback past the registration")
	var rows int
	require.NoError(t, w.store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM pool_current_event",
	).Scan(&rows))
	require.Zero(t, rows)
}

func store(w *poolCurrentWorkload) *Store { return w.store }

// Replaying an applied position with another pool's certificate removes the
// first pool's rows through the replay cleanup, which maintains its state even
// though no new certificate of that pool follows.
func TestPoolCurrentStateFollowsReplayCleanup(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xab)
	pool := w.pools[0]
	other := depositHeldPoolKey(0xac)
	w.pools = append(w.pools, other)
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	w.apply(2_000, 0, depositHeldRegistration(pool), nil)
	w.slot = 2_000
	w.check(0, "applied")
	slots, _, _ := w.answer(4_000, 3, 4_999)
	require.Equal(t, uint64(2_000), slots[string(pool.Bytes())])

	w.apply(2_000, 0, depositHeldRegistration(other), nil)
	w.check(1, "replayed with another pool's registration")
	slots, fast, history := w.answer(4_000, 3, 4_999)
	require.Equal(t, uint64(1_000), slots[string(pool.Bytes())])
	require.Equal(t, uint64(2_000), slots[string(other.Bytes())])
	require.Equal(t, int64(2), fast)
	require.Zero(t, history)
}

// A Mithril-style import baseline and a synthetic retirement write rows
// without a certificate; both maintain the pool's state.
func TestPoolCurrentStateFollowsImportAndSyntheticRetirement(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xaa)
	pool := w.pools[0]
	require.NoError(t, w.store.ImportPool(
		&models.Pool{PoolKeyHash: pool.Bytes()},
		&models.PoolRegistration{PoolKeyHash: pool.Bytes(), AddedSlot: 40},
		nil,
	))
	w.check(0, "import")
	slots, fast, history := w.answer(4_000, 3, 4_999)
	require.Equal(t, uint64(40), slots[string(pool.Bytes())])
	require.Equal(t, int64(1), fast)
	require.Zero(t, history)

	require.NoError(t, w.store.RetirePools(nil, [][]byte{pool.Bytes()}, 2, 100))
	w.check(1, "synthetic retirement")
	slots, fast, history = w.answer(4_000, 3, 4_999)
	require.NotContains(t, slots, string(pool.Bytes()))
	require.Equal(t, int64(1), fast)
	require.Zero(t, history)
}

// The epoch-boundary and tip callers ask about the current epoch and the one
// that just ended, however far the epoch table has advanced since a pool's
// rows were written, so they never leave the rows; reward recovery, repair and
// import ask about older epochs and rank the history.
func TestPoolCurrentStateCurrentEpochCallersDoNotFallBack(t *testing.T) {
	t.Parallel()
	store := newPoolCurrentStore(t)
	depositHeldEpochs(t, store, 12)
	w := &poolCurrentWorkload{
		t:     t,
		store: store,
		used:  map[[2]uint64]struct{}{},
	}
	for i := range 4 {
		w.pools = append(w.pools, depositHeldPoolKey(byte(0xb0+i)))
	}
	// Certificates at the start of history and in the latest epoch.
	w.apply(100, 0, depositHeldRegistration(w.pools[0]), nil)
	w.apply(100, 1, depositHeldRegistration(w.pools[1]), nil)
	w.apply(500, 0, depositHeldRetirement(w.pools[1], 1), nil)
	w.apply(11*depositHeldEpochLength+10, 0,
		depositHeldRegistration(w.pools[2]), nil)
	w.apply(11*depositHeldEpochLength+20, 0,
		depositHeldRegistration(w.pools[0]), nil)
	w.apply(11*depositHeldEpochLength+30, 0,
		depositHeldRetirement(w.pools[2], 12), nil)
	// The epoch table advances without a certificate touching the pools.
	for epoch := uint64(12); epoch < 40; epoch++ {
		require.NoError(t, store.SetEpoch(
			epoch*depositHeldEpochLength, epoch, nil, nil, nil, nil, 6, 1,
			depositHeldEpochLength, nil,
		))
	}
	for _, epoch := range []uint64{38, 39} {
		start := epoch * depositHeldEpochLength
		_, fast, history := w.answer(start, epoch-1, start+depositHeldEpochLength-1)
		require.Equal(t, int64(len(w.pools)), fast, "epoch %d", epoch)
		require.Zero(t, history, "epoch %d fell back to history", epoch)
	}
	_, fast, history := w.answer(0, 0, depositHeldEpochLength-1)
	require.Equal(t, int64(1), fast)
	require.Equal(t, int64(3), history, "a pre-horizon epoch ranks history")
}

// The lookup reads the stored rows of the requested pools by key and touches
// no certificate history table.
func TestPoolCurrentStateLookupDoesNotScanHistory(t *testing.T) {
	t.Parallel()
	store := newPoolCurrentStore(t)
	args := []any{[]byte{1}, []byte{2}}
	rows, err := store.writeDB.Query(
		"EXPLAIN QUERY PLAN "+poolCurrentStateLookupSQL(len(args)), args...,
	)
	require.NoError(t, err)
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	joined := strings.Join(plan, "\n")
	for _, table := range []string{
		"pool_registration", "pool_retirement", "certs",
		"applied_transaction", "leios_transaction_context",
	} {
		require.NotContains(t, joined, table)
	}
	require.NotContains(t, joined, "SCAN pool_current_event")
	require.NotContains(t, joined, "SCAN pool_current_state")
}

// The read path does not check the rows against the history, so every
// statement that writes a table the rows are derived from has to be one that
// refreshes them. A new writer fails this test until it is added here
// together with its refresh.
func TestPoolCurrentStateWritersRefresh(t *testing.T) {
	t.Parallel()
	writer := regexp.MustCompile(
		`(INSERT INTO|DELETE FROM|UPDATE)\s+"?(pool_registration|pool_retirement|certs|leios_transaction_context|applied_transaction)"?[\s(]`,
	)
	// Each statement and the function that refreshes the pools it touches.
	want := []string{
		"certificates.go DELETE FROM certs: DeleteCertificatesAfterSlot",
		"pool.go INSERT INTO pool_registration: insertPoolRegistration callers ImportPool, applyPoolRegistrationCertificate",
		"pool.go INSERT INTO pool_retirement: RetirePools",
		"transaction_certificates.go INSERT INTO certs: certificate rows hold no pool ranking key until a registration or retirement row refers to them",
		"transaction_certificates.go INSERT INTO pool_retirement: applyPoolRetirementCertificate",
		"transaction_certificates.go UPDATE certs: certificate_id back-reference, not a ranking key",
		"transaction_write.go DELETE FROM applied_transaction: DeleteTransactionsAfterSlot",
		"transaction_write.go DELETE FROM certs: DeleteTransactionsAfterSlot",
		"transaction_write.go DELETE FROM leios_transaction_context: DeleteTransactionsAfterSlot",
		"transaction_write.go INSERT INTO applied_transaction: new transaction row, no pool certificate refers to it yet",
		"transaction_write.go INSERT INTO leios_transaction_context: recordTransactionLedgerContext",
		"transaction_write.go UPDATE applied_transaction: replay, followed by deleteSpecializedCertificates and the certificate apply",
		"utxo_overlay.go INSERT INTO applied_transaction: new transaction row, no pool certificate refers to it yet",
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var got []string
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		seen := map[string]bool{}
		for _, match := range writer.FindAllStringSubmatch(string(data), -1) {
			key := file + " " + match[1] + " " + match[2]
			if seen[key] {
				continue
			}
			seen[key] = true
			got = append(got, key)
		}
	}
	slices.Sort(got)
	var wantKeys []string
	for _, entry := range want {
		wantKeys = append(wantKeys, strings.SplitN(entry, ":", 2)[0])
	}
	slices.Sort(wantKeys)
	require.Equal(t, wantKeys, got)
	for _, file := range []string{
		"certificates.go", "pool.go", "transaction_certificates.go",
		"transaction_write.go",
	} {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Contains(t, string(data), "refreshPoolCurrentState")
	}
}
