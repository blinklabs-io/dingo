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

package mempool

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func pendingFixture(
	t *testing.T,
	txBytes []byte,
) (appliedTx, *MempoolTransaction) {
	t.Helper()
	tx, err := safedecode.Transaction(uint(conway.EraIdConway), txBytes)
	require.NoError(t, err)
	hash := tx.Hash().String()
	overlay := newUtxoOverlay()
	overlay.applyTx(hash, uint(conway.EraIdConway), tx, txBytes)
	return overlay.applied[0], &MempoolTransaction{
		Hash: hash,
		Type: uint(conway.EraIdConway),
		Cbor: txBytes,
	}
}

func TestRevalidationCandidateKeepsDescendantOfRemovedRejectedParent(
	t *testing.T,
) {
	t.Parallel()
	parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(t)
	parentAt, parentTx := pendingFixture(t, parentBytes)
	childAt, childTx := pendingFixture(t, childBytes)
	m := newTestMempoolWithValidator(t, newMockValidator())
	defer m.Stop(context.Background())
	reject := func(
		gledger.Transaction,
		map[utxoref.Key]struct{},
		map[utxoref.Key]lcommon.Utxo,
		*utxoref.StateOverlay,
	) error {
		return errors.New("input already spent")
	}
	accept := func(
		gledger.Transaction,
		map[utxoref.Key]struct{},
		map[utxoref.Key]lcommon.Utxo,
		*utxoref.StateOverlay,
	) error {
		return nil
	}

	candidate := newRevalidationCandidate()
	m.revalidateAppliedTx(candidate, parentAt, parentTx, reject)
	require.Contains(t, candidate.invalid, parentHash)

	// The parent was confirmed while revalidation ran; its outputs now exist
	// in the ledger, so a child admitted afterwards is valid.
	candidate.remove(map[string]struct{}{parentHash: {}})
	m.revalidateAppliedTx(candidate, childAt, childTx, accept)

	require.Contains(t, candidate.txByHash, childHash)
	require.NotContains(t, candidate.invalid, childHash)
}

func TestRevalidationCandidateValidatesDescendantOfRejectedParent(
	t *testing.T,
) {
	t.Parallel()
	parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(t)
	parentAt, parentTx := pendingFixture(t, parentBytes)
	childAt, childTx := pendingFixture(t, childBytes)
	m := newTestMempoolWithValidator(t, newMockValidator())
	defer m.Stop(context.Background())
	var validated []string
	validate := func(
		tx gledger.Transaction,
		_ map[utxoref.Key]struct{},
		_ map[utxoref.Key]lcommon.Utxo,
		_ *utxoref.StateOverlay,
	) error {
		validated = append(validated, tx.Hash().String())
		if tx.Hash().String() == parentHash {
			return errors.New("input already spent")
		}
		return nil
	}

	candidate := newRevalidationCandidate()
	m.revalidateAppliedTx(candidate, parentAt, parentTx, validate)
	m.revalidateAppliedTx(candidate, childAt, childTx, validate)

	require.Equal(
		t,
		[]string{parentHash, childHash},
		validated,
		"a rejected parent may be confirmed, so its child is still validated",
	)
	require.Contains(t, candidate.invalid, parentHash)
	require.Contains(t, candidate.txByHash, childHash)
	require.NotContains(t, candidate.overlay.created, utxoref.ForUtxo(
		mustProduced(t, parentBytes),
	), "a rejected parent's outputs leave the overlay")
}

func mustProduced(t *testing.T, txBytes []byte) lcommon.Utxo {
	t.Helper()
	tx, err := gledger.NewTransactionFromCbor(uint(conway.EraIdConway), txBytes)
	require.NoError(t, err)
	produced := tx.Produced()
	require.NotEmpty(t, produced)
	return produced[0]
}

func TestPendingUtxoOnlyTransactionRetainsNoDecodedForm(t *testing.T) {
	t.Parallel()
	parentBytes, _, _, _ := getDependentTestTxBytes(t)
	at, _ := pendingFixture(t, parentBytes)
	require.Nil(t, at.stateCbor)

	withdrawal, _ := pendingFixture(
		t,
		withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 1),
	)
	require.NotNil(t, withdrawal.stateCbor)
}

func TestMempoolDuplicateAtCapacityRetainsNothingNew(t *testing.T) {
	t.Parallel()
	first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 1)
	second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 1)
	pool, err := NewFIFO(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: retainedSize(t, first),
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), first),
	)

	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), first),
		"a duplicate is accepted without a new entry even when the pool is full",
	)
	require.Equal(t, retainedSize(t, first), pool.currentSizeBytes)
	require.Len(t, pool.Transactions(), 1)
	require.Len(t, pool.overlay.applied, 1)

	var full *MempoolFullError
	require.ErrorAs(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), second),
		&full,
		"the refresh must not make room for a different transaction",
	)
}

// retainedTxBytes measures what the pool retains for its transactions: each
// entry's CBOR plus the CBOR of every decoded output the UTxO overlay keeps
// for it.
// TestMempoolDuplicateOfExpiredTransactionIsChargedAgain resubmits a
// transaction after expiry removed it. Expiry must leave nothing a duplicate
// could refresh, so the resubmission is a new admission held to capacity.
func TestMempoolDuplicateOfExpiredTransactionIsChargedAgain(t *testing.T) {
	t.Parallel()
	first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 1)
	second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 1)
	pool, err := NewFIFO(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: retainedSize(t, first),
		TransactionTTL:  time.Minute,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	txType := uint(conway.EraIdConway)
	require.NoError(t, pool.AddTransaction(txType, first))
	pool.Lock()
	for _, tx := range pool.transactions {
		tx.LastSeen = time.Now().Add(-2 * time.Minute)
	}
	pool.Unlock()
	pool.removeExpiredTransactions()
	require.Empty(t, pool.Transactions())
	require.Zero(t, pool.currentSizeBytes)

	require.NoError(t, pool.AddTransaction(txType, second))
	var full *MempoolFullError
	require.ErrorAs(
		t,
		pool.AddTransaction(txType, first),
		&full,
		"an expired transaction is admitted afresh, not refreshed",
	)
	require.Len(t, pool.Transactions(), 1)
}

func retainedTxBytes(pool *Mempool) (int64, int) {
	pool.RLock()
	defer pool.RUnlock()
	var total int64
	for _, tx := range pool.transactions {
		total += int64(len(tx.Cbor))
	}
	for _, utxo := range pool.overlay.created {
		total += int64(len(utxo.Output.Cbor()))
	}
	return total, len(pool.overlay.applied)
}

func TestMempoolSizeCounterMatchesRetainedTransactionBytes(t *testing.T) {
	t.Parallel()
	pool, err := NewFIFO(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	var hashes []string
	for _, seed := range []byte{0x01, 0x02, 0x03} {
		txBytes := withdrawalTxCbor(t, seed, withdrawalStakeKey, uint64(seed))
		require.NoError(
			t,
			pool.AddTransaction(uint(conway.EraIdConway), txBytes),
		)
		tx, err := gledger.NewTransactionFromCbor(
			uint(conway.EraIdConway),
			txBytes,
		)
		require.NoError(t, err)
		hashes = append(hashes, tx.Hash().String())
	}
	check := func(stage string, wantTxs int) {
		t.Helper()
		retained, applied := retainedTxBytes(pool.Mempool)
		require.Equal(t, wantTxs, applied, stage)
		require.Equal(t, retained, pool.currentSizeBytes, stage)
		require.Equal(
			t,
			wantTxs,
			pool.overlay.accounts.Len(),
			"%s: every withdrawal is retained once for the state overlay, "+
				"and removal releases it",
			stage,
		)
	}
	check("after admission", 3)

	pool.RemoveTransaction(hashes[0])
	check("after removal", 2)

	require.NoError(t, pool.rebuildOverlay())
	check("after revalidation", 2)

	// Overlay entries must not carry a raw copy of the transaction bytes,
	// which currentSizeBytes would not count. A transaction that changes
	// account state keeps the pool entry's own slice so the state overlay can
	// be rebuilt; that slice must alias the pool's, not duplicate it.
	appliedType := reflect.TypeFor[appliedTx]()
	for field := range appliedType.Fields() {
		kind := field.Type.Kind()
		isBytes := kind == reflect.String ||
			(kind == reflect.Slice && field.Type.Elem().Kind() == reflect.Uint8)
		if field.Name != "hash" && field.Name != "stateCbor" {
			require.Falsef(
				t,
				isBytes,
				"appliedTx.%s retains raw bytes",
				field.Name,
			)
		}
	}
	pool.RLock()
	defer pool.RUnlock()
	for _, at := range pool.overlay.applied {
		require.NotEmpty(t, at.stateCbor)
		tx := pool.txByHash[at.hash]
		require.NotNil(t, tx)
		require.Same(
			t,
			&tx.Cbor[0],
			&at.stateCbor[0],
			"stateCbor must share the pool entry's bytes",
		)
	}
}

// retainedSize is what the pool charges for admitting txBytes: the CBOR and
// the decoded outputs its UTxO overlay keeps.
func retainedSize(t *testing.T, txBytes []byte) int64 {
	t.Helper()
	tx, err := gledger.NewTransactionFromCbor(uint(conway.EraIdConway), txBytes)
	require.NoError(t, err)
	return int64(len(txBytes)) + producedOutputBytes(tx.Produced())
}

// confirmingSessionValidator models a block confirming parentHash while the
// first revalidation pass runs: that pass sees the old ledger and is then
// reported stale, and every later pass sees the parent confirmed.
type confirmingSessionValidator struct {
	parentHash string
	sessions   atomic.Int32
}

func (v *confirmingSessionValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (v *confirmingSessionValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]lcommon.Utxo,
	*utxoref.StateOverlay,
) error {
	return nil
}

func (v *confirmingSessionValidator) WithTxValidationSession(
	fn func(
		func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]lcommon.Utxo,
			*utxoref.StateOverlay,
		) error,
		func() bool,
	) error,
) error {
	confirmed := v.sessions.Add(1) > 1
	return fn(
		func(
			tx gledger.Transaction,
			_ map[utxoref.Key]struct{},
			_ map[utxoref.Key]lcommon.Utxo,
			_ *utxoref.StateOverlay,
		) error {
			if confirmed && tx.Hash().String() == v.parentHash {
				return errors.New("input already spent")
			}
			return nil
		},
		func() bool { return confirmed },
	)
}

// TestRevalidationKeepsDescendantWhenParentConfirmedMidPass pins the
// publication guard: a pass discarded because a block confirming the parent
// was published under it is retried, and the retry keeps the child.
func TestRevalidationKeepsDescendantWhenParentConfirmedMidPass(t *testing.T) {
	t.Parallel()
	parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(t)
	for name, construct := range map[string]func(MempoolConfig) (*Mempool, error){
		"fifo": func(cfg MempoolConfig) (*Mempool, error) {
			pool, err := NewFIFO(cfg)
			if err != nil {
				return nil, err
			}
			return pool.Mempool, nil
		},
		"dag": func(cfg MempoolConfig) (*Mempool, error) {
			pool, err := NewDAG(cfg)
			if err != nil {
				return nil, err
			}
			return pool.Mempool, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			validator := &confirmingSessionValidator{parentHash: parentHash}
			pool, err := construct(MempoolConfig{
				Validator:       validator,
				MempoolCapacity: 1 << 20,
				PromRegistry:    prometheus.NewRegistry(),
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = pool.Stop(context.Background()) })
			txType := uint(conway.EraIdConway)
			require.NoError(t, pool.AddTransaction(txType, parentBytes))
			require.NoError(t, pool.AddTransaction(txType, childBytes))

			require.NoError(t, pool.rebuildOverlay())

			require.EqualValues(t, 2, validator.sessions.Load())
			txs := pool.Transactions()
			require.Len(t, txs, 1)
			require.Equal(t, childHash, txs[0].Hash)
		})
	}
}
