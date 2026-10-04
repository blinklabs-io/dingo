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
	"testing"

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
	overlay.applyTx(hash, uint(conway.EraIdConway), tx)
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
		*utxoref.AccountOverlay,
	) error {
		return errors.New("input already spent")
	}
	accept := func(
		gledger.Transaction,
		map[utxoref.Key]struct{},
		map[utxoref.Key]lcommon.Utxo,
		*utxoref.AccountOverlay,
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

func TestRevalidationCandidateRejectsDescendantOfRejectedParent(
	t *testing.T,
) {
	t.Parallel()
	parentBytes, childBytes, parentHash, childHash := getDependentTestTxBytes(t)
	parentAt, parentTx := pendingFixture(t, parentBytes)
	childAt, childTx := pendingFixture(t, childBytes)
	m := newTestMempoolWithValidator(t, newMockValidator())
	defer m.Stop(context.Background())
	validate := func(
		tx gledger.Transaction,
		_ map[utxoref.Key]struct{},
		_ map[utxoref.Key]lcommon.Utxo,
		_ *utxoref.AccountOverlay,
	) error {
		if tx.Hash().String() == parentHash {
			return errors.New("input already spent")
		}
		return nil
	}

	candidate := newRevalidationCandidate()
	m.revalidateAppliedTx(candidate, parentAt, parentTx, validate)
	m.revalidateAppliedTx(candidate, childAt, childTx, validate)

	require.Contains(t, candidate.invalid, childHash)
	require.Empty(t, candidate.txByHash)
}

func TestMempoolDuplicateAtCapacityRetainsNothingNew(t *testing.T) {
	t.Parallel()
	first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 1)
	second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 1)
	validator := &balanceValidator{}
	validator.balance.Store(100)
	pool, err := NewFIFO(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: int64(len(first)),
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
	require.Equal(t, int64(len(first)), pool.currentSizeBytes)
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

func retainedTxBytes(pool *Mempool) (int64, int) {
	pool.RLock()
	defer pool.RUnlock()
	var total int64
	for _, tx := range pool.transactions {
		total += int64(len(tx.Cbor))
	}
	return total, len(pool.overlay.applied)
}

func TestMempoolSizeCounterMatchesRetainedTransactionBytes(t *testing.T) {
	t.Parallel()
	validator := &balanceValidator{}
	validator.balance.Store(1_000)
	pool, err := NewFIFO(MempoolConfig{
		Validator:       validator,
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
	}
	check("after admission", 3)

	pool.RemoveTransaction(hashes[0])
	check("after removal", 2)

	require.NoError(t, pool.rebuildOverlay())
	check("after revalidation", 2)

	// Overlay entries must not carry a second copy of the transaction bytes,
	// which currentSizeBytes would not count.
	appliedType := reflect.TypeFor[appliedTx]()
	for field := range appliedType.Fields() {
		kind := field.Type.Kind()
		isBytes := kind == reflect.String ||
			(kind == reflect.Slice && field.Type.Elem().Kind() == reflect.Uint8)
		if field.Name != "hash" {
			require.Falsef(t, isBytes, "appliedTx.%s retains raw bytes", field.Name)
		}
	}
}
