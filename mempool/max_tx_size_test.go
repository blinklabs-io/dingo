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
	"encoding/hex"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// sizeLimitValidator reports a fixed MaxTxSize, or a per-era one from
// eraLimits when set, and counts how many transactions reach ledger
// validation.
type sizeLimitValidator struct {
	validated atomic.Int64
	eraLimits map[uint]uint
	maxSize   uint
	known     bool
}

func (v *sizeLimitValidator) MaxTxSize(txType uint) (uint, bool) {
	if v.eraLimits != nil {
		limit, ok := v.eraLimits[txType]
		return limit, ok
	}
	return v.maxSize, v.known
}

func (v *sizeLimitValidator) ValidateTx(gledger.Transaction) error {
	v.validated.Add(1)
	return nil
}

func (v *sizeLimitValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]lcommon.Utxo,
	*utxoref.StateOverlay,
) error {
	v.validated.Add(1)
	return nil
}

func decodeTestTx(t *testing.T) ([]byte, int) {
	t.Helper()
	txBytes, err := hex.DecodeString(testTxHex)
	require.NoError(t, err)
	tx, err := safedecode.Transaction(uint(conway.EraIdConway), txBytes)
	require.NoError(t, err)
	size, err := lcommon.TxSize(tx)
	require.NoError(t, err)
	return txBytes, size
}

func TestAddTransactionRejectsOversizedBeforeMutationGate(t *testing.T) {
	t.Parallel()
	txBytes, ledgerSize := decodeTestTx(t)
	v := &sizeLimitValidator{maxSize: uint(ledgerSize - 1), known: true}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	// Hold the gate: an oversized submission must be refused without waiting
	// for it.
	m.mutationMutex.Lock()
	gateHeld := true
	release := func() {
		if gateHeld {
			gateHeld = false
			m.mutationMutex.Unlock()
		}
	}
	defer release()

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.AddTransaction(
			context.Background(),
			uint(conway.EraIdConway),
			txBytes,
		)
	}()
	err := testutil.RequireReceive(
		t, errCh, 5*time.Second,
		"oversized submission waited on the mutation gate",
	)
	release()

	var sizeErr shelley.MaxTxSizeUtxoError
	require.ErrorAs(t, err, &sizeErr)
	require.Equal(t, uint(ledgerSize), sizeErr.TxSize)
	require.Equal(t, uint(ledgerSize-1), sizeErr.MaxTxSize)
	require.Zero(t, v.validated.Load(), "ledger rules ran for oversized tx")
	require.Empty(t, m.Transactions())
}

func TestAddTransactionAcceptsTxAtMaxTxSize(t *testing.T) {
	t.Parallel()
	txBytes, ledgerSize := decodeTestTx(t)
	v := &sizeLimitValidator{maxSize: uint(ledgerSize), known: true}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), txBytes,
	))
	require.Equal(t, int64(1), v.validated.Load())
	require.Len(t, m.Transactions(), 1)
}

func TestAddTransactionSkipsSizeCheckWhenLimitUnknown(t *testing.T) {
	t.Parallel()
	txBytes, _ := decodeTestTx(t)
	v := &sizeLimitValidator{maxSize: 1, known: false}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), txBytes,
	))
	require.Equal(t, int64(1), v.validated.Load())
}

// A Dijkstra mempool transaction is a three-element array, which common.TxSize
// measures at its full length; the early check must not discount it.
func TestAddTransactionRejectsOversizedThreeElementDijkstraTx(t *testing.T) {
	t.Parallel()
	txBytes := testutil.DijkstraShapeMempoolTxBytes(
		t, testutil.ShapeTxBody(nil),
	)
	tx, err := safedecode.Transaction(uint(gledger.TxTypeDijkstra), txBytes)
	require.NoError(t, err)
	ledgerSize, err := lcommon.TxSize(tx)
	require.NoError(t, err)
	require.Equal(t, len(txBytes), ledgerSize)

	v := &sizeLimitValidator{maxSize: uint(ledgerSize - 1), known: true}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	err = m.AddTransaction(
		context.Background(), uint(gledger.TxTypeDijkstra), txBytes,
	)
	var sizeErr shelley.MaxTxSizeUtxoError
	require.ErrorAs(t, err, &sizeErr)
	require.Equal(t, uint(ledgerSize), sizeErr.TxSize)
	require.Zero(t, v.validated.Load(), "ledger rules ran for oversized tx")
	require.Empty(t, m.Transactions())
}

// The limit is the one for the submitted era, not a single current limit.
func TestAddTransactionAppliesLimitOfSubmittedEra(t *testing.T) {
	t.Parallel()
	txBytes, ledgerSize := decodeTestTx(t)
	v := &sizeLimitValidator{eraLimits: map[uint]uint{
		uint(conway.EraIdConway):    uint(ledgerSize - 1),
		uint(gledger.TxTypeBabbage): uint(ledgerSize),
	}}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	err := m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), txBytes,
	)
	var sizeErr shelley.MaxTxSizeUtxoError
	require.ErrorAs(t, err, &sizeErr)
	require.Equal(t, uint(ledgerSize-1), sizeErr.MaxTxSize)
	require.Zero(t, v.validated.Load(), "ledger rules ran for oversized tx")
}
