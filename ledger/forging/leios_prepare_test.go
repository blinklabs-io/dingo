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

package forging

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func preparationCandidates(tb testing.TB, count int) []MempoolTransaction {
	tb.Helper()
	txs := make([]MempoolTransaction, count)
	for i := range txs {
		input := make([]byte, 32)
		for j := range input {
			input[j] = byte(i >> (8 * (j % 8)))
		}
		raw, err := cbor.Encode([]any{
			map[uint]any{
				0: []any{[]any{input, uint64(0)}},
				1: []any{
					[]any{
						append([]byte{0x61}, make([]byte, 28)...),
						uint64(1000000),
					},
				},
				2: uint64(200000),
			},
			map[uint]any{}, true, nil,
		})
		require.NoError(tb, err)
		tx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(tb, err)
		txs[i] = MempoolTransaction{
			Hash: tx.Hash().String(),
			Cbor: raw,
			Type: conway.TxTypeConway,
		}
	}
	return txs
}

func TestLeiosPreparationMatchesSerial(t *testing.T) {
	t.Parallel()
	txs := preparationCandidates(t, 2*leiosPreparationWindow+3)
	txs[1].Cbor = []byte{0xff}
	txs[63].Hash = "invalid"
	txs[64].Type = ^uint(0)
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			t.Parallel()
			p := newLeiosCandidatePreparation(txs, workers)
			for i := range txs {
				expected := prepareLeiosCandidate(txs[i])
				actual, err := p.next(t.Context())
				require.NoError(t, err)
				require.Equal(t, expected.mempool, actual.mempool)
				require.Equal(t, expected.blockTx, actual.blockTx)
				if expected.err != nil {
					require.EqualError(t, actual.err, expected.err.Error())
				} else {
					require.NoError(t, actual.err)
					require.Equal(t, expected.tx.Hash(), actual.tx.Hash())
					require.Equal(t, expected.tx.Cbor(), actual.tx.Cbor())
				}
			}
		})
	}
}

func TestLeiosPreparationCancellationJoinsAndBoundsWork(t *testing.T) {
	t.Parallel()
	txs := preparationCandidates(t, 3*leiosPreparationWindow)
	p := newLeiosCandidatePreparation(txs, 4)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var active, calls atomic.Int64
	original := p.prepare
	p.prepare = func(tx MempoolTransaction) preparedLeiosCandidate {
		active.Add(1)
		defer active.Add(-1)
		calls.Add(1)
		cancel()
		return original(tx)
	}
	_, err := p.next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, active.Load())
	require.LessOrEqual(t, calls.Load(), int64(4))
	p = newLeiosCandidatePreparation(txs, 4)
	calls.Store(0)
	p.prepare = func(tx MempoolTransaction) preparedLeiosCandidate {
		calls.Add(1)
		return original(tx)
	}
	_, err = p.next(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(leiosPreparationWindow), calls.Load())
	for range leiosPreparationWindow - 1 {
		_, err = p.next(t.Context())
		require.NoError(t, err)
	}
	require.Equal(t, int64(leiosPreparationWindow), calls.Load())
}

func TestLeiosPreparationRunsIndependentWorkConcurrently(t *testing.T) {
	t.Parallel()
	txs := preparationCandidates(t, leiosPreparationWindow)
	p := newLeiosCandidatePreparation(txs, 4)
	entered := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	original := p.prepare
	p.prepare = func(tx MempoolTransaction) preparedLeiosCandidate {
		if tx.Hash == txs[0].Hash {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		} else if tx.Hash == txs[1].Hash {
			select {
			case <-entered:
			case <-ctx.Done():
			}
			close(release)
		}
		return original(tx)
	}
	done := make(chan error, 1)
	go func() { _, err := p.next(ctx); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("preparation workers did not complete concurrently")
	}
}

func TestLeiosPreparationStopsClaimingAtDeadline(t *testing.T) {
	t.Parallel()
	txs := preparationCandidates(t, leiosPreparationWindow)
	p := newLeiosCandidatePreparation(txs, 1)
	expired := false
	calls := 0
	original := p.prepare
	p.expired = func() bool { return expired }
	p.prepare = func(tx MempoolTransaction) preparedLeiosCandidate {
		calls++
		expired = true
		return original(tx)
	}
	_, err := p.next(t.Context())
	require.ErrorIs(t, err, errLeiosPreparationExpired)
	require.Equal(t, 1, calls, "no additional candidate may start after expiry")
}

func BenchmarkLeiosPreparation(b *testing.B) {
	txs := preparationCandidates(b, 8854)
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprint(workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				p := newLeiosCandidatePreparation(txs, workers)
				for range txs {
					candidate, err := p.next(b.Context())
					if err != nil || candidate.err != nil {
						b.Fatal(err, candidate.err)
					}
				}
			}
		})
	}
}
