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

package ledger

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/utxoref"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func prepareFixtureEndorserBlock(
	t *testing.T,
	f *leiosCertApplyFixture,
	slot uint64,
) forging.LeiosValidationReceipt {
	t.Helper()
	receipt, err := f.ls.WithPreparedLeiosSelection(
		t.Context(),
		f.originHash,
		slot,
		func(
			validate func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			_ forging.TxRefScriptSizeFunc,
			admit func([]byte) error,
			current func() bool,
		) error {
			consumed := make(map[utxoref.Key]struct{})
			created := make(map[utxoref.Key]lcommon.Utxo)
			for _, raw := range f.ebTxs {
				tx, err := validate(raw, consumed, created)
				if err != nil {
					return err
				}
				if err := admit(raw); err != nil {
					return err
				}
				for _, input := range tx.Consumed() {
					consumed[utxoref.ForInput(input)] = struct{}{}
				}
				for _, output := range tx.Produced() {
					created[utxoref.ForUtxo(output)] = output
				}
			}
			require.True(t, current())
			return nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	return receipt
}

func TestPreparedLeiosSelectionMatchesFullValidation(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	originalEra := eras.DijkstraEraDesc
	era := originalEra
	var slots []uint64
	era.ValidateTxFunc = func(tx lcommon.Transaction, slot uint64, state lcommon.LedgerState, pp lcommon.ProtocolParameters) error {
		slots = append(slots, slot)
		return originalEra.ValidateTxFunc(tx, slot, state, pp)
	}
	f.ls.currentEra = era
	f.ls.activeEras = []eras.EraDesc{era}
	f.ls.publishSnapshotsLocked()
	receipt := prepareFixtureEndorserBlock(t, f, leiosCertApplyAnnouncingSlot)
	block, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	txs := make([][]byte, len(f.ebTxs))
	for i, raw := range f.ebTxs {
		txs[i] = raw
	}
	require.True(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			txs,
		),
	)
	require.NoError(
		t,
		f.ls.ValidateLeiosEndorserBlockTransactions(
			t.Context(),
			block.Header(),
			f.originHash,
			txs,
		),
	)
	require.Len(t, slots, 2)
	require.Equal(
		t,
		[]uint64{leiosCertApplyAnnouncingSlot, leiosCertApplyAnnouncingSlot},
		slots,
	)
	f.requireEndorserEffects(false)
}

func TestPreparedLeiosReceiptRejectsChangedContext(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	receipt := prepareFixtureEndorserBlock(t, f, leiosCertApplyAnnouncingSlot)
	txs := make([][]byte, len(f.ebTxs))
	for i, raw := range f.ebTxs {
		txs[i] = bytes.Clone(raw)
	}
	for _, tc := range []struct {
		name         string
		number, slot uint64
		parent       []byte
		certified    bool
	}{
		{"slot", 1, leiosCertApplyAnnouncingSlot + 1, f.originHash, false},
		{"block number", 2, leiosCertApplyAnnouncingSlot, f.originHash, false},
		{"parent", 1, leiosCertApplyAnnouncingSlot, bytes.Repeat([]byte{0x51}, 32), false},
		{"certified closure", 1, leiosCertApplyAnnouncingSlot, f.originHash, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, _ := f.block(
				tc.number,
				tc.slot,
				tc.parent,
				tc.certified,
				true,
			)
			require.False(
				t,
				receipt.MatchesLeiosAnnouncement(
					t.Context(),
					block.Header(),
					f.originHash,
					txs,
				),
			)
		})
	}
	block, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	require.True(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			txs,
		),
	)
	require.False(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			bytes.Repeat([]byte{0x52}, 32),
			txs,
		),
	)
	require.False(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			nil,
		),
	)
	txs[0][len(txs[0])-1] ^= 1
	require.False(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			txs,
		),
	)
}

func TestPreparedLeiosReceiptRejectsNewPublicationAndCancellation(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	receipt := prepareFixtureEndorserBlock(t, f, leiosCertApplyAnnouncingSlot)
	block, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	txs := [][]byte{f.ebTxs[0]}
	prepared := receipt.(forging.LeiosPreparationReceipt)
	require.True(
		t,
		prepared.StillValidForParent(
			t.Context(),
			f.originHash,
			leiosCertApplyAnnouncingSlot,
		),
	)
	require.False(
		t,
		prepared.StillValidForParent(
			t.Context(),
			[]byte{1},
			leiosCertApplyAnnouncingSlot,
		),
	)
	require.False(
		t,
		prepared.StillValidForParent(
			t.Context(),
			f.originHash,
			leiosCertApplyAnnouncingSlot+1,
		),
	)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.False(
		t,
		prepared.StillValidForParent(
			ctx,
			f.originHash,
			leiosCertApplyAnnouncingSlot,
		),
	)
	require.False(
		t,
		receipt.MatchesLeiosAnnouncement(
			ctx,
			block.Header(),
			f.originHash,
			txs,
		),
	)
	f.ls.publishSnapshotsLocked()
	require.False(
		t,
		prepared.StillValidForParent(
			t.Context(),
			f.originHash,
			leiosCertApplyAnnouncingSlot,
		),
	)
	require.False(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			txs,
		),
		"same hash in a new publication is not the same validation context",
	)
}

func TestPreparedLeiosSelectionRejectsInvalidWitness(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.corruptEndorserWitness()
	receipt, err := f.ls.WithPreparedLeiosSelection(
		t.Context(),
		f.originHash,
		leiosCertApplyAnnouncingSlot,
		func(
			validate func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			_ forging.TxRefScriptSizeFunc,
			_ func([]byte) error,
			_ func() bool,
		) error {
			_, err := validate(
				f.ebTxs[0],
				map[utxoref.Key]struct{}{},
				map[utxoref.Key]lcommon.Utxo{},
			)
			return err
		},
	)
	require.Error(t, err)
	require.Nil(t, receipt)
}

func TestPreparedLeiosSelectionPreservesOrderedAccountEffects(t *testing.T) {
	t.Parallel()
	account := pathKeyAccount(0x42)
	f := newLeiosCertApplyFixtureWithTransactions(
		t,
		[]pathAccount{{key: 0x42, reward: 5}},
		0x42,
		[]pathTx{
			{
				top: pathLevel{
					fields: pathDeposits(
						map[cbor.ByteString]uint64{account: 20},
					),
					funds: 20,
				},
			},
			{top: pathLevel{fields: pathExact(account, 25, 26)}},
		},
	)
	receipt := prepareFixtureEndorserBlock(t, f, leiosCertApplyAnnouncingSlot)
	block, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	txs := make([][]byte, len(f.ebTxs))
	for i, raw := range f.ebTxs {
		txs[i] = raw
	}
	require.True(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			txs,
		),
	)
	require.NoError(
		t,
		f.ls.ValidateLeiosEndorserBlockTransactions(
			t.Context(),
			block.Header(),
			f.originHash,
			txs,
		),
	)
	require.False(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			[][]byte{txs[1], txs[0]},
		),
	)
	f.requireEndorserEffects(false)
}

func TestPreparedLeiosSelectionCannotAdmitUnvalidatedBytes(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	for _, validated := range []bool{false, true} {
		receipt, err := f.ls.WithPreparedLeiosSelection(
			t.Context(),
			f.originHash,
			leiosCertApplyAnnouncingSlot,
			func(
				validate func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
				_ forging.TxRefScriptSizeFunc,
				admit func([]byte) error,
				_ func() bool,
			) error {
				raw := bytes.Clone(f.ebTxs[0])
				if validated {
					_, err := validate(
						raw,
						map[utxoref.Key]struct{}{},
						map[utxoref.Key]lcommon.Utxo{},
					)
					if err != nil {
						return err
					}
					raw[len(raw)-1] ^= 1
				}
				require.ErrorContains(t, admit(raw), "does not match")
				return nil
			},
		)
		require.ErrorContains(t, err, "does not match")
		require.Nil(t, receipt)
	}
}

func TestPreparedLeiosSelectionDoesNotCertifyIgnoredStorageFault(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.ls.currentPParams = nil
	f.ls.publishSnapshotsLocked()
	receipt, err := f.ls.WithPreparedLeiosSelection(
		t.Context(),
		f.originHash,
		leiosCertApplyAnnouncingSlot,
		func(
			validate func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			_ forging.TxRefScriptSizeFunc,
			_ func([]byte) error,
			_ func() bool,
		) error {
			_, err := validate(
				f.ebTxs[0],
				map[utxoref.Key]struct{}{},
				map[utxoref.Key]lcommon.Utxo{},
			)
			require.Error(t, err)
			return nil
		},
	)
	require.Error(t, err)
	require.Nil(t, receipt)
}

func BenchmarkLeiosSelectionReceipt(b *testing.B) {
	for _, count := range []int{3089, 8854} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			ls := &LedgerState{}
			ls.publishSnapshotsLocked()
			consensus, _ := ls.loadStateSnapshots()
			parent := bytes.Repeat([]byte{0x71}, 32)
			txs := make([][]byte, count)
			for i := range txs {
				txs[i] = bytes.Repeat([]byte{byte(i % 256)}, 600)
			}
			receipt := &leiosSelectionReceipt{ls: ls, generation: consensus.generation, parent: parent, slot: 10, blockNumber: 1, transactions: leiosSelectionDigest(txs)}
			header := &dijkstra.DijkstraBlockHeader{BabbageBlockHeader: babbage.BabbageBlockHeader{Body: babbage.BabbageBlockHeaderBody{Slot: 10, BlockNumber: 1, PrevHash: lcommon.NewBlake2b256(parent)}}, LeiosHeaderExtension: []cbor.RawMessage{{0xf4}, {0xf6}}}
			require.True(b, receipt.MatchesLeiosAnnouncement(context.Background(), header, parent, txs))
			b.ReportAllocs()
			b.SetBytes(int64(count * 600))
			b.ResetTimer()
			for b.Loop() {
				if !receipt.MatchesLeiosAnnouncement(context.Background(), header, parent, txs) {
					b.Fatal("receipt mismatch")
				}
			}
		})
	}
}

func TestParallelLeiosSelectionAdmitsOnlyTheValidatedToken(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	valid := bytes.Clone(f.ebTxs[0])
	f.corruptEndorserWitness()
	receipt, err := f.ls.WithParallelLeiosSelection(
		t.Context(),
		f.originHash,
		leiosCertApplyAnnouncingSlot,
		func(
			prepare func([]byte) (forging.PreparedLeiosTransaction, error),
			validate func(forging.PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			_ forging.TxRefScriptSizeFunc,
			admit func(forging.PreparedLeiosTransaction) error,
			_ func() bool,
		) error {
			original, err := prepare(valid)
			if err != nil {
				return err
			}
			other, err := prepare(f.ebTxs[0])
			if err != nil {
				return err
			}
			_, err = validate(
				original,
				map[utxoref.Key]struct{}{},
				map[utxoref.Key]lcommon.Utxo{},
			)
			if err != nil {
				return err
			}
			require.ErrorContains(t, admit(other), "does not match")
			return nil
		},
	)
	require.ErrorContains(t, err, "does not match")
	require.Nil(t, receipt)
	f.requireEndorserEffects(false)
}

func TestParallelLeiosPreparationOwnsWireBytes(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	raw := bytes.Clone(f.ebTxs[0])
	token, err := prepareLeiosSelectionTransaction(raw)
	require.NoError(t, err)
	original := bytes.Clone(token.BlockTransactionCbor())
	raw[len(raw)-1] ^= 1
	require.Equal(t, original, token.BlockTransactionCbor())
}

func TestParallelLeiosSelectionDoesNotApplyAnUnadmittedCandidate(t *testing.T) {
	t.Parallel()
	account := pathKeyAccount(0x42)
	f := newLeiosCertApplyFixtureWithTransactions(
		t,
		[]pathAccount{{key: 0x42, reward: 5}},
		0x42,
		[]pathTx{
			{
				top: pathLevel{
					fields: pathDeposits(
						map[cbor.ByteString]uint64{account: 20},
					),
					funds: 20,
				},
			},
			{top: pathLevel{fields: pathExact(account, 5, 26)}},
		},
	)
	receipt, err := f.ls.WithParallelLeiosSelection(
		t.Context(),
		f.originHash,
		leiosCertApplyAnnouncingSlot,
		func(
			prepare func([]byte) (forging.PreparedLeiosTransaction, error),
			validate func(forging.PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			_ forging.TxRefScriptSizeFunc,
			admit func(forging.PreparedLeiosTransaction) error,
			_ func() bool,
		) error {
			consumed := map[utxoref.Key]struct{}{}
			created := map[utxoref.Key]lcommon.Utxo{}
			first, err := prepare(f.ebTxs[0])
			if err != nil {
				return err
			}
			if _, err := validate(first, consumed, created); err != nil {
				return err
			}
			second, err := prepare(f.ebTxs[1])
			if err != nil {
				return err
			}
			if _, err := validate(second, consumed, created); err != nil {
				return err
			}
			return admit(second)
		},
	)
	require.NoError(t, err)
	block, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	require.True(
		t,
		receipt.MatchesLeiosAnnouncement(
			t.Context(),
			block.Header(),
			f.originHash,
			[][]byte{f.ebTxs[1]},
		),
	)
	require.NoError(
		t,
		f.ls.ValidateLeiosEndorserBlockTransactions(
			t.Context(),
			block.Header(),
			f.originHash,
			[][]byte{f.ebTxs[1]},
		),
	)
	f.requireEndorserEffects(false)
}

func TestParallelLeiosSelectionValidatesThePreparedObject(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	era := eras.DijkstraEraDesc
	original := era.ValidateTxFunc
	var validated lcommon.Transaction
	era.ValidateTxFunc = func(tx lcommon.Transaction, slot uint64, state lcommon.LedgerState, pp lcommon.ProtocolParameters) error {
		validated = tx
		return original(tx, slot, state, pp)
	}
	f.ls.currentEra = era
	f.ls.activeEras = []eras.EraDesc{era}
	f.ls.publishSnapshotsLocked()
	receipt, err := f.ls.WithParallelLeiosSelection(
		t.Context(),
		f.originHash,
		leiosCertApplyAnnouncingSlot,
		func(
			prepare func([]byte) (forging.PreparedLeiosTransaction, error),
			validate func(forging.PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
			_ forging.TxRefScriptSizeFunc,
			admit func(forging.PreparedLeiosTransaction) error,
			_ func() bool,
		) error {
			token, err := prepare(f.ebTxs[0])
			if err != nil {
				return err
			}
			prepared := token.Transaction()
			selected, err := validate(
				token,
				map[utxoref.Key]struct{}{},
				map[utxoref.Key]lcommon.Utxo{},
			)
			if err != nil {
				return err
			}
			require.Same(t, prepared, validated)
			require.Same(t, prepared, selected)
			return admit(token)
		},
	)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	f.requireEndorserEffects(false)
}

func TestParallelLeiosSelectionYieldsWriterToLedgerTransition(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	entered := make(chan struct{})
	finished := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(
		func() { cancel(); testutil.RequireReceive(t, finished, 5*time.Second, "selection cleanup") },
	)
	era := eras.DijkstraEraDesc
	era.ValidateTxFunc = func(_ lcommon.Transaction, _ uint64, state lcommon.LedgerState, _ lcommon.ProtocolParameters) error {
		evaluation := state.(interface{ EvaluationContext() context.Context }).EvaluationContext()
		close(entered)
		<-evaluation.Done()
		return evaluation.Err()
	}
	f.ls.currentEra = era
	f.ls.activeEras = []eras.EraDesc{era}
	f.ls.publishSnapshotsLocked()
	type result struct {
		receipt forging.LeiosValidationReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		defer close(finished)
		receipt, err := f.ls.WithParallelLeiosSelection(
			ctx,
			f.originHash,
			leiosCertApplyAnnouncingSlot,
			func(
				prepare func([]byte) (forging.PreparedLeiosTransaction, error),
				validate func(forging.PreparedLeiosTransaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
				_ forging.TxRefScriptSizeFunc,
				_ func(forging.PreparedLeiosTransaction) error,
				_ func() bool,
			) error {
				token, err := prepare(f.ebTxs[0])
				if err != nil {
					return err
				}
				_, err = validate(
					token,
					map[utxoref.Key]struct{}{},
					map[utxoref.Key]lcommon.Utxo{},
				)
				return err
			},
		)
		done <- result{receipt: receipt, err: err}
	}()
	testutil.RequireReceive(t, entered, 5*time.Second, "validation owns writer")
	finish := f.ls.beginTxValidationTransition()
	defer finish()
	outcome := testutil.RequireReceive(
		t,
		done,
		5*time.Second,
		"transition preempts validation",
	)
	require.ErrorIs(t, outcome.err, forging.ErrTxValidationParentMismatch)
	require.ErrorIs(t, outcome.err, ErrLeiosValidationParentSuperseded)
	require.Nil(t, outcome.receipt)
	f.ls.leiosSelectionMu.Lock()
	registrations := len(f.ls.leiosSelections)
	f.ls.leiosSelectionMu.Unlock()
	require.Zero(t, registrations)
	f.requireEndorserEffects(false)
}

func TestParallelLeiosPreparationPreservesIndefiniteWireBytes(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	raw := append([]byte{0x9f}, f.ebTxs[0][1:]...)
	raw = append(raw, 0xff)
	token, err := prepareLeiosSelectionTransaction(raw)
	require.NoError(t, err)
	require.Equal(t, raw, token.BlockTransactionCbor())
}

func TestPreparedLeiosSelectionDoesNotQueueBehindLedgerTransition(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	finish := f.ls.beginTxValidationTransition()
	defer finish()
	opened := false
	called := false
	f.ls.txValidationSessionOpenedHook = func() { opened = true }
	receipt, err := f.ls.WithPreparedLeiosSelection(t.Context(), f.originHash, leiosCertApplyAnnouncingSlot, func(
		_ func([]byte, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) (lcommon.Transaction, error),
		_ forging.TxRefScriptSizeFunc,
		_ func([]byte) error,
		_ func() bool,
	) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, forging.ErrTxValidationParentMismatch)
	require.Nil(t, receipt)
	require.False(t, opened, "speculation must not queue for the writer during a ledger transition")
	require.False(t, called)
}
