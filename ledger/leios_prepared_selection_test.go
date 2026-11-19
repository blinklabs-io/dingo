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

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"

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
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
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
				return admit(raw)
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
