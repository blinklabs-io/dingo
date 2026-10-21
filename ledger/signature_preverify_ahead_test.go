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
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

func signatureAheadBlock(txs ...lcommon.Transaction) gledger.Block {
	return &validityOutcomeTestBlock{txs: txs, era: conway.EraConway}
}

func stopSignaturesWithin(t *testing.T, p *signaturePreverifyAhead) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		p.stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("signaturePreverifyAhead.stop did not return")
	}
}

func TestSignaturePreverifyAheadServesEveryWantedBlock(t *testing.T) {
	t.Parallel()
	good := preverifyTestTransactionWithWitness(t, 7, 1)
	bad := preverifyTestTransactionWithWitness(t, 8, 2)
	// Corrupt the signature after the witness was made: the preverified
	// result must carry the failure, as inline verification would.
	witness := bad.WitnessSet.VkeyWitnesses.Items()[0]
	witness.Signature = append([]byte(nil), witness.Signature...)
	witness.Signature[0] ^= 0xff
	bad.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{witness},
		false,
	)
	skipped := preverifyTestTransactionWithWitness(t, 9, 3)

	blocks := []gledger.Block{
		signatureAheadBlock(good, bad),
		signatureAheadBlock(skipped),
		signatureAheadBlock(),
	}
	p := startSignaturePreverifyAhead(
		context.Background(),
		blocks,
		[]bool{true, false, true},
	)
	defer stopSignaturesWithin(t, p)

	first := p.forBlock(0)
	require.Len(t, first, 2)
	require.Equal(t, lcommon.PreverifySignatures(good), first[0])
	require.Equal(t, lcommon.PreverifySignatures(bad), first[1])
	require.NotEqual(
		t, first[0], first[1],
		"a corrupted signature must not preverify like a valid one",
	)
	require.Nil(t, p.forBlock(1), "a block that is not validated gets nothing")
	require.Empty(t, p.forBlock(2))
	require.Nil(t, p.forBlock(3))
	require.Nil(t, p.forBlock(-1))
}

func TestSignaturePreverifyAheadStopWithoutTake(t *testing.T) {
	t.Parallel()
	txs := make([]lcommon.Transaction, 0, 32)
	for i := range 32 {
		txs = append(
			txs,
			preverifyTestTransactionWithWitness(t, uint64(i+1), byte(i)),
		)
	}
	p := startSignaturePreverifyAhead(
		context.Background(),
		[]gledger.Block{
			signatureAheadBlock(txs...),
			signatureAheadBlock(txs...),
		},
		[]bool{true, true},
	)
	stopSignaturesWithin(t, p)
	stopSignaturesWithin(t, p)
	select {
	case <-p.done:
	default:
		t.Fatal("stop returned before the workers exited")
	}
	// After stop, a block whose results were not all computed is served
	// nothing rather than a partial set, and a call never blocks.
	_ = p.forBlock(1)
}

func TestSignaturePreverifyAheadNilIsInert(t *testing.T) {
	t.Parallel()
	var p *signaturePreverifyAhead
	require.Nil(t, p.forBlock(0))
	p.stop()
}

func TestPreverifiedSignaturesForTxPrefersBlockPosition(t *testing.T) {
	t.Parallel()
	first := preverifyTestTransactionWithWitness(t, 1, 1)
	second := preverifyTestTransactionWithWitness(t, 2, 2)
	positional := []*lcommon.PreverifiedSignatures{
		lcommon.PreverifySignatures(first),
		lcommon.PreverifySignatures(second),
	}
	ctx := withBlockPreverifiedSignatures(context.Background(), positional)
	got, ahead := preverifiedSignaturesForTx(ctx, 1, second)
	require.Same(t, positional[1], got)
	require.True(t, ahead)
	got, ahead = preverifiedSignaturesForTx(ctx, 2, second)
	require.Nil(t, got)
	require.False(t, ahead)

	byHash := preverifyTransactionSignatures([]lcommon.Transaction{first})
	ctx = withPreverifiedSignatures(context.Background(), byHash)
	got, ahead = preverifiedSignaturesForTx(ctx, 0, first)
	require.Same(t, byHash[first.LeiosHash()], got)
	require.False(t, ahead)
	got, _ = preverifiedSignaturesForTx(context.Background(), 0, first)
	require.Nil(t, got)
}

// aheadBadSignatureChain is aheadSpendChain with the last transaction's
// witness signature corrupted, so the block that carries it must be rejected.
func aheadBadSignatureChain(
	t *testing.T,
) func(f *requiredDatumFixture, freeTxId []byte) [][]byte {
	good := aheadSpendChain(t)
	return func(f *requiredDatumFixture, freeTxId []byte) [][]byte {
		txs := good(f, freeTxId)
		last, err := conway.NewConwayTransactionFromCbor(txs[len(txs)-1])
		require.NoError(t, err)
		witness := last.WitnessSet.VkeyWitnesses.Items()[0]
		witness.Signature = append([]byte(nil), witness.Signature...)
		witness.Signature[0] ^= 0xff
		witnessSet, err := cbor.Encode(map[uint]any{
			0: []any{[]any{witness.Vkey, witness.Signature}},
		})
		require.NoError(t, err)
		corrupted, err := cbor.Encode([]any{
			cbor.RawMessage(last.Body.Cbor()),
			cbor.RawMessage(witnessSet),
			true,
			nil,
		})
		require.NoError(t, err)
		txs[len(txs)-1] = corrupted
		return txs
	}
}

func withValidateAhead(enabled, prefetch bool) func(*LedgerStateConfig) {
	return func(cfg *LedgerStateConfig) {
		cfg.LedgerValidateAheadEnabled = enabled
		cfg.LedgerPrefetchAheadEnabled = prefetch
	}
}

// TestLedgerValidateAheadMatchesSerialState replays the same chain with the
// serial path and with signatures verified ahead, alone and together with
// the UTxO prefetch, and requires the same stored state.
func TestLedgerValidateAheadMatchesSerialState(t *testing.T) {
	t.Parallel()
	serial := newAheadReplayEnvWith(
		t, withValidateAhead(false, false), aheadSpendChain(t),
	)
	require.NoError(t, serial.replayOneBatch())
	want := serial.state(t)
	require.Equal(t, serial.blocks[2].Hash().Bytes(), want.Tip.Point.Hash)
	require.Zero(t, serial.ls.signaturePreverifyAheadServed.Load())

	for _, prefetch := range []bool{false, true} {
		ahead := newAheadReplayEnvWith(
			t, withValidateAhead(true, prefetch), aheadSpendChain(t),
		)
		require.NoError(t, ahead.replayOneBatch())
		require.Equal(t, want, ahead.state(t), "prefetch=%v", prefetch)
		require.Equal(
			t,
			uint64(len(ahead.txs)),
			ahead.ls.signaturePreverifyAheadServed.Load(),
			"every validated transaction must use its ahead result",
		)
	}
}

// TestLedgerValidateAheadRejectsBadSignature pins that a signature failure
// found ahead rejects the block exactly as inline verification does.
func TestLedgerValidateAheadRejectsBadSignature(t *testing.T) {
	t.Parallel()
	serial := newAheadReplayEnvWith(
		t, withValidateAhead(false, false), aheadBadSignatureChain(t),
	)
	ahead := newAheadReplayEnvWith(
		t, withValidateAhead(true, false), aheadBadSignatureChain(t),
	)

	serialErr := serial.replayOneBatch()
	aheadErr := ahead.replayOneBatch()

	require.Error(t, serialErr)
	require.ErrorContains(t, serialErr, "signature")
	require.Error(t, aheadErr)
	require.Equal(t, serialErr.Error(), aheadErr.Error())
	require.Equal(t, serial.state(t), ahead.state(t))
	require.Equal(t, ochainsync.Tip{}, ahead.state(t).Tip)
	require.NotZero(t, ahead.ls.signaturePreverifyAheadServed.Load())
}
