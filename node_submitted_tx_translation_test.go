// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dingo

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func historicalSpendTxCbor(
	t *testing.T,
	inputID []byte,
	inputIndex uint32,
	amount uint64,
) []byte {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	address := append(
		[]byte{0x60},
		lcommon.Blake2b224Hash(key.Public().(ed25519.PublicKey)).Bytes()...)
	body := map[uint]any{
		0: []any{[]any{inputID, uint64(inputIndex)}},
		1: []any{[]any{address, amount}},
		2: uint64(200_000),
	}
	unsigned, err := cbor.Encode([]any{body, map[uint]any{}, nil})
	require.NoError(t, err)
	tx, err := mary.NewMaryTransactionFromCbor(unsigned)
	require.NoError(t, err)
	signature := ed25519.Sign(key, tx.Hash().Bytes())
	raw, err := cbor.Encode([]any{
		body,
		map[uint]any{
			0: []any{[]any{key.Public().(ed25519.PublicKey), signature}},
		},
		nil,
	})
	require.NoError(t, err)
	return raw
}

func seedHistoricalSubmissionUtxo(
	t *testing.T,
	set *utxoset.Set,
	txID []byte,
) {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	address := append(
		[]byte{0x60},
		lcommon.Blake2b224Hash(key.Public().(ed25519.PublicKey)).Bytes()...)
	output, err := cbor.Encode([]any{address, uint64(10_000_000)})
	require.NoError(t, err)
	require.NoError(t, set.ApplyUtxoChanges(nil, []ledger.UtxoEvent{{
		Added: []ledger.UtxoOutput{{
			Ref:  models.UtxoId{Hash: txID},
			Cbor: output,
		}},
	}}))
}

func TestHistoricalSubmissionUsesCurrentEraAcrossMempoolBoundaries(
	t *testing.T,
) {
	t.Parallel()

	f := newMithrilTestLedger(t)

	rootID := bytes.Repeat([]byte{0x5a}, 32)
	seedHistoricalSubmissionUtxo(t, f.utxos, rootID)
	raw := historicalSpendTxCbor(t, rootID, 0, 9_800_000)
	previous, err := gledger.NewTransactionFromCbor(
		uint(mary.EraIdMary),
		raw,
	)
	require.NoError(t, err)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	fifo, err := dingomempool.NewFIFO(dingomempool.MempoolConfig{
		Validator: f.ls,
		EventBus:  bus,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		PromRegistry:    prometheus.NewRegistry(),
		MempoolCapacity: 1 << 20,
	})
	require.NoError(t, err)
	pool := fifo.Mempool
	require.NoError(t, pool.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})

	require.NoError(t, pool.AddTransaction(
		context.Background(),
		uint(mary.EraIdMary),
		raw,
	))
	stored, ok := pool.GetTransaction(previous.Hash().String())
	require.True(t, ok)
	require.Equal(t, uint(alonzo.EraIdAlonzo), stored.Type)
	decoded, err := gledger.NewTransactionFromCbor(stored.Type, stored.Cbor)
	require.NoError(t, err)
	require.Equal(t, previous.Hash(), decoded.Hash())

	consumer := pool.AddConsumer(ouroboros.ConnectionId{})
	require.NotNil(t, consumer)
	relayed := consumer.NextTx(false)
	require.NotNil(t, relayed)
	require.Equal(t, uint(alonzo.EraIdAlonzo), relayed.Type)
}

func TestHistoricalSubmissionAppliesCurrentEraRules(t *testing.T) {
	t.Parallel()

	f := newMithrilTestLedger(t)

	missingID := bytes.Repeat([]byte{0x7b}, 32)
	raw := historicalSpendTxCbor(t, missingID, 0, 9_000_000)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	fifo, err := dingomempool.NewFIFO(dingomempool.MempoolConfig{
		Validator:       f.ls,
		EventBus:        bus,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MempoolCapacity: 1 << 20,
	})
	require.NoError(t, err)
	pool := fifo.Mempool
	require.NoError(t, pool.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})

	err = pool.AddTransaction(
		context.Background(),
		uint(mary.EraIdMary),
		raw,
	)
	var badInputs shelley.BadInputsUtxoError
	require.ErrorAs(t, err, &badInputs)
	var submittedEra interface{ SubmissionEra() uint }
	require.ErrorAs(t, err, &submittedEra)
	require.Equal(t, uint(alonzo.EraIdAlonzo), submittedEra.SubmissionEra())
}
