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

package ledger

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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
	raw, err := cbor.Encode([]any{
		map[uint]any{
			0: []any{[]any{inputID, uint64(inputIndex)}},
			1: []any{[]any{revalidationTestAddress, amount}},
			2: uint64(200_000),
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	return raw
}

func TestHistoricalSubmissionUsesCurrentEraAcrossMempoolBoundaries(
	t *testing.T,
) {
	t.Parallel()

	f := newPendingAccountsFixtureWithRule(
		t,
		0,
		shelley.UtxoValidateBadInputsUtxo,
		&shelley.ShelleyProtocolParameters{},
	)
	f.ls.activeEras = eras.Eras
	f.ls.publishSnapshotsLocked()

	rootID := bytes.Repeat([]byte{0x5a}, 32)
	seedLedgerUtxo(t, f.ls.db, rootID, 0, 10_000_000)
	raw := historicalSpendTxCbor(t, rootID, 0, 9_000_000)
	previous, err := gledger.NewTransactionFromCbor(
		uint(babbage.EraIdBabbage),
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
		uint(babbage.EraIdBabbage),
		raw,
	))
	stored, ok := pool.GetTransaction(previous.Hash().String())
	require.True(t, ok)
	require.Equal(t, uint(conway.EraIdConway), stored.Type)
	decoded, err := gledger.NewTransactionFromCbor(stored.Type, stored.Cbor)
	require.NoError(t, err)
	require.Equal(t, previous.Hash(), decoded.Hash())

	consumer := pool.AddConsumer(ouroboros.ConnectionId{})
	require.NotNil(t, consumer)
	relayed := consumer.NextTx(false)
	require.NotNil(t, relayed)
	require.Equal(t, uint(conway.EraIdConway), relayed.Type)
}

func TestHistoricalSubmissionAppliesCurrentEraRules(t *testing.T) {
	t.Parallel()

	f := newPendingAccountsFixtureWithRule(
		t,
		0,
		shelley.UtxoValidateBadInputsUtxo,
		&shelley.ShelleyProtocolParameters{},
	)
	f.ls.activeEras = eras.Eras
	f.ls.publishSnapshotsLocked()

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
		uint(babbage.EraIdBabbage),
		raw,
	)
	var badInputs shelley.BadInputsUtxoError
	require.ErrorAs(t, err, &badInputs)
	var submittedEra interface{ SubmissionEra() uint }
	require.ErrorAs(t, err, &submittedEra)
	require.Equal(t, uint(conway.EraIdConway), submittedEra.SubmissionEra())
}
