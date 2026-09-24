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

package txpump

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func testPump(genesisTime time.Time, startupTimeout time.Duration) *Pump {
	return NewPump(
		&Config{StartupTimeout: startupTimeout},
		NewWallet(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		genesisTime,
	)
}

// TestWaitForGenesis proves that node connectivity cannot release transaction
// generation before the network start boundary.
func TestWaitForGenesis(t *testing.T) {
	pump := testPump(time.Now().Add(25*time.Millisecond), time.Second)
	started := time.Now()
	require.NoError(t, pump.waitForGenesis(context.Background(), nil))
	require.GreaterOrEqual(t, time.Since(started), 20*time.Millisecond)
}

// TestWaitForGenesisHonorsStartupTimeout ensures a malformed future runtime
// start surfaces as a readiness failure instead of hanging the workload.
func TestWaitForGenesisHonorsStartupTimeout(t *testing.T) {
	pump := testPump(time.Now().Add(time.Hour), time.Millisecond)
	startup := make(chan time.Time, 1)
	startup <- time.Now()
	err := pump.waitForGenesis(context.Background(), startup)
	require.ErrorContains(t, err, "genesis has not started")
}

// TestCurrentSlotBeforeGenesisIsZero guards the signed-duration to uint64
// conversion that previously produced an architecture-independent wrap.
func TestCurrentSlotBeforeGenesisIsZero(t *testing.T) {
	pump := testPump(time.Now().Add(time.Hour), time.Second)
	require.Zero(t, pump.currentSlot())
}

func TestRunSkipsBatchWhenKeyedWalletReconciliationFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := &UTxOKey{Address: []byte{1, 2, 3}}
	wallet := NewWallet()
	wallet.Add(UTxO{TxHash: "funding", Amount: 5_000_000, SigningKey: key})
	pump := NewPump(
		&Config{TxCountMin: 1, TxCountMax: 1},
		wallet,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		time.Now().Add(-time.Second),
	)
	var batches int
	pump.dialPrimaryFn = func() (*NodeClient, error) {
		return &NodeClient{addr: "test"}, nil
	}
	pump.runBatchFn = func(context.Context, *NodeClient, int) int {
		batches++
		return 0
	}
	pump.cooldownFn = func(context.Context) bool {
		cancel()
		return false
	}

	require.ErrorIs(t, pump.Run(ctx), context.Canceled)
	require.Zero(
		t,
		batches,
		"a stale keyed wallet must not be submitted after reconciliation failure",
	)
	require.Equal(
		t,
		1,
		wallet.Len(),
		"reconciliation failure must retain keyed wallet state",
	)
}

func TestRunStillBatchesUnsignedWalletAfterDisconnectedReconciliationClient(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wallet := NewWallet()
	wallet.Add(UTxO{TxHash: "funding", Amount: 5_000_000})
	pump := NewPump(
		&Config{TxCountMin: 1, TxCountMax: 1},
		wallet,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		time.Now().Add(-time.Second),
	)
	var batches int
	pump.dialPrimaryFn = func() (*NodeClient, error) {
		return &NodeClient{addr: "test"}, nil
	}
	pump.runBatchFn = func(context.Context, *NodeClient, int) int {
		batches++
		return 0
	}
	pump.cooldownFn = func(context.Context) bool {
		cancel()
		return false
	}

	require.ErrorIs(t, pump.Run(ctx), context.Canceled)
	require.Equal(
		t,
		1,
		batches,
		"unsigned wallets must retain their pacing-only batch path",
	)
}

func TestDeriveTestTxIDUsesCardanoTransactionBodyHash(t *testing.T) {
	txBytes, err := BuildDelegationTx(
		[]UTxO{{
			TxHash: "0000000000000000000000000000000000000000000000000000000000000001",
			Amount: 2_000_000,
		}},
		make([]byte, 28),
		make([]byte, 28),
		MinFee,
		// A payment-key-hash enterprise address: the 0x60 header and a
		// 28-byte hash. A shorter payload is rejected by any real decoder.
		append([]byte{0x60}, make([]byte, 28)...),
	)
	require.NoError(t, err)

	// gouroboros decodes the transaction the way a node does, so its hash is
	// an oracle independent of deriveTestTxID's own CBOR handling. Comparing
	// against a second Blake2b-256 of the same decoded element would restate
	// the implementation instead of checking it.
	tx, err := ledger.NewTransactionFromCbor(uint(conwayEraID), txBytes)
	require.NoError(t, err)
	require.Equal(t, tx.Hash().String(), deriveTestTxID(txBytes))
}
