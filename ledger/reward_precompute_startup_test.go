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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestLedgerStateStartQueuesStartupRewardPrecompute(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	nonce := []byte{0x30, 0x93, 0x65, 0x6a}
	require.NoError(t, db.SetEpoch(
		0, 0, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 7,
		ProtocolMinor: 0,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.ShelleyEraDesc.Id, nil,
	))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ls.Close()
	})

	// Occupy the precompute worker slot before Start. queueRewardPrecompute
	// hands the event to a worker goroutine that clears
	// rewardPrecomputePending under the same mutex, so a worker that reaches
	// the mutex before the hook leaves nothing for the hook to observe. With
	// the slot taken the queued round stays pending, which is what Start owes
	// the in-progress epoch.
	ls.rewardPrecomputeMu.Lock()
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputeMu.Unlock()

	startupQueued := make(chan struct{})
	ls.startupRewardPrecomputeHook = func() {
		ls.rewardPrecomputeMu.Lock()
		pending := ls.rewardPrecomputePending
		var queued event.EpochTransitionEvent
		if pending != nil {
			queued = *pending
		}
		ls.rewardPrecomputeMu.Unlock()
		require.NotNil(
			t, pending, "Start must queue the established current epoch",
		)
		require.Equal(t, uint64(0), queued.NewEpoch)
		require.Equal(t, nonce, queued.EpochNonce)
		close(startupQueued)
	}

	_ = ls.Start(t.Context())
	testutil.RequireReceive(t, startupQueued, 2*time.Second, "startup precompute queued")
}

// The EventBus subscription that drives the reward precompute only fires at an
// epoch boundary, so an epoch already in progress when the process starts has
// no event to carry it. Startup must queue that round itself, or the next
// boundary calculates it inline inside the rollover write transaction.
func TestQueueStartupRewardPrecomputeQueuesInProgressEpoch(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentEpoch = models.Epoch{
		EpochId:       655,
		StartSlot:     197596800,
		LengthInSlots: 432000,
		Nonce:         []byte{0x30, 0x93, 0x65, 0x6a},
	}

	queued := make(chan event.EpochTransitionEvent, 1)
	ls.queueStartupRewardPrecomputeWith(
		func(evt event.EpochTransitionEvent) error {
			queued <- evt
			return nil
		},
	)

	evt := testutil.RequireReceive(
		t, queued, 2*time.Second, "startup precompute queued",
	)
	// precomputeStakeRewardsAfterEpochTransition derives the application epoch
	// as NewEpoch+1 and uses BoundarySlot as the capture slot, so these two
	// fields are what decide which round gets precomputed.
	require.Equal(t, uint64(655), evt.NewEpoch)
	require.Equal(t, uint64(197596800), evt.BoundarySlot)
	require.Equal(t, uint64(654), evt.PreviousEpoch)
	require.Equal(t, uint64(197596799), evt.SnapshotSlot)
	require.Equal(t, ls.currentEpoch.Nonce, evt.EpochNonce)
}

// A nonce-less or zero-length epoch is one that was never established, so there
// is no round to catch up and nothing should be queued. queueRewardPrecompute
// also drops an event without a nonce, so queueing one would spawn a worker
// that immediately does nothing.
func TestQueueStartupRewardPrecomputeSkipsUnestablishedEpoch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		epoch models.Epoch
	}{
		{
			name: "no length",
			epoch: models.Epoch{
				EpochId: 655,
				Nonce:   []byte{0x01},
			},
		},
		{
			name: "no nonce",
			epoch: models.Epoch{
				EpochId:       655,
				LengthInSlots: 432000,
			},
		},
		{
			name:  "zero value",
			epoch: models.Epoch{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{}
			ls.currentEpoch = tc.epoch

			queued := make(chan event.EpochTransitionEvent, 1)
			ls.queueStartupRewardPrecomputeWith(
				func(evt event.EpochTransitionEvent) error {
					queued <- evt
					return nil
				},
			)

			testutil.RequireNoReceive(
				t, queued, 100*time.Millisecond,
				"unestablished epoch must not queue a precompute",
			)
		})
	}
}

// Epoch 0 has no predecessor and starts at slot 0; neither derived field may
// underflow into a bogus epoch or slot.
func TestQueueStartupRewardPrecomputeHandlesEpochZero(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentEpoch = models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 432000,
		Nonce:         []byte{0x01},
	}

	queued := make(chan event.EpochTransitionEvent, 1)
	ls.queueStartupRewardPrecomputeWith(
		func(evt event.EpochTransitionEvent) error {
			queued <- evt
			return nil
		},
	)

	evt := testutil.RequireReceive(
		t, queued, 2*time.Second, "startup precompute queued",
	)
	require.Equal(t, uint64(0), evt.NewEpoch)
	require.Equal(t, uint64(0), evt.PreviousEpoch)
	require.Equal(t, uint64(0), evt.SnapshotSlot)
	require.Equal(t, uint64(0), evt.BoundarySlot)
}
