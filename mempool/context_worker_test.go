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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type cancellableWorkerValidator struct {
	*mockValidator
	started chan context.Context
	release chan struct{}
}

func (v *cancellableWorkerValidator) WithTxValidationSession(ctx context.Context, _ func(
	func(gledger.Transaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo, *utxoref.StateOverlay) error,
	func() bool,
	func(func() error) (bool, error),
) error) error {
	v.started <- ctx
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-v.release:
		return nil
	}
}
func TestStopCancelsBlockedWorkerValidation(t *testing.T) {
	t.Parallel()
	v := &cancellableWorkerValidator{mockValidator: newMockValidator(), started: make(chan context.Context, 1), release: make(chan struct{})}
	m := newTestMempoolWithValidator(t, v)
	t.Cleanup(func() { close(v.release); require.NoError(t, m.Stop(context.Background())); m.eventBus.Close() })
	testutil.WaitForCondition(t, func() bool { return m.eventBus.HasSubscribers(chain.ChainUpdateEventType) }, time.Second, "chain worker subscribes")
	m.eventBus.Publish(chain.ChainUpdateEventType, event.Event{})
	workerCtx := testutil.RequireReceive(t, v.started, time.Second, "worker enters validation")
	stopCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx))
	require.ErrorIs(t, workerCtx.Err(), context.Canceled)
}

func TestChainUpdateWorkerRateLimitsFailedRebuilds(t *testing.T) {
	t.Parallel()
	const revalidationInterval = 100 * time.Millisecond
	type rebuildAttempt struct {
		number int
		at     time.Time
	}
	ctx, cancel := context.WithCancel(context.Background())
	updates := make(chan event.Event, 8)
	attempts := make(chan rebuildAttempt, 4)
	workerDone := make(chan struct{})
	calls := 0
	go func() {
		defer close(workerDone)
		runChainUpdateWorker(
			ctx,
			nil,
			updates,
			revalidationInterval,
			nil,
			func() error {
				calls++
				select {
				case attempts <- rebuildAttempt{number: calls, at: time.Now()}:
				case <-ctx.Done():
					return ctx.Err()
				}
				return errors.New("ledger snapshot changed")
			},
			func(error) {},
		)
	}()
	t.Cleanup(func() {
		cancel()
		<-workerDone
	})

	updates <- event.Event{}
	first := testutil.RequireReceive(t, attempts, time.Second, "first rebuild")
	require.Equal(t, 1, first.number)
	updates <- event.Event{}
	updates <- event.Event{}
	testutil.WaitForCondition(
		t,
		func() bool { return len(updates) == 0 },
		time.Second,
		"events coalesce during the failed rebuild cooldown",
	)
	second := testutil.RequireReceive(t, attempts, time.Second, "cooldown retry")
	require.Equal(t, 2, second.number)
	require.GreaterOrEqual(t, second.at.Sub(first.at), revalidationInterval,
		"failed rebuild is not retried before its cooldown expires")

	third := testutil.RequireReceive(t, attempts, time.Second, "scheduled retry")
	require.Equal(t, 3, third.number)
	require.GreaterOrEqual(t, third.at.Sub(second.at), revalidationInterval,
		"repeated failed rebuild remains rate limited")
}
