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
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
