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

package peergov

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type syncProgressProviderFunc func() float64

func (f syncProgressProviderFunc) SyncProgress() float64 {
	return f()
}

type lockCheckingLedgerPeerProvider struct {
	mockLedgerPeerProvider
	check func()
}

func (p *lockCheckingLedgerPeerProvider) CurrentSlot() uint64 {
	p.check()
	return 0
}

func TestReconcileReadsBootstrapProgressOutsidePeerGovernorLock(t *testing.T) {
	var pg *PeerGovernor
	var slotCalls atomic.Int32
	var progressCalls atomic.Int32
	var calledWhileLocked atomic.Bool
	checkUnlocked := func(calls *atomic.Int32) {
		calls.Add(1)
		if !pg.mu.TryLock() {
			calledWhileLocked.Store(true)
			return
		}
		pg.mu.Unlock()
	}
	ledgerProvider := &lockCheckingLedgerPeerProvider{
		check: func() { checkUnlocked(&slotCalls) },
	}
	progressProvider := syncProgressProviderFunc(func() float64 {
		checkUnlocked(&progressCalls)
		return 0
	})
	pg = NewPeerGovernor(PeerGovernorConfig{
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		LedgerPeerProvider:   ledgerProvider,
		UseLedgerAfterSlot:   1,
		SyncProgressProvider: progressProvider,
		SyncProgressForExit:  0.95,
	})

	pg.reconcile(context.Background())

	require.GreaterOrEqual(t, slotCalls.Load(), int32(1))
	require.Equal(t, int32(1), progressCalls.Load())
	require.False(t, calledWhileLocked.Load())
}
