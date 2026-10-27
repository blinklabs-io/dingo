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

type progressProbe struct {
	pg                *PeerGovernor
	slotCalls         atomic.Int32
	progressCalls     atomic.Int32
	calledWhileLocked atomic.Bool
}

func (pr *progressProbe) checkUnlocked(calls *atomic.Int32) {
	calls.Add(1)
	if !pr.pg.mu.TryLock() {
		pr.calledWhileLocked.Store(true)
		return
	}
	pr.pg.mu.Unlock()
}

func newProgressProbe(
	peers ...*Peer,
) *progressProbe {
	pr := &progressProbe{}
	pr.pg = NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		LedgerPeerProvider: &lockCheckingLedgerPeerProvider{
			check: func() { pr.checkUnlocked(&pr.slotCalls) },
		},
		UseLedgerAfterSlot: 1,
		SyncProgressProvider: syncProgressProviderFunc(func() float64 {
			pr.checkUnlocked(&pr.progressCalls)
			return 0
		}),
		SyncProgressForExit: 0.95,
	})
	pr.pg.mu.Lock()
	pr.pg.peers = append(pr.pg.peers, peers...)
	pr.pg.mu.Unlock()
	return pr
}

func warmBootstrapPeer() *Peer {
	return &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	}
}

func TestReconcileReadsBootstrapProgressOutsidePeerGovernorLock(t *testing.T) {
	pr := newProgressProbe(warmBootstrapPeer())

	pr.pg.reconcile(context.Background())

	require.GreaterOrEqual(t, pr.slotCalls.Load(), int32(1))
	require.Equal(t, int32(1), pr.progressCalls.Load())
	require.False(t, pr.calledWhileLocked.Load())
}

func TestReconcileSkipsBootstrapProgressWithoutBootstrapPeers(t *testing.T) {
	pr := newProgressProbe()

	pr.pg.reconcile(context.Background())

	require.Zero(t, pr.progressCalls.Load())
}

func TestReconcileSkipsBootstrapProgressAfterBootstrapExit(t *testing.T) {
	pr := newProgressProbe(warmBootstrapPeer())
	pr.pg.mu.Lock()
	pr.pg.bootstrapExited = true
	pr.pg.mu.Unlock()

	pr.pg.reconcile(context.Background())

	require.Zero(t, pr.progressCalls.Load())
}
