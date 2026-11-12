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

package forging

import (
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// preparingRemoteKESSigner is a fakeRemoteKESSigner that also implements
// RemoteKESPreparer and counts Prepare calls.
type preparingRemoteKESSigner struct {
	fakeRemoteKESSigner
	prepares   atomic.Int32
	prepareErr error
}

func (p *preparingRemoteKESSigner) Prepare() error {
	p.prepares.Add(1)
	return p.prepareErr
}

// singleLeaderSlot leads exactly one slot.
type singleLeaderSlot uint64

func (s singleLeaderSlot) ShouldProduceBlock(slot uint64) bool {
	return slot == uint64(s)
}

func (s singleLeaderSlot) NextLeaderSlot(from uint64) (uint64, bool) {
	if from <= uint64(s) {
		return uint64(s), true
	}
	return 0, false
}

func newRemotePrepareCreds(
	t *testing.T,
	signer RemoteKESSigner,
) *PoolCredentials {
	t.Helper()
	vrfPath, _, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	t.Cleanup(pc.Close)
	require.NoError(t, pc.LoadFromAgentSign(vrfPath, opCertPath, signer))
	return pc
}

func TestForgerPreparesRemoteSignerOnceBeforeLeaderSlot(t *testing.T) {
	t.Parallel()
	signer := &preparingRemoteKESSigner{}
	f := &BlockForger{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		creds:         newRemotePrepareCreds(t, signer),
		leaderChecker: singleLeaderSlot(103),
		slotDuration:  time.Second,
	}

	// 3s lead at 1s slots: slot 99 is too early.
	f.prepareRemoteSignerForUpcomingLeaderSlot(99)
	require.Never(t, func() bool { return signer.prepares.Load() > 0 },
		50*time.Millisecond, 5*time.Millisecond)

	f.prepareRemoteSignerForUpcomingLeaderSlot(100)
	require.Eventually(t, func() bool { return signer.prepares.Load() == 1 },
		2*time.Second, 5*time.Millisecond)

	// The same leader slot is prepared once, and the leader slot itself
	// prepares nothing: the sign there opens a session if it must.
	f.prepareRemoteSignerForUpcomingLeaderSlot(101)
	f.prepareRemoteSignerForUpcomingLeaderSlot(102)
	f.prepareRemoteSignerForUpcomingLeaderSlot(103)
	require.Never(t, func() bool { return signer.prepares.Load() > 1 },
		50*time.Millisecond, 5*time.Millisecond)
}

func TestPrepareRemoteSigner(t *testing.T) {
	t.Parallel()
	t.Run("signer without Prepare", func(t *testing.T) {
		t.Parallel()
		pc := newRemotePrepareCreds(t, &fakeRemoteKESSigner{})
		prepared, err := pc.PrepareRemoteSigner()
		require.NoError(t, err)
		require.False(t, prepared)
	})
	t.Run("local keys", func(t *testing.T) {
		t.Parallel()
		pc := NewPoolCredentials()
		t.Cleanup(pc.Close)
		prepared, err := pc.PrepareRemoteSigner()
		require.NoError(t, err)
		require.False(t, prepared)
	})
	t.Run("prepare failure", func(t *testing.T) {
		t.Parallel()
		want := errors.New("agent unreachable")
		pc := newRemotePrepareCreds(
			t,
			&preparingRemoteKESSigner{prepareErr: want},
		)
		prepared, err := pc.PrepareRemoteSigner()
		require.True(t, prepared)
		require.ErrorIs(t, err, want)
	})
}
