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
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// duringElectionLeader runs hook inside the leader check: after the forge
// attempt has selected its credential snapshot and before it signs, the window
// in which a reload lands in production.
type duringElectionLeader struct{ hook func() }

func (l duringElectionLeader) ShouldProduceBlock(uint64) bool {
	l.hook()
	return true
}

func (duringElectionLeader) NextLeaderSlot(from uint64) (uint64, bool) {
	return from, true
}

// TestCredentialSwapDuringALeaderSlotDoesNotLoseTheSlot drives a whole forge
// attempt with the credentials replaced mid-attempt. A validated swap must
// leave the slot's block forged and adopted; Close, which also advances the
// generation, is the control that shows the harness can observe a lost slot.
func TestCredentialSwapDuringALeaderSlotDoesNotLoseTheSlot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		hook        func(live *PoolCredentials, next *PoolCredentials)
		wantAdopted float64
		wantMissed  float64
	}{
		{
			name: "validated swap",
			hook: func(live, next *PoolCredentials) {
				if err := live.ReplaceWith(next); err != nil {
					panic(err)
				}
			},
			wantAdopted: 1,
		},
		{
			name:       "close is the control that does lose it",
			hook:       func(live, _ *PoolCredentials) { live.Close() },
			wantMissed: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newCredentialsRotationFixture(t)
			live := fixture.validated(t, 1)
			next := fixture.validated(t, 2)
			broadcaster := &forgerTestBroadcaster{}
			forger, err := NewBlockForger(ForgerConfig{
				Mode:        ModeProduction,
				Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
				Credentials: live,
				LeaderChecker: duringElectionLeader{
					hook: func() { tc.hook(live, next) },
				},
				BlockBuilder: &forgerTestBuilder{
					block: newForgerTestBlock(10, 2),
				},
				BlockBroadcaster: broadcaster,
				SlotClock: forgerTestSlotClock{
					currentSlot:       10,
					chainTipSlot:      9,
					slotsPerKESPeriod: 100,
				},
				PromRegistry: prometheus.NewRegistry(),
			})
			require.NoError(t, err)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Equal(
				t,
				tc.wantAdopted,
				testutil.ToFloat64(forger.metrics.forgeAdopted),
			)
			require.Equal(
				t,
				tc.wantMissed,
				testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
			)
		})
	}
}

// TestReplaceWithNeverExposesATornSnapshot swaps between certificates that
// differ in counter and start period while readers snapshot the credentials.
// Every snapshot must pair a certificate with the lifetime derived from that
// same certificate; a swap that published fields one at a time would let a
// reader see a counter from one certificate and a window from the other.
func TestReplaceWithNeverExposesATornSnapshot(t *testing.T) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)
	genesis := synthGenesis(100, 62, time.Second, time.Unix(0, 0))

	build := func(counter uint64) *PoolCredentials {
		pc := NewPoolCredentials()
		require.NoError(t, pc.LoadFromFiles(
			fixture.vrfPath,
			fixture.kesPath,
			fixture.opCert(t, counter, counter%2),
		))
		require.NoError(t, pc.ValidateOpCert())
		require.NoError(t, pc.ValidateKESPeriod(genesis, 100))
		return pc
	}
	const swaps = 60
	live := build(1)
	nexts := make([]*PoolCredentials, 0, swaps)
	for counter := uint64(2); counter < 2+swaps; counter++ {
		nexts = append(nexts, build(counter))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	torn := make(chan string, 1)
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := live.acquireCredentialGeneration()
				cert := snap.operationalCert
				if snap.opCertStartKES != cert.KESPeriod ||
					snap.opCertExpiryKES != cert.KESPeriod+62 {
					select {
					case torn <- "lifetime does not belong to the certificate":
					default:
					}
				}
				snap.release()
			}
		})
	}
	for _, next := range nexts {
		require.NoError(t, live.ReplaceWith(next))
	}
	close(stop)
	wg.Wait()
	select {
	case msg := <-torn:
		require.Fail(t, "torn snapshot", msg)
	default:
	}
	require.Equal(t, uint64(1+swaps), live.GetOpCert().IssueNumber)
}
