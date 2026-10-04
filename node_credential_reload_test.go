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

package dingo

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// reloadTestNode is a block producer whose operational certificate file the
// test can replace in place, the way a mounted Secret is rotated.
type reloadTestNode struct {
	*Node
	opcertPath string
	coldVKey   ed25519.PublicKey
	coldSKey   ed25519.PrivateKey
	logs       *syncedLogBuffer
	live       *forging.PoolCredentials
}

// syncedLogBuffer is a goroutine-safe log sink.
type syncedLogBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newReloadTestNode(t *testing.T, counter uint64) *reloadTestNode {
	t.Helper()
	vrf, kes, opcertPath := devnetCredPaths(t)
	coldVKey, coldSKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	r := &reloadTestNode{
		opcertPath: opcertPath,
		coldVKey:   coldVKey,
		coldSKey:   coldSKey,
		logs:       &syncedLogBuffer{},
	}
	r.Node = newTestNodeForBP(
		t, true, vrf, kes, opcertPath,
		shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour)),
	)
	r.config.logger = slog.New(slog.NewJSONHandler(r.logs, nil))
	r.rotateTo(t, counter, nil)
	live, err := r.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)
	r.live = live
	r.blockProducerCreds.Store(live)
	return r
}

// rotateTo replaces the configured certificate file with one carrying
// counter, signed by the pool's cold key.
func (r *reloadTestNode) rotateTo(
	t *testing.T,
	counter uint64,
	kesPeriod *uint64,
) {
	t.Helper()
	fresh := opCertFixtureForColdKey(
		t,
		r.coldVKey,
		r.coldSKey,
		counter,
		kesPeriod,
	)
	data, err := os.ReadFile(fresh)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(r.opcertPath, data, 0o644))
}

func (r *reloadTestNode) reload(
	ledgerCheck func(*forging.PoolCredentials) error,
) error {
	if ledgerCheck == nil {
		ledgerCheck = func(*forging.PoolCredentials) error { return nil }
	}
	return r.reloadBlockProducerCredentials(0, true, ledgerCheck)
}

func (r *reloadTestNode) requireLiveCounter(t *testing.T, want uint64) {
	t.Helper()
	require.True(t, r.live.IsLoaded())
	require.Equal(t, want, r.live.GetOpCert().IssueNumber)
	require.NotZero(t, r.live.OpCertExpiryPeriod())
}

func TestReloadBlockProducerCredentialsRotatesTheLiveCredentials(t *testing.T) {
	t.Parallel()
	r := newReloadTestNode(t, 1)
	r.rotateTo(t, 2, nil)

	require.NoError(t, r.reload(nil))

	r.requireLiveCounter(t, 2)
	var reloaded map[string]any
	for line := range strings.SplitSeq(r.logs.String(), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil &&
			rec["msg"] == "block producer credentials reloaded" {
			reloaded = rec
		}
	}
	require.NotNil(t, reloaded, "a successful reload must be logged")
	require.EqualValues(t, 1, reloaded["old_opcert_counter"])
	require.EqualValues(t, 2, reloaded["new_opcert_counter"])
	require.Contains(t, reloaded, "old_opcert_kes_period")
	require.Contains(t, reloaded, "new_opcert_kes_period")
}

func TestReloadBlockProducerCredentialsRefusesClockOutsideConfirmedHistory(
	t *testing.T,
) {
	t.Parallel()
	for _, period := range []uint64{0, 5} {
		t.Run(fmt.Sprintf("start period %d", period), func(t *testing.T) {
			t.Parallel()
			r := newReloadTestNode(t, 1)
			r.rotateTo(t, 2, &period)
			require.ErrorContains(t, r.reloadBlockProducerCredentials(
				100000000, false,
				func(*forging.PoolCredentials) error { return nil },
			), "confirmed era history")
			r.requireLiveCounter(t, 1)
			require.NotContains(t, r.logs.String(), "credentials reloaded")
		})
	}
}

func TestReloadBlockProducerCredentialsExportedEntryRotates(t *testing.T) {
 t.Parallel()
 start := time.Now().Add(-time.Minute)
 started := newStartupCleanupProducerNodeWithGenesisStart(t, &start)
 r := newReloadTestNode(t, 1)
 r.ledgerState = started.ledgerState
 r.rotateTo(t, 2, nil)
 require.NoError(t, r.ReloadBlockProducerCredentials())
 r.requireLiveCounter(t, 2)
}

func TestReloadBlockProducerCredentialsExportedEntryRefusesUnconfirmedClock(t *testing.T) {
	t.Parallel()
	started := newStartupCleanupProducerNode(t)
	r := newReloadTestNode(t, 1)
	r.ledgerState = started.ledgerState
	r.rotateTo(t, 2, nil)
	require.ErrorContains(t, r.ReloadBlockProducerCredentials(), "confirmed era history")
	r.requireLiveCounter(t, 1)
}

// TestReloadBlockProducerCredentialsRejectsAndKeepsTheLoadedOnes covers every
// way a reload can be refused. Each leaves the previously loaded credentials
// loaded, validated and unchanged: a failed reload must never downgrade a
// producer that is forging.
func TestReloadBlockProducerCredentialsRejectsAndKeepsTheLoadedOnes(
	t *testing.T,
) {
	t.Parallel()

	futurePeriod := uint64(5)
	for _, tc := range []struct {
		name        string
		rotate      func(t *testing.T, r *reloadTestNode)
		ledgerCheck func(r *reloadTestNode) func(*forging.PoolCredentials) error
		wantErr     string
	}{
		{
			name: "lower counter",
			rotate: func(t *testing.T, r *reloadTestNode) {
				r.rotateTo(t, 2, nil)
			},
			wantErr: "below the loaded counter",
		},
		{
			name: "KES period out of the window",
			rotate: func(t *testing.T, r *reloadTestNode) {
				r.rotateTo(t, 4, &futurePeriod)
			},
			wantErr: "in the future",
		},
		{
			name: "counter the ledger refuses",
			rotate: func(t *testing.T, r *reloadTestNode) {
				r.rotateTo(t, 4, nil)
			},
			ledgerCheck: func(r *reloadTestNode) func(*forging.PoolCredentials) error {
				return func(creds *forging.PoolCredentials) error {
					return r.validateBlockProducerLedgerWithViewAtSlot(
						creds,
						opCertSeqLedgerView{
							regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
							latestSeq:  9,
						},
						nil,
						0,
					)
				}
			},
			wantErr: "below last seen",
		},
		{
			name: "unreadable certificate",
			rotate: func(t *testing.T, r *reloadTestNode) {
				require.NoError(t, os.WriteFile(r.opcertPath, []byte("{"), 0o644))
			},
			wantErr: "load pool credentials",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newReloadTestNode(t, 3)
			tc.rotate(t, r)
			var check func(*forging.PoolCredentials) error
			if tc.ledgerCheck != nil {
				check = tc.ledgerCheck(r)
			}

			require.ErrorContains(t, r.reload(check), tc.wantErr)

			r.requireLiveCounter(t, 3)
			require.NotContains(t, r.logs.String(), "credentials reloaded")
		})
	}
}

func TestReloadBlockProducerCredentialsRefusesWhenNotApplicable(t *testing.T) {
	t.Parallel()

	t.Run("no block producer running", func(t *testing.T) {
		t.Parallel()
		r := newReloadTestNode(t, 1)
		r.blockProducerCreds.Store(nil)
		require.ErrorContains(t, r.reload(nil), "not loaded")
	})

	t.Run("credentials sourced from a KES agent", func(t *testing.T) {
		t.Parallel()
		r := newReloadTestNode(t, 1)
		r.rotateTo(t, 2, nil)
		r.config.shelleyKESAgentSocket = "/run/kes-agent.sock"
		require.ErrorContains(
			t,
			r.reload(nil),
			"rotate the key through the agent",
		)
		r.requireLiveCounter(t, 1)
	})
}

// TestQuiesceZeroizesTheLiveCredentialsAfterTheirUsers pins the teardown
// contract: the live credentials are closed by the same stop sequence that
// quiesces the forger, and only after the forger, the KES agent client and the
// leader election, all of which read from or install into them.
func TestQuiesceZeroizesTheLiveCredentialsAfterTheirUsers(t *testing.T) {
	t.Parallel()
	r := newReloadTestNode(t, 1)
	r.Node.leaderElection = &leader.Election{}
	r.Node.blockForger = &forging.BlockForger{}

	var names []string
	var credStop func() error
	for _, stop := range r.quiesceComponentStops() {
		names = append(names, stop.name)
		if stop.name == "block producer credentials" {
			credStop = stop.stop
		}
	}
	require.NotNil(t, credStop, "stops: %v", names)
	require.Equal(
		t,
		[]string{
			"block forger",
			"leader election",
			"block producer credentials",
		},
		names,
	)

	seed := r.live.GetVRFSKey()
	require.NotEmpty(t, seed)
	require.NoError(t, credStop())

	require.False(t, r.live.IsLoaded())
	require.Nil(t, r.live.GetVRFSKey())
	require.Nil(t, r.blockProducerCreds.Load())
	require.Error(
		t,
		r.reload(nil),
		"a reload arriving after teardown must not resurrect the keys",
	)
}

// TestReloadBlockProducerCredentialsRefusesDuringALifecycleOperation pins that
// the signal-driven reload does not read the ledger state or the live
// credentials while startup, shutdown, or a live restore or truncate is
// replacing them: it runs on its own goroutine, and a restore nils
// n.ledgerState under liveLifecycleMu.
func TestReloadBlockProducerCredentialsRefusesDuringALifecycleOperation(
	t *testing.T,
) {
	t.Parallel()

	gates := map[string]func(*Node) *sync.Mutex{
		"startup or shutdown": func(n *Node) *sync.Mutex {
			return &n.startupLifecycleMu
		},
		"live restore or truncate": func(n *Node) *sync.Mutex {
			return &n.liveLifecycleMu
		},
	}
	for name, gate := range gates {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := newReloadTestNode(t, 1)
			r.rotateTo(t, 2, nil)
			mu := gate(r.Node)
			mu.Lock()
			defer mu.Unlock()
			require.ErrorIs(
				t,
				r.ReloadBlockProducerCredentials(),
				errLifecycleBusy,
			)
			r.requireLiveCounter(t, 1)
		})
	}
}
