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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
	"github.com/stretchr/testify/require"
)

func TestConfigPopulateDMQNetworkMagic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		dmq       internalconfig.DMQConfig
		network   string
		wantMagic uint32
		wantErr   string
	}{
		{
			name:    "disabled needs no magic",
			dmq:     internalconfig.DMQConfig{Topic: "mithril"},
			network: "devnet",
		},
		{
			name: "mithril mainnet",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network:   "mainnet",
			wantMagic: 2912307721,
		},
		{
			name: "mithril preprod",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network:   "preprod",
			wantMagic: 2147483649,
		},
		{
			name: "mithril preview",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network:   "preview",
			wantMagic: 2147483650,
		},
		{
			name: "explicit magic wins",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril", NetworkMagic: 42,
			},
			network:   "mainnet",
			wantMagic: 42,
		},

		{
			name: "network without a default",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network: "devnet",
			wantErr: "set dmq.networkMagic",
		},
		{
			name: "unknown topic",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "other",
			},
			network: "mainnet",
			wantErr: `no network magic for topic "other"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := NewConfig()
			cfg.cfg.Network = tt.network
			cfg.cfg.DMQ = tt.dmq
			n := &Node{config: cfg}
			err := n.configPopulateDMQNetworkMagic()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantMagic, n.config.cfg.DMQ.NetworkMagic)
		})
	}
}

func TestDMQStakeAuthorityWithoutLedger(t *testing.T) {
	t.Parallel()
	_, err := (&dmqStakeAuthority{}).PoolActiveStake(
		ocommon.PoolKeyHash{},
	)
	require.ErrorContains(t, err, "ledger state unavailable")
}

// A live truncate closes and replaces the ledger state while DMQ submissions
// keep looking up pool stake; the lookups must never touch a ledger state
// being swapped, and must follow the rebuilt one afterwards.
func TestDMQStakeAuthorityAcrossLiveTruncate(t *testing.T) {
	t.Parallel()
	const numBlocks = 6
	n, points := newLiveLifecycleTestNode(t, numBlocks)
	auth := &n.dmqStake
	auth.setLedgerState(n.ledgerState)
	stop := make(chan struct{})
	started := make(chan struct{})
	var lookups atomic.Int64
	var wg sync.WaitGroup
	// Only the detached state may fail a lookup: any other error means a
	// lookup reached storage that was closed or not yet reopened.
	var lookupErr error
	wg.Go(func() {
		for {
			_, err := auth.PoolActiveStake(ocommon.PoolKeyHash{})
			if err != nil && err.Error() != "ledger state unavailable" {
				lookupErr = err
				return
			}
			if lookups.Add(1) == 1 {
				close(started)
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	})
	<-started
	targetSlot := points[numBlocks/2].Slot
	before := lookups.Load()
	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	during := lookups.Load() - before
	close(stop)
	wg.Wait()
	require.NoError(t, err)
	require.NoError(t, lookupErr)
	require.Positive(t, during, "no stake lookup ran during the truncate")
	_, err = auth.PoolActiveStake(ocommon.PoolKeyHash{})
	require.NoError(t, err)
}

// Quiesce must not wait without bound on a stake lookup stalled inside the
// ledger: a lookup that never returns may still be reading the storage about
// to be closed, so it escalates like any other unconfirmed drain.
func TestQuiesceBoundsDMQStakeDetach(t *testing.T) {
	t.Parallel()
	n := &Node{}
	n.dmqStake.setLedgerState(&ledger.LedgerState{})
	n.config.cfg = &internalconfig.Config{ShutdownTimeout: "20ms"}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	// A lookup in flight holds the read side for as long as it runs.
	n.dmqStake.mu.RLock()
	t.Cleanup(n.dmqStake.mu.RUnlock)

	done := make(chan error, 1)
	go func() { done <- n.quiesceForLiveLifecycleOp(context.Background()) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("quiesce waited on a stalled dmq stake lookup without bound")
	}
	require.ErrorIs(t, err, errStorageDrainUnconfirmed)
	require.ErrorContains(t, err, "dmq stake lookups")
}

// Run starts the DMQ stack before the API providers, so a provider starting
// can submit through the socket the running node serves and see the node's
// own admission checks answer.
func TestNodeRunServesDMQSubmission(t *testing.T) {
	t.Parallel()
	n := newAPIPluginRuntimeNode(t)
	const magic = 42
	socket := filepath.Join(t.TempDir(), "dmq.sock")
	dmqCfg := internalconfig.DefaultDMQConfig()
	dmqCfg.Enabled = true
	dmqCfg.NetworkMagic = magic
	dmqCfg.SocketPath = socket
	n.config.cfg.DMQ = dmqCfg

	errProbe := errors.New("probe done")
	var reason ocommon.RejectReason
	probe := &apiLifecycleProbe{onStart: func() error {
		reason = submitDMQ(t, socket, magic)
		return errProbe
	}}
	registerAPIProbe(t, n.pluginHost, plugin.CapabilityAPIBlockfrost, "probe", probe)
	selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)

	require.ErrorIs(t, n.Run(context.Background()), errProbe)
	require.Equal(t, int32(1), probe.starts.Load())
	// The node's own ledger answers the KES period lookup: an empty chain's
	// confirmed history does not reach the wall clock, so admission fails
	// closed before authentication.
	require.Equal(
		t,
		ocommon.OtherReason{
			Message: "confirmed era history does not cover wall clock",
		},
		reason,
	)
}

// syncLogBuffer is a log sink that node goroutines may write while the test
// reads it.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A topology file that cannot be loaded stops startup before the DMQ stack
// opens its socket and before any API provider starts.
func TestNodeRunRejectsBadDMQTopology(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path func(t *testing.T, dir string) string
	}{
		{
			name: "malformed",
			path: func(t *testing.T, dir string) string {
				path := filepath.Join(dir, "dmq-topology.json")
				require.NoError(t, os.WriteFile(path, []byte("{"), 0o600))
				return path
			},
		},
		{
			name: "missing",
			path: func(_ *testing.T, dir string) string {
				return filepath.Join(dir, "absent.json")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n := newAPIPluginRuntimeNode(t)
			dir := t.TempDir()
			socket := filepath.Join(dir, "dmq.sock")
			dmqCfg := internalconfig.DefaultDMQConfig()
			dmqCfg.Enabled = true
			dmqCfg.NetworkMagic = 42
			dmqCfg.SocketPath = socket
			dmqCfg.TopologyFile = tt.path(t, dir)
			n.config.cfg.DMQ = dmqCfg
			probe := &apiLifecycleProbe{}
			registerAPIProbe(
				t, n.pluginHost, plugin.CapabilityAPIBlockfrost, "probe", probe,
			)
			selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)

			err := n.Run(context.Background())
			require.ErrorContains(t, err, "loading dmq topology")
			require.NoFileExists(t, socket)
			require.Zero(t, probe.starts.Load())
		})
	}
}

// A topology file that loads lets startup continue, and the log reports how
// many distinct peers it names. A file that names none is accepted with a
// warning, because ledger peer discovery can supply peers without one.
func TestNodeRunLoadsDMQTopology(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		content  string
		wantPeer string
		wantWarn bool
	}{
		{
			name: "peers",
			content: `{
				"localRoots": [{
					"accessPoints": [
						{"address": "10.0.0.1", "port": 3141},
						{"address": "10.0.0.2", "port": 3141}
					],
					"valency": 2
				}],
				"publicRoots": [{
					"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
					"valency": 1
				}]
			}`,
			wantPeer: "peers=2",
		},
		{
			// The topology the Mithril documentation publishes: a valency of
			// 2 over one access point, and a null peerSnapshotFile.
			name: "mithril documented topology",
			content: `{
				"bootstrapPeers": [],
				"localRoots": [{
					"accessPoints": [
						{"address": "dmq.example.com", "port": 6161, "valency": 1}
					],
					"advertise": true,
					"trustable": false,
					"valency": 2
				}],
				"peerSnapshotFile": null,
				"publicRoots": [{"accessPoints": [], "advertise": false}]
			}`,
			wantPeer: "peers=1",
		},
		{
			name:     "no peers",
			content:  `{}`,
			wantPeer: "peers=0",
			wantWarn: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runNodeWithDMQTopology(t, tt.content, tt.wantPeer, tt.wantWarn)
		})
	}
}

func runNodeWithDMQTopology(
	t *testing.T,
	content, wantPeer string,
	wantWarn bool,
) {
	t.Helper()
	n := newAPIPluginRuntimeNode(t)
	dir := t.TempDir()
	topologyFile := filepath.Join(dir, "dmq-topology.json")
	require.NoError(t, os.WriteFile(topologyFile, []byte(content), 0o600))
	dmqCfg := internalconfig.DefaultDMQConfig()
	dmqCfg.Enabled = true
	dmqCfg.NetworkMagic = 42
	// A Unix socket path is limited to about 100 bytes, which a subtest's
	// t.TempDir path can exceed, so the socket gets a short directory.
	socketDir, err := os.MkdirTemp("", "dmq")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	dmqCfg.SocketPath = filepath.Join(socketDir, "dmq.sock")
	dmqCfg.TopologyFile = topologyFile
	n.config.cfg.DMQ = dmqCfg
	logs := &syncLogBuffer{}
	n.config.logger = slog.New(slog.NewTextHandler(logs, nil))

	errProbe := errors.New("probe done")
	probe := &apiLifecycleProbe{onStart: func() error { return errProbe }}
	registerAPIProbe(
		t, n.pluginHost, plugin.CapabilityAPIBlockfrost, "probe", probe,
	)
	selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)

	require.ErrorIs(t, n.Run(context.Background()), errProbe)
	require.Equal(t, int32(1), probe.starts.Load())
	require.Contains(t, logs.String(), "loaded dmq topology")
	require.Contains(t, logs.String(), wantPeer)
	if wantWarn {
		require.Contains(t, logs.String(), "dmq topology names no peers")
	} else {
		require.NotContains(t, logs.String(), "dmq topology names no peers")
	}
}

// submitDMQ submits an unsigned message over a DMQ local socket and returns
// the server's reject reason, or nil if it was accepted.
func submitDMQ(
	t *testing.T,
	socket string,
	magic uint32,
) ocommon.RejectReason {
	t.Helper()
	verdict := make(chan ocommon.RejectReason, 1)
	conn, err := ouroboros.NewConnection(
		ouroboros.WithDMQ(true),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithLocalMessageSubmissionConfig(
			localmessagesubmission.NewConfig(
				localmessagesubmission.WithAuthenticator(
					ocommon.NewNoOpAuthenticator(nil),
				),
				localmessagesubmission.WithTTLValidator(
					ocommon.NewNoOpTTLValidator(nil),
				),
				localmessagesubmission.WithAcceptMessageFunc(
					func(localmessagesubmission.CallbackContext) {
						verdict <- nil
					},
				),
				localmessagesubmission.WithRejectMessageFunc(
					func(
						_ localmessagesubmission.CallbackContext,
						reason ocommon.RejectReason,
					) {
						verdict <- reason
					},
				),
			),
		),
	)
	require.NoError(t, err)
	require.NoError(t, conn.Dial("unix", socket))
	defer conn.Close()
	// #nosec G115 -- test fixture timestamp
	expiry := uint32(time.Now().Add(10 * time.Minute).Unix())
	require.NoError(t, conn.LocalMessageSubmission().Client.SubmitMessage(
		&ocommon.DmqMessage{
			Payload: ocommon.DmqMessagePayload{
				MessageBody: bytes.Repeat([]byte("node"), 100),
				KESPeriod:   1,
				ExpiresAt:   expiry,
			},
			KESSignature: make([]byte, 448),
			OperationalCertificate: ocommon.OperationalCertificate{
				KESVerificationKey: make([]byte, 32),
				IssueNumber:        1,
				KESPeriod:          1,
				ColdSignature:      make([]byte, 64),
			},
			ColdVerificationKey: make([]byte, 32),
		},
	))
	select {
	case r := <-verdict:
		return r
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no verdict from the dmq socket")
		return nil
	}
}
