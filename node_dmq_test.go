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
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/dmq"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
	"github.com/prometheus/client_golang/prometheus"
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

// A node that cannot answer a lookup has no grounds to call a message
// invalid, so every failure must carry the sentinel admission reports as other.
func TestDMQStakeAuthorityWithoutLedger(t *testing.T) {
	t.Parallel()
	auth := &dmqStakeAuthority{}
	_, err := auth.PoolActiveStake(ocommon.PoolKeyHash{})
	require.ErrorIs(t, err, dmq.ErrChainStateUnavailable)
	require.ErrorIs(t, err, errDMQLedgerDetached)
	_, err = auth.CurrentSlot()
	require.ErrorIs(t, err, dmq.ErrChainStateUnavailable)
	_, err = auth.CurrentKESPeriod()
	require.ErrorIs(t, err, dmq.ErrChainStateUnavailable)
	_, _, err = auth.PoolOpCertIssueNumber(ocommon.PoolKeyHash{})
	require.ErrorIs(t, err, dmq.ErrChainStateUnavailable)
}

func dmqPoolStakeAt(
	auth *dmqStakeAuthority,
	slot uint64,
	pool ocommon.PoolKeyHash,
) (uint64, error) {
	auth.mu.RLock()
	defer auth.mu.RUnlock()
	return auth.poolStakeAtLocked(slot, pool)
}

// The authority answers from the snapshot that elects leaders at the slot: a
// pool in it carries its stake, a pool absent from it carries none, and a
// distribution the ledger has not captured is unavailable rather than empty.
func TestDMQStakeAuthorityReadsElectingStake(t *testing.T) {
	t.Parallel()
	n, points := newLiveLifecycleTestNode(t, 2)
	auth := &n.dmqStake
	auth.setLedgerState(n.ledgerState)
	slot := points[0].Slot

	unknown := ocommon.PoolKeyHash{0xbb}
	_, err := dmqPoolStakeAt(auth, slot, unknown)
	require.ErrorIs(t, err, dmq.ErrChainStateUnavailable)
	require.ErrorIs(t, err, ledger.ErrLeaderStakeSnapshotUnavailable)

	epoch, err := n.ledgerState.SlotToEpoch(slot)
	require.NoError(t, err)
	staked := ocommon.PoolKeyHash{0xaa}
	for _, row := range []struct {
		epoch uint64
		pool  ocommon.PoolKeyHash
		stake uint64
	}{
		{praos.StakeSnapshotEpoch(epoch.EpochId), staked, 700},
		// The next epoch's distribution elects nobody yet.
		{praos.StakeSnapshotEpoch(epoch.EpochId) + 1, unknown, 900},
	} {
		require.NoError(t, n.ledgerState.Database().Metadata().
			SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
				Epoch:        row.epoch,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  row.pool[:],
				TotalStake:   types.Uint64(row.stake),
			}, nil))
	}

	stake, err := dmqPoolStakeAt(auth, slot, staked)
	require.NoError(t, err)
	require.Equal(t, uint64(700), stake)
	stake, err = dmqPoolStakeAt(auth, slot, unknown)
	require.NoError(t, err)
	require.Zero(t, stake)
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
	// A fixed chain slot, so lookups reach storage rather than stopping at a
	// wall clock the test chain's era history does not cover.
	slot := points[0].Slot
	lookup := func() error {
		_, err := dmqPoolStakeAt(auth, slot, ocommon.PoolKeyHash{})
		// The test chain holds no stake snapshot, which is an answer read
		// from storage.
		if errors.Is(err, ledger.ErrLeaderStakeSnapshotUnavailable) {
			return nil
		}
		return err
	}
	wg.Go(func() {
		for {
			err := lookup()
			if err != nil && !errors.Is(err, errDMQLedgerDetached) {
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
	require.NoError(t, lookup())
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
			Message: "dmq: chain state unavailable: " +
				"confirmed era history does not cover wall clock",
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

func TestDMQStakeAuthorityLedgerPeerSnapshot(t *testing.T) {
	t.Parallel()
	auth := &dmqStakeAuthority{}
	_, err := auth.LedgerPeerSnapshot(t.Context())
	require.ErrorIs(t, err, dmq.ErrChainStateUnavailable)
	require.ErrorIs(t, err, errDMQLedgerDetached)

	n, points := newLiveLifecycleTestNode(t, 2)
	auth.setLedgerState(n.ledgerState)
	snapshot, err := auth.LedgerPeerSnapshot(t.Context())
	require.NoError(t, err)
	require.True(t, snapshot.Slot.HasSlot)
	require.LessOrEqual(t, snapshot.Slot.Slot, points[len(points)-1].Slot)

	auth.setLedgerState(nil)
	_, err = auth.LedgerPeerSnapshot(t.Context())
	require.ErrorIs(t, err, dmq.ErrChainStateUnavailable)
}

func TestDMQPeerConfig(t *testing.T) {
	t.Parallel()
	topo := &dmq.Topology{}
	base := internalconfig.DefaultDMQConfig()
	base.Enabled = true
	base.N2NPort = 4001
	base.MinPeers, base.MaxPeers = 3, 10

	t.Run("both address families", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		got := n.dmqPeerConfig(base, topo)
		require.NotNil(t, got)
		require.Equal(t, []dmq.ListenAddr{
			{Address: "0.0.0.0:4001"},
			{Address: "[::]:4001", SkipIfUnsupported: true},
		}, got.ListenAddrs)
		require.Equal(t, 3, got.MinPeers)
		require.Equal(t, 10, got.MaxPeers)
		require.Same(t, topo, got.Topology)
		require.NotNil(t, got.LedgerPeers, "mithril has an SRV prefix")
	})
	t.Run("one address family", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		cfg := base
		cfg.ListenIPv6Address = ""
		got := n.dmqPeerConfig(cfg, nil)
		require.Equal(
			t,
			[]dmq.ListenAddr{{Address: "0.0.0.0:4001"}},
			got.ListenAddrs,
		)
		require.Nil(t, got.Topology)
	})
	t.Run("no SRV prefix means no ledger peers", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		cfg := base
		cfg.Topic = "other"
		require.Nil(t, n.dmqPeerConfig(cfg, nil).LedgerPeers)
	})
	t.Run("no peer port serves local clients only", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		cfg := base
		cfg.N2NPort = 0
		require.Nil(t, n.dmqPeerConfig(cfg, topo))
	})
	t.Run("a topology without a peer port is reported", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		logs := &syncLogBuffer{}
		n.config.logger = slog.New(slog.NewTextHandler(logs, nil))
		cfg := base
		cfg.N2NPort = 0
		cfg.TopologyFile = "/etc/dingo/dmq-topology.json"
		require.Nil(t, n.dmqPeerConfig(cfg, topo))
		require.Contains(t, logs.String(), "dmq.n2nPort is 0")
	})
	t.Run("the defaults warn of nothing", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		logs := &syncLogBuffer{}
		n.config.logger = slog.New(slog.NewTextHandler(logs, nil))
		cfg := internalconfig.DefaultDMQConfig()
		cfg.Enabled = true
		require.Nil(t, n.dmqPeerConfig(cfg, nil))
		require.NotContains(t, logs.String(), "level=WARN")
	})
	t.Run("a peer port with no room for peers is reported", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		logs := &syncLogBuffer{}
		n.config.logger = slog.New(slog.NewTextHandler(logs, nil))
		cfg := base
		cfg.MinPeers, cfg.MaxPeers = 0, 0
		require.Nil(t, n.dmqPeerConfig(cfg, topo))
		require.Contains(t, logs.String(), "dmq.maxPeers is 0")
	})
	t.Run("room for no peers serves local clients only", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		cfg := base
		cfg.MinPeers, cfg.MaxPeers = 0, 0
		require.Nil(t, n.dmqPeerConfig(cfg, topo))
	})
	t.Run("settings too large for an int are clamped", func(t *testing.T) {
		t.Parallel()
		n := newAPIPluginRuntimeNode(t)
		cfg := base
		cfg.MinPeers, cfg.MaxPeers = 1, ^uint(0)
		require.Equal(t, math.MaxInt32, n.dmqPeerConfig(cfg, nil).MaxPeers)
	})
}

// dmqPeerMessage is a message of the shape CIP-0137 fixes, which a peer
// that does not authenticate admits.
func dmqPeerMessage(t *testing.T, body string) ocommon.DmqMessage {
	t.Helper()
	// #nosec G115 -- test fixture timestamp
	expiry := uint32(time.Now().Add(10 * time.Minute).Unix())
	msg := ocommon.DmqMessage{
		Payload: ocommon.DmqMessagePayload{
			MessageBody: bytes.Repeat([]byte(body), 100),
			KESPeriod:   1,
			ExpiresAt:   expiry,
		},
		KESSignature: make([]byte, 448),
		OperationalCertificate: ocommon.OperationalCertificate{
			KESVerificationKey: make([]byte, 32),
			ColdSignature:      make([]byte, 64),
		},
		ColdVerificationKey: make([]byte, 32),
	}
	require.NoError(t, msg.SetComputedMessageID())
	return msg
}

// newDMQPeerStack is a stack of the node's DMQ network that authenticates
// nothing, standing in for another DMQ node.
func newDMQPeerStack(
	t *testing.T,
	magic uint32,
	peers *dmq.PeerConfig,
) *dmq.Stack {
	t.Helper()
	socketDir, err := os.MkdirTemp("", "dmq")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	s, err := dmq.NewStack(dmq.StackConfig{
		PromRegistry:   prometheus.NewRegistry(),
		NetworkMagic:   magic,
		SocketPath:     filepath.Join(socketDir, "dmq.sock"),
		Authenticator:  ocommon.NewNoOpAuthenticator(nil),
		MessageBodyMin: 1,
		Peers:          peers,
	})
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, s.Stop(ctx))
	})
	return s
}

// A node with DMQ enabled dials the peers its topology names, accepts the
// ones that dial it, and passes messages to them, all in the one process that
// also runs the Cardano stack.
func TestNodeRunConnectsDMQPeers(t *testing.T) {
	t.Parallel()
	const magic = 42
	remote := newDMQPeerStack(t, magic, &dmq.PeerConfig{
		MaxPeers:    5,
		ListenAddrs: []dmq.ListenAddr{{Address: "127.0.0.1:0"}},
	})
	remoteAddr := remote.PeerListenAddrs()[0].(*net.TCPAddr)
	visitor := newDMQPeerStack(t, magic, nil)

	// The node's own peer port is found by binding and releasing one.
	probePort, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	nodePort := probePort.Addr().(*net.TCPAddr).Port
	require.NoError(t, probePort.Close())

	n := newAPIPluginRuntimeNode(t)
	topologyFile := filepath.Join(t.TempDir(), "dmq-topology.json")
	require.NoError(t, os.WriteFile(topologyFile, []byte(`{
		"localRoots": [{
			"accessPoints": [{"address": "127.0.0.1", "port": `+
		strconv.Itoa(remoteAddr.Port)+`}],
			"valency": 1
		}]
	}`), 0o600))
	socketDir, err := os.MkdirTemp("", "dmq")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	dmqCfg := internalconfig.DefaultDMQConfig()
	dmqCfg.Enabled = true
	dmqCfg.NetworkMagic = magic
	dmqCfg.SocketPath = filepath.Join(socketDir, "dmq.sock")
	dmqCfg.N2NPort = uint(nodePort) // #nosec G115 -- a TCP port
	dmqCfg.ListenAddress = "127.0.0.1"
	dmqCfg.ListenIPv6Address = ""
	dmqCfg.MinPeers, dmqCfg.MaxPeers = 1, 5
	dmqCfg.TopologyFile = topologyFile
	n.config.cfg.DMQ = dmqCfg

	errProbe := errors.New("probe done")
	probe := &apiLifecycleProbe{onStart: func() error {
		stack := n.dmqStack
		testutil.WaitForCondition(t, func() bool {
			return stack.PeerCount() == 1 && remote.PeerCount() == 1
		}, 10*time.Second, "the node dials its topology peer")

		// A message in the node's pool reaches the peer it dialed.
		sent := dmqPeerMessage(t, "from-node")
		added, err := stack.Mempool().Add(sent)
		require.NoError(t, err)
		require.True(t, added)
		testutil.WaitForCondition(t, func() bool {
			return remote.Mempool().Has(sent.ID())
		}, 10*time.Second, "the message reaches the dialed peer")

		// A peer that dials the node's port is accepted as well.
		conn, err := net.DialTimeout(
			"tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(nodePort)),
			10*time.Second,
		)
		require.NoError(t, err)
		_, err = visitor.AttachPeer(context.Background(), conn, true)
		require.NoError(t, err)
		testutil.WaitForCondition(t, func() bool {
			return stack.PeerCount() == 2
		}, 10*time.Second, "the node accepts an inbound peer")
		testutil.WaitForCondition(t, func() bool {
			return visitor.Mempool().Has(sent.ID())
		}, 10*time.Second, "the message reaches the inbound peer too")
		return errProbe
	}}
	registerAPIProbe(
		t, n.pluginHost, plugin.CapabilityAPIBlockfrost, "probe", probe,
	)
	selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)

	require.ErrorIs(t, n.Run(context.Background()), errProbe)
	require.Equal(t, int32(1), probe.starts.Load())
}

// Ledger peer lookups keep running while a live truncate replaces the ledger
// state: a lookup may only find the state detached, never one being closed.
func TestDMQStakeAuthorityLedgerPeerSnapshotAcrossLiveTruncate(t *testing.T) {
	t.Parallel()
	const numBlocks = 6
	n, points := newLiveLifecycleTestNode(t, numBlocks)
	auth := &n.dmqStake
	auth.setLedgerState(n.ledgerState)
	stop := make(chan struct{})
	started := make(chan struct{})
	var lookups atomic.Int64
	var wg sync.WaitGroup
	var lookupErr error
	wg.Go(func() {
		for {
			_, err := auth.LedgerPeerSnapshot(context.Background())
			if err != nil && !errors.Is(err, errDMQLedgerDetached) {
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
	require.Positive(t, during, "no snapshot lookup ran during the truncate")
	_, err = auth.LedgerPeerSnapshot(context.Background())
	require.NoError(t, err)
}
