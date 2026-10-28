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

//go:build dingo_dmq_interop

package dmqinterop

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/dmq"
	"github.com/blinklabs-io/dingo/keystore"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// dmqNetworkMagic is the DMQ network magic run.sh starts dmq-node with.
const dmqNetworkMagic = 2147483650

// The devnet's pool has a KES period of 0 and a slot length of one second
// with 129600 slots per KES period, which the authenticator is told so it
// checks the real signature windows.
const (
	slotsPerKESPeriod = 129600
	maxKESEvolutions  = 60
)

type environment struct {
	// nodeAddr is where dmq-node accepts peer connections.
	nodeAddr string
	// listenAddr is where dmq-node's topology tells it to dial in.
	listenAddr string
	// keysDir holds the devnet pool's KES, VRF and operational certificate.
	keysDir string
	// cfgDir is mounted into the container, so a message written here can be
	// submitted from inside it.
	cfgDir    string
	container string
	timeout   time.Duration
}

func loadEnvironment(t *testing.T) environment {
	t.Helper()
	env := environment{
		nodeAddr:   os.Getenv("DMQ_INTEROP_NODE_ADDR"),
		listenAddr: os.Getenv("DMQ_INTEROP_LISTEN_ADDR"),
		keysDir:    os.Getenv("DMQ_INTEROP_KEYS"),
		cfgDir:     os.Getenv("DMQ_INTEROP_CFG"),
		container:  os.Getenv("DMQ_INTEROP_CONTAINER"),
		timeout:    30 * time.Minute,
	}
	if env.nodeAddr == "" || env.keysDir == "" || env.cfgDir == "" ||
		env.container == "" {
		t.Skip("DMQ_INTEROP_* is not set; run internal/test/dmqinterop/run.sh")
	}
	if value := os.Getenv("DMQ_INTEROP_TIMEOUT"); value != "" {
		timeout, err := time.ParseDuration(value)
		require.NoError(t, err)
		env.timeout = timeout
	}
	return env
}

// anyStake gives every pool stake. The devnet pool's stake is real, but it is
// held by the Cardano node dmq-node asks, so this side does not repeat it; the
// signatures and certificate are still checked in full.
type anyStake struct{}

func (anyStake) PoolActiveStake(ocommon.PoolKeyHash) (uint64, error) {
	return 1000, nil
}

func newAuthenticator(t *testing.T) *ocommon.MessageAuthenticator {
	t.Helper()
	auth, err := ocommon.NewMessageAuthenticator(
		ocommon.MessageAuthenticatorConfig{
			StakeAuthority: anyStake{},
			CurrentSlot:    func() (uint64, error) { return 0, nil },
			PoolOpCertIssueNumber: func(
				ocommon.PoolKeyHash,
			) (uint64, bool, error) {
				return 0, false, nil
			},
			SlotsPerKESPeriod: slotsPerKESPeriod,
			MaxKESEvolutions:  maxKESEvolutions,
		},
	)
	require.NoError(t, err)
	return auth
}

func newStack(t *testing.T) *dmq.Stack {
	t.Helper()
	return newStackWithPeers(t, nil)
}

// newStackWithPeers is a stack whose listener and peer manager run as the
// node's do, when peers is set.
func newStackWithPeers(t *testing.T, peers *dmq.PeerConfig) *dmq.Stack {
	t.Helper()
	dir, err := os.MkdirTemp("", "dmq")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	stack, err := dmq.NewStack(dmq.StackConfig{
		Logger: slog.New(slog.NewTextHandler(
			os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo},
		)),
		PromRegistry:  prometheus.NewRegistry(),
		NetworkMagic:  dmqNetworkMagic,
		SocketPath:    filepath.Join(dir, "dmq.sock"),
		Authenticator: newAuthenticator(t),
		Peers:         peers,
	})
	require.NoError(t, err)
	require.NoError(t, stack.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second,
		)
		defer cancel()
		require.NoError(t, stack.Stop(ctx))
	})
	return stack
}

// signedMessage builds a message signed by the devnet pool's real KES key and
// certificate, with a body inside CIP-0137's 90 to 2000 bytes.
func signedMessage(
	t *testing.T,
	env environment,
	label string,
) ocommon.DmqMessage {
	t.Helper()
	store := keystore.NewKeyStore(keystore.KeyStoreConfig{
		VRFSKeyPath:       filepath.Join(env.keysDir, "vrf.skey"),
		KESSKeyPath:       filepath.Join(env.keysDir, "kes.skey"),
		OpCertPath:        filepath.Join(env.keysDir, "opcert.cert"),
		SlotsPerKESPeriod: slotsPerKESPeriod,
		MaxKESEvolutions:  maxKESEvolutions,
	})
	require.NoError(t, store.LoadFromFiles())
	opcert := store.OpCert()
	body := bytes.Repeat([]byte(label+"|"), 400/(len(label)+1)+1)[:400]
	payload := ocommon.DmqMessagePayload{
		MessageBody: body,
		KESPeriod:   opcert.KESPeriod,
		// #nosec G115 -- a timestamp minutes from now
		ExpiresAt: uint32(time.Now().Add(10 * time.Minute).Unix()),
	}
	// dmq-node verifies the signature over the payload array exactly as it
	// appears in the message, the bytes between the message ID and the
	// signature, and not wrapped in a byte string.
	signable, err := cbor.Encode(payload)
	require.NoError(t, err)
	signature, err := store.KESSigner().Sign(opcert.KESPeriod, signable)
	require.NoError(t, err)
	msg := ocommon.DmqMessage{
		Payload:      payload,
		KESSignature: signature,
		OperationalCertificate: ocommon.OperationalCertificate{
			KESVerificationKey: opcert.KESVKey,
			IssueNumber:        opcert.IssueNumber,
			KESPeriod:          opcert.KESPeriod,
			ColdSignature:      opcert.Signature,
		},
		ColdVerificationKey: opcert.ColdVKey,
	}
	require.NoError(t, msg.SetComputedMessageID())
	return msg
}

// submitToNode hands msg to dmq-node's local socket from inside its
// container, and returns the node's verdict.
func submitToNode(
	t *testing.T,
	env environment,
	msg ocommon.DmqMessage,
	name string,
) string {
	t.Helper()
	raw, err := cbor.Encode(msg)
	require.NoError(t, err)
	require.NoError(
		t, os.WriteFile(filepath.Join(env.cfgDir, name), raw, 0o600),
	)
	out, err := exec.Command(
		"docker", "exec", env.container,
		"/opt/bin/dmq-submit", "/data/dmq.sock",
		fmt.Sprint(dmqNetworkMagic), "/etc/dmq/"+name,
	).CombinedOutput()
	if err != nil {
		t.Logf("dmq-submit: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// submitUntilAccepted signs a fresh message and submits it until dmq-node
// accepts one, and returns the accepted message. It retries because dmq-node
// only accepts a pool's messages once the pool's set stake snapshot, which it
// reads from the Cardano node each epoch, is nonzero, and a message expires
// ten minutes after it is signed, so each attempt signs its own.
func submitUntilAccepted(
	t *testing.T,
	env environment,
	label string,
) ocommon.DmqMessage {
	t.Helper()
	deadline := time.Now().Add(env.timeout)
	verdict := ""
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		msg := signedMessage(t, env, label)
		verdict = submitToNode(t, env, msg, label+".cbor")
		if verdict == "ACCEPTED" {
			return msg
		}
		t.Logf("dmq-node says %q; waiting for the devnet's stake", verdict)
		time.Sleep(15 * time.Second)
	}
	require.FailNow(
		t, "dmq-node never accepted the message", "last: %s", verdict,
	)
	return ocommon.DmqMessage{}
}

// minSigDelay is how long dmq-node makes a pool wait between messages.
const minSigDelay = 60 * time.Second

var primeOnce sync.Once

// primeNode gets a throwaway message into dmq-node first. dmq-node 0.7.2.1
// does not announce the first message its mempool ever holds to any peer, no
// matter whether the peer connected before or after it, so a test message sent
// to a freshly started node would never reach Dingo. Later messages are
// announced at once. The wait is dmq-node's per-pool minimum between messages,
// which it would otherwise refuse the test message for.
func primeNode(t *testing.T, env environment) {
	t.Helper()
	primeOnce.Do(func() {
		submitUntilAccepted(t, env, "primer")
		time.Sleep(minSigDelay + 2*time.Second)
	})
}

func dialNode(
	t *testing.T,
	env environment,
	stack *dmq.Stack,
) *dmq.PeerConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", env.nodeAddr, 10*time.Second)
	require.NoError(t, err)
	peer, err := stack.AttachPeer(t.Context(), conn, true)
	require.NoError(t, err)
	return peer
}

func waitFor(
	t *testing.T,
	timeout time.Duration,
	msg string,
	cond func() bool,
) {
	t.Helper()
	require.Eventually(t, cond, timeout, 250*time.Millisecond, msg)
}

func closedPeer(peer *dmq.PeerConn) bool {
	select {
	case <-peer.Done():
		return true
	default:
		return false
	}
}

// Dingo dials dmq-node: the handshake settles on version 2, keep-alive runs
// on mini-protocol 12, and the connection stays up.
func TestInteropDingoDialsDmqNode(t *testing.T) {
	env := loadEnvironment(t)
	stack := newStack(t)

	peer := dialNode(t, env, stack)
	require.Equal(t, uint16(2), peer.Version())
	select {
	case <-peer.Done():
		require.FailNow(t, "dmq-node closed the connection", "%v", peer.Err())
	case <-time.After(20 * time.Second):
	}
	require.NoError(t, peer.Err())
}

// dmq-node holds a message signed by the devnet pool, and Dingo pulls it and
// verifies the signatures and certificate itself.
func TestInteropDmqNodeServesAMessageToDingo(t *testing.T) {
	env := loadEnvironment(t)
	primeNode(t, env)
	msg := submitUntilAccepted(t, env, "node-to-dingo")

	stack := newStack(t)
	peer := dialNode(t, env, stack)
	waitFor(t, env.timeout, "Dingo receives the message", func() bool {
		return stack.Mempool().Has(msg.ID())
	})
	got, ok := stack.Mempool().Get(msg.ID())
	require.True(t, ok)
	require.Equal(t, msg.Payload.MessageBody, got.Payload.MessageBody)
	require.Equal(t, msg.KESSignature, got.KESSignature)
	require.False(
		t,
		closedPeer(peer),
		"the connection survived: %v",
		peer.Err(),
	)
}

// Dingo holds a message signed by the devnet pool and dmq-node, which dials
// in because its topology names this machine, pulls it and accepts it. A
// second Dingo stack then fetches it from dmq-node, which proves dmq-node
// kept it.
func TestInteropDmqNodePullsAMessageFromDingo(t *testing.T) {
	env := loadEnvironment(t)
	if env.listenAddr == "" {
		t.Skip("DMQ_INTEROP_LISTEN_ADDR is not set")
	}
	primeNode(t, env)
	msg := signedMessage(t, env, "dingo-to-node")

	serving := newStackWithPeers(t, &dmq.PeerConfig{
		MaxPeers:    5,
		ListenAddrs: []dmq.ListenAddr{{Address: env.listenAddr}},
	})
	added, err := serving.Mempool().Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	fetching := newStack(t)
	dialNode(t, env, fetching)
	waitFor(t, env.timeout, "a second Dingo fetches it from dmq-node",
		func() bool { return fetching.Mempool().Has(msg.ID()) })
	got, ok := fetching.Mempool().Get(msg.ID())
	require.True(t, ok)
	require.Equal(t, msg.Payload.MessageBody, got.Payload.MessageBody)
}

// Dingo's peer manager dials dmq-node from a topology, as the node does, and
// the connection holds.
func TestInteropPeerManagerDialsDmqNode(t *testing.T) {
	env := loadEnvironment(t)
	host, portText, err := net.SplitHostPort(env.nodeAddr)
	require.NoError(t, err)
	port, err := strconv.ParseUint(portText, 10, 16)
	require.NoError(t, err)

	stack := newStackWithPeers(t, &dmq.PeerConfig{
		MinPeers: 1,
		MaxPeers: 5,
		Topology: &dmq.Topology{LocalRoots: []dmq.RootGroup{{
			Peers:   []dmq.Peer{{Host: host, Port: uint(port)}},
			Valency: 1,
		}}},
	})
	waitFor(t, 30*time.Second, "the manager connects to dmq-node", func() bool {
		return stack.PeerCount() == 1
	})
	require.Never(t, func() bool { return stack.PeerCount() != 1 },
		20*time.Second, 500*time.Millisecond,
		"the connection to dmq-node holds")
}
