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
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/stretchr/testify/require"
)

// fakeSignClient stands in for a sign-mode KES agent client: a reload only
// prepares it and installs it, and never signs through it.
type fakeSignClient struct {
	mu         sync.Mutex
	opCert     []byte
	prepareErr error
	prepared   bool
	closed     bool
}

func (f *fakeSignClient) Sign(uint64, []byte) ([]byte, error) {
	return nil, errors.New("fake sign client does not sign")
}

func (f *fakeSignClient) Prepare() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepared = true
	return f.prepareErr
}

func (f *fakeSignClient) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSignClient) state() (prepared, closed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prepared, f.closed
}

// signModeReloadNode is a reloadTestNode configured for KES agent sign mode,
// whose reloads build fakeSignClients.
type signModeReloadNode struct {
	*reloadTestNode
	mu         sync.Mutex
	built      []*fakeSignClient
	prepareErr error
	initial    *fakeSignClient
}

func newSignModeReloadNode(t *testing.T, counter uint64) *signModeReloadNode {
	t.Helper()
	s := &signModeReloadNode{reloadTestNode: newReloadTestNode(t, counter)}
	s.config.shelleyKESAgentSocket = "/run/kes-agent.sock"
	s.config.shelleyKESAgentMode = kesagent.ModeSign
	s.initial = &fakeSignClient{}
	s.kesAgentClient = s.initial
	s.keyReload.signClientFactory = func(
		key *bursa.LoadedKey,
	) (kesAgentSignClient, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		c := &fakeSignClient{
			opCert:     append([]byte(nil), key.RawCBOR...),
			prepareErr: s.prepareErr,
		}
		s.built = append(s.built, c)
		return c, nil
	}
	return s
}

func (s *signModeReloadNode) builtClients() []*fakeSignClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*fakeSignClient(nil), s.built...)
}

// opCertCBOR returns the CBOR of a certificate for the node's pool with
// counter, as an agent presents it at admission.
func (s *signModeReloadNode) opCertCBOR(t *testing.T, counter uint64) []byte {
	t.Helper()
	path := opCertFixtureForColdKey(t, s.coldVKey, s.coldSKey, counter, nil)
	key, err := forging.LoadOperationalCertificateFile(path)
	require.NoError(t, err)
	return key.RawCBOR
}

func noLedgerCheck(*forging.PoolCredentials) error { return nil }

func TestAgentSignReloadInstallsTheNewCertificateAndClient(t *testing.T) {
	t.Parallel()
	s := newSignModeReloadNode(t, 1)
	s.rotateTo(t, 2, nil)

	require.NoError(t, s.reload(nil))

	s.requireLiveCounter(t, 2)
	built := s.builtClients()
	require.Len(t, built, 1)
	prepared, closed := built[0].state()
	require.True(t, prepared, "the agent must admit the new certificate first")
	require.False(t, closed)
	require.Same(t, built[0], s.kesAgentClient)
	// The replaced client stays open for a forge attempt still holding it.
	_, initialClosed := s.initial.state()
	require.False(t, initialClosed)
	require.Contains(t, s.logs.String(), "credentials reloaded")
}

func TestAgentSignReloadWithTheAgentsNewerCertificate(t *testing.T) {
	t.Parallel()
	s := newSignModeReloadNode(t, 1)
	agentOpCert := s.opCertCBOR(t, 2)

	require.NoError(t, s.reloadAgentSignCredentials(
		0, true, noLedgerCheck, agentOpCert,
	))

	s.requireLiveCounter(t, 2)
	built := s.builtClients()
	require.Len(t, built, 1)
	require.Equal(t, agentOpCert, built[0].opCert,
		"the new client must expect the agent's certificate")
	require.Contains(t, s.logs.String(), `"source":"kes agent"`)
}

func TestAgentSignReloadLeavesTheLiveCredentialsOnRefusal(t *testing.T) {
	t.Parallel()

	t.Run("unchanged certificate", func(t *testing.T) {
		t.Parallel()
		s := newSignModeReloadNode(t, 1)
		require.ErrorIs(
			t,
			s.reloadAgentSignCredentials(0, true, noLedgerCheck, nil),
			errCredentialsUnchanged,
		)
		require.Empty(t, s.builtClients())
		s.requireLiveCounter(t, 1)
	})

	t.Run("changed certificate with the same counter", func(t *testing.T) {
		t.Parallel()
		s := newSignModeReloadNode(t, 2)
		period := uint64(1)
		s.rotateTo(t, 2, &period)
		require.ErrorContains(t, s.reload(nil), "not above the loaded counter")
		require.Empty(t, s.builtClients())
		s.requireLiveCounter(t, 2)
	})

	t.Run("lower counter", func(t *testing.T) {
		t.Parallel()
		s := newSignModeReloadNode(t, 2)
		s.rotateTo(t, 1, nil)
		require.ErrorContains(t, s.reload(nil), "not above the loaded counter")
		s.requireLiveCounter(t, 2)
	})

	t.Run("agent does not serve the certificate", func(t *testing.T) {
		t.Parallel()
		s := newSignModeReloadNode(t, 1)
		s.prepareErr = errors.New("admission refused")
		s.rotateTo(t, 2, nil)
		require.ErrorContains(
			t,
			s.reload(nil),
			"does not serve the new certificate",
		)
		s.requireLiveCounter(t, 1)
		built := s.builtClients()
		require.Len(t, built, 1)
		_, closed := built[0].state()
		require.True(t, closed, "a refused client must be closed")
		require.Same(t, s.initial, s.kesAgentClient)
	})

	t.Run("ledger check fails", func(t *testing.T) {
		t.Parallel()
		s := newSignModeReloadNode(t, 1)
		s.rotateTo(t, 2, nil)
		err := s.reload(func(*forging.PoolCredentials) error {
			return errors.New("counter gap")
		})
		require.ErrorContains(t, err, "ledger check")
		s.requireLiveCounter(t, 1)
		require.Same(t, s.initial, s.kesAgentClient)
	})

	t.Run("agent certificate for another pool", func(t *testing.T) {
		t.Parallel()
		s := newSignModeReloadNode(t, 1)
		other := newSignModeReloadNode(t, 1)
		err := s.reloadAgentSignCredentials(
			0, true, noLedgerCheck, other.opCertCBOR(t, 2),
		)
		require.Error(t, err)
		s.requireLiveCounter(t, 1)
		require.Same(t, s.initial, s.kesAgentClient)
	})
}

func TestReloadInServeKeyModeIsRefused(t *testing.T) {
	t.Parallel()
	s := newSignModeReloadNode(t, 1)
	s.config.shelleyKESAgentMode = kesagent.ModeServeKey
	s.rotateTo(t, 2, nil)
	require.ErrorContains(t, s.reload(nil), "serve-key mode")
	s.requireLiveCounter(t, 1)
}

func TestShouldTryAgentOpCertSkipsARecentRetry(t *testing.T) {
	t.Parallel()
	n := &Node{}
	now := time.Now()
	require.True(t, n.shouldTryAgentOpCert([]byte("a"), now))
	require.False(t, n.shouldTryAgentOpCert([]byte("a"), now.Add(time.Second)))
	require.True(t, n.shouldTryAgentOpCert([]byte("b"), now.Add(time.Second)))
	require.True(t, n.shouldTryAgentOpCert(
		[]byte("b"),
		now.Add(time.Second+agentOpCertRetryInterval),
	))
}

func TestOfferAgentOpCertKeepsTheNewestOffer(t *testing.T) {
	t.Parallel()
	n := &Node{}
	n.offerAgentOpCert([]byte("old"))
	n.offerAgentOpCert([]byte("new"))
	got := testutil.RequireReceive(
		t, n.agentOpCertOffers(), time.Second, "offer",
	)
	require.Equal(t, []byte("new"), got)
	testutil.RequireNoReceive(t, n.agentOpCertOffers(), 20*time.Millisecond,
		"only the newest offer is kept")
}

func TestAgentOpCertLoopReloadsEachNewOffer(t *testing.T) {
	t.Parallel()
	n := newTestNodeForBP(t, false, "", "", "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reloads := make(chan []byte, 4)
	go n.agentOpCertLoop(ctx, func(opCert []byte) error {
		reloads <- opCert
		return errors.New("refused")
	})

	n.offerAgentOpCert([]byte("cert-2"))
	require.Equal(t, []byte("cert-2"),
		testutil.RequireReceive(t, reloads, time.Second, "first offer"))
	// The same certificate again, right after a failed reload, is skipped.
	n.offerAgentOpCert([]byte("cert-2"))
	n.offerAgentOpCert([]byte("cert-3"))
	require.Equal(t, []byte("cert-3"),
		testutil.RequireReceive(t, reloads, time.Second, "a different offer"))
}

func TestCredentialFileWatchReloadsOnceAfterAStableChange(t *testing.T) {
	t.Parallel()
	n := newTestNodeForBP(t, false, "", "", "", nil)
	dir := t.TempDir()
	file := filepath.Join(dir, "opcert.cert")
	require.NoError(t, os.WriteFile(file, []byte("counter 1"), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reloads := make(chan struct{}, 4)
	go n.credentialFileWatchLoop(ctx, 5*time.Millisecond, []string{file},
		func() error {
			reloads <- struct{}{}
			return nil
		},
	)

	testutil.RequireNoReceive(t, reloads, 50*time.Millisecond,
		"unchanged files must not reload")
	require.NoError(t, os.WriteFile(file, []byte("counter 2"), 0o600))
	testutil.RequireReceive(t, reloads, 2*time.Second, "reload after a change")
	testutil.RequireNoReceive(t, reloads, 50*time.Millisecond,
		"one change reloads once")
}

func TestCredentialFileWatchSeesASymlinkSwap(t *testing.T) {
	t.Parallel()
	n := newTestNodeForBP(t, false, "", "", "", nil)
	// The layout Kubernetes uses for a mounted Secret: the file is a symlink
	// through ..data, which is swapped atomically to a new directory.
	dir := t.TempDir()
	v1 := filepath.Join(dir, "..2026_v1")
	v2 := filepath.Join(dir, "..2026_v2")
	require.NoError(t, os.Mkdir(v1, 0o700))
	require.NoError(t, os.Mkdir(v2, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(v1, "opcert.cert"), []byte("counter 1"), 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(v2, "opcert.cert"), []byte("counter 2"), 0o600,
	))
	data := filepath.Join(dir, "..data")
	require.NoError(t, os.Symlink("..2026_v1", data))
	file := filepath.Join(dir, "opcert.cert")
	require.NoError(t, os.Symlink(filepath.Join("..data", "opcert.cert"), file))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reloads := make(chan struct{}, 4)
	go n.credentialFileWatchLoop(ctx, 5*time.Millisecond, []string{file},
		func() error {
			reloads <- struct{}{}
			return nil
		},
	)
	testutil.RequireNoReceive(t, reloads, 50*time.Millisecond, "no change yet")

	tmp := filepath.Join(dir, "..data_tmp")
	require.NoError(t, os.Symlink("..2026_v2", tmp))
	require.NoError(t, os.Rename(tmp, data))
	testutil.RequireReceive(t, reloads, 2*time.Second, "reload after the swap")
}
