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
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// twoPoolSignNode is a node forging for two pools, each signing through its
// own KES agent in sign mode. Every reload builds a fakeSignClient through
// first's factory, whichever pool it is for.
type twoPoolSignNode struct {
	first, second *signModeReloadNode
	pp            *poolProducer
}

func newTwoPoolSignNode(t *testing.T) *twoPoolSignNode {
	t.Helper()
	first := newSignModeReloadNode(t, 1)
	second := newSignModeReloadNode(t, 1)
	first.config.promRegistry = prometheus.NewRegistry()
	pool := func(s *signModeReloadNode, socket string) BlockProducerPool {
		return BlockProducerPool{
			ShelleyVRFKey:                 s.config.shelleyVRFKey,
			ShelleyOperationalCertificate: s.opcertPath,
			ShelleyKESAgentSocket:         socket,
			ShelleyKESAgentMode:           kesagent.ModeSign,
		}
	}
	first.config.blockProducerPools = []BlockProducerPool{
		pool(first, "/run/kes-agent-a.sock"),
		pool(second, "/run/kes-agent-b.sock"),
	}
	pp := &poolProducer{
		index:          1,
		keys:           producerKeysFromPool(first.config.blockProducerPools[1]),
		poolID:         second.live.GetPoolID().String(),
		creds:          second.live,
		kesAgentClient: second.initial,
	}
	first.multiPool.producers = []*poolProducer{pp}
	return &twoPoolSignNode{first: first, second: second, pp: pp}
}

func TestReloadKeepsAnotherPoolsFailureBesideAnUnchangedPool(t *testing.T) {
	t.Parallel()
	n := newTwoPoolSignNode(t)
	firstID := n.first.live.GetPoolID().String()
	require.NotNil(t, n.first.reloadFailureCounter())
	// The first pool's certificate is unchanged; the second pool's file is
	// broken, so its reload fails.
	require.NoError(t, os.WriteFile(n.second.opcertPath, []byte("{"), 0o644))

	err := n.first.reloadEveryPool(0, true, noLedgerCheck, false)
	require.Error(t, err)
	require.False(
		t,
		errors.Is(err, errCredentialsUnchanged),
		"an unchanged pool must not turn another pool's failure into unchanged: %v",
		err,
	)
	require.ErrorContains(t, err, "pool "+n.pp.poolID)
	require.NotContains(t, err.Error(), firstID)
	require.Equal(t, 0.0, testutil.ToFloat64(
		n.first.multiPool.reloadFailures.WithLabelValues(firstID),
	), "an unchanged pool is not a failed reload")
	n.first.requireLiveCounter(t, 1)
	n.second.requireLiveCounter(t, 1)
}

func TestReloadReportsUnchangedOnlyWhenEveryPoolIsUnchanged(t *testing.T) {
	t.Parallel()
	n := newTwoPoolSignNode(t)
	require.ErrorIs(
		t,
		n.first.reloadEveryPool(0, true, noLedgerCheck, false),
		errCredentialsUnchanged,
	)
	require.Empty(t, n.first.builtClients())
}

// startLoops gives the node a ledger for the reload clock and starts its
// reload loops.
func (n *twoPoolSignNode) startLoops(t *testing.T) {
	t.Helper()
	start := time.Now().Add(-time.Minute)
	n.first.ledgerState = newStartupCleanupProducerNodeWithGenesisStart(
		t,
		&start,
	).ledgerState
	n.first.startKeyReloadLoops()
	t.Cleanup(n.first.stopKeyReloadLoops)
}

// waitReloaded waits for the log of a completed reload of poolID. The log is
// written after the client swap, and reading it through the synced buffer
// orders the test's reads of the client fields after that swap.
func (n *twoPoolSignNode) waitReloaded(t *testing.T, poolID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for line := range strings.SplitSeq(n.first.logs.String(), "\n") {
			if strings.Contains(line, "block producer credentials reloaded") &&
				strings.Contains(line, `"pool_id":"`+poolID+`"`) {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
}

func TestAgentOpCertOfferReloadsOnlyItsOwnPool(t *testing.T) {
	t.Parallel()

	t.Run("second pool", func(t *testing.T) {
		t.Parallel()
		n := newTwoPoolSignNode(t)
		n.startLoops(t)
		offer := n.pp.keySource(n.first.Node).onNewerOpCert
		require.NotNil(t, offer, "the second pool has no newer opcert hook")

		offer(n.second.opCertCBOR(t, 2))

		n.waitReloaded(t, n.pp.poolID)
		n.second.requireLiveCounter(t, 2)
		n.first.requireLiveCounter(t, 1)
		require.Same(t, n.first.initial, n.first.kesAgentClient)
		require.NotSame(t, n.second.initial, n.pp.kesAgentClient)
		require.NotContains(
			t,
			n.first.logs.String(),
			"cannot change pool or VRF identity",
		)
	})

	t.Run("first pool", func(t *testing.T) {
		t.Parallel()
		n := newTwoPoolSignNode(t)
		n.startLoops(t)
		offer := n.first.primaryKeySource().onNewerOpCert
		require.NotNil(t, offer, "the first pool has no newer opcert hook")

		offer(n.first.opCertCBOR(t, 2))

		n.waitReloaded(t, n.first.live.GetPoolID().String())
		n.first.requireLiveCounter(t, 2)
		n.second.requireLiveCounter(t, 1)
		require.Same(t, n.second.initial, n.pp.kesAgentClient)
		require.NotSame(t, n.first.initial, n.first.kesAgentClient)
	})
}

func TestKeyWatchCoversEveryPool(t *testing.T) {
	t.Parallel()
	n := newTwoPoolSignNode(t)
	files := n.first.watchedCredentialFiles()
	second := n.first.config.blockProducerPools[1]
	require.Contains(t, files, second.ShelleyVRFKey)
	require.Contains(t, files, second.ShelleyOperationalCertificate)
	require.Contains(t, files, n.first.opcertPath)
}

func TestKeyWatchRunsWhileAnyPoolCanReload(t *testing.T) {
	t.Parallel()
	n := newTwoPoolSignNode(t)
	n.first.config.blockProducerPools[0].ShelleyKESAgentMode = kesagent.ModeServeKey
	n.first.config.shelleyKeyWatchInterval = time.Millisecond
	n.startLoops(t)
	require.NotContains(
		t,
		n.first.logs.String(),
		"ignored in KES agent serve-key mode",
	)

	// The watch is running: a new certificate for the sign-mode pool is
	// picked up from its file.
	n.second.rotateTo(t, 2, nil)

	n.waitReloaded(t, n.pp.poolID)
	n.second.requireLiveCounter(t, 2)
}

// newServeKeyAndSignPoolNode is a twoPoolSignNode whose first pool takes its
// KES key from an agent in serve-key mode, where a reload is always refused.
func newServeKeyAndSignPoolNode(t *testing.T) *twoPoolSignNode {
	t.Helper()
	n := newTwoPoolSignNode(t)
	n.first.config.blockProducerPools[0].ShelleyKESAgentMode = kesagent.ModeServeKey
	return n
}

func TestKeyWatchSkipsServeKeyPools(t *testing.T) {
	t.Parallel()
	n := newServeKeyAndSignPoolNode(t)
	files := n.first.watchedCredentialFiles()
	require.NotContains(t, files, n.first.opcertPath,
		"a serve-key pool cannot reload, so its files are not watched")
	require.Contains(t, files, n.first.config.blockProducerPools[1].ShelleyOperationalCertificate)
}

func TestKeyWatchReloadDoesNotFailServeKeyPools(t *testing.T) {
	t.Parallel()
	n := newServeKeyAndSignPoolNode(t)
	firstID := n.first.live.GetPoolID().String()
	require.NotNil(t, n.first.reloadFailureCounter())
	n.first.config.shelleyKeyWatchInterval = time.Millisecond
	n.startLoops(t)

	n.second.rotateTo(t, 2, nil)

	// The first pool is reloaded, or refused, before the second, so its
	// count is final once the second pool has reloaded.
	n.waitReloaded(t, n.pp.poolID)
	n.second.requireLiveCounter(t, 2)
	require.Equal(t, 0.0, testutil.ToFloat64(
		n.first.multiPool.reloadFailures.WithLabelValues(firstID),
	), "a file change is not a failed reload of a serve-key pool")
}

func TestReloadSkipsServeKeyPoolsOnlyForTheFileWatch(t *testing.T) {
	t.Parallel()
	n := newServeKeyAndSignPoolNode(t)
	firstID := n.first.live.GetPoolID().String()
	require.NotNil(t, n.first.reloadFailureCounter())
	failures := func() float64 {
		return testutil.ToFloat64(
			n.first.multiPool.reloadFailures.WithLabelValues(firstID),
		)
	}

	// The file watch skips the serve-key pool: with the sign-mode pool
	// unchanged, the whole reload is unchanged.
	require.ErrorIs(
		t,
		n.first.reloadEveryPool(0, true, noLedgerCheck, true),
		errCredentialsUnchanged,
	)
	require.Equal(t, 0.0, failures())

	// A SIGHUP still reports and counts the serve-key pool's refusal.
	err := n.first.reloadEveryPool(0, true, noLedgerCheck, false)
	require.ErrorContains(t, err, "serve-key mode")
	require.ErrorContains(t, err, "pool "+firstID)
	require.Equal(t, 1.0, failures())
}

// TestAgentSignReloadOfAnotherPoolContactsTheAgentOutsideTheLifecycleGates
// pins that a pool after the first is checked with its agent, which can wait
// on the network, without holding liveLifecycleMu, whether a SIGHUP or its
// agent's offer reloads it.
func TestAgentSignReloadOfAnotherPoolContactsTheAgentOutsideTheLifecycleGates(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		reload func(t *testing.T, n *twoPoolSignNode) error
	}{
		{
			name: "signal",
			reload: func(t *testing.T, n *twoPoolSignNode) error {
				n.second.rotateTo(t, 2, nil)
				return n.first.reloadCredentials(reloadTriggerSignal, nil)
			},
		},
		{
			name: "agent offer",
			reload: func(t *testing.T, n *twoPoolSignNode) error {
				return n.first.reloadPoolProducerAgentOpCert(
					n.pp,
					n.second.opCertCBOR(t, 2),
				)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := newTwoPoolSignNode(t)
			start := time.Now().Add(-time.Minute)
			n.first.ledgerState = newStartupCleanupProducerNodeWithGenesisStart(
				t,
				&start,
			).ledgerState
			gateFree := false
			n.first.onPrepare = func() {
				if n.first.liveLifecycleMu.TryLock() {
					gateFree = true
					n.first.liveLifecycleMu.Unlock()
				}
			}

			require.NoError(t, tc.reload(t, n))

			n.second.requireLiveCounter(t, 2)
			n.first.requireLiveCounter(t, 1)
			built := n.first.builtClients()
			require.Len(t, built, 1)
			require.Same(t, built[0], n.pp.kesAgentClient)
			require.Same(t, n.first.initial, n.first.kesAgentClient)
			require.True(t, gateFree,
				"the agent must be checked before the lifecycle gates are taken")
		})
	}
}

func TestAgentSignReloadRefusesAnotherPoolsChangedVRFKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		reload func(t *testing.T, n *twoPoolSignNode) error
	}{
		{
			name: "unchanged certificate",
			reload: func(t *testing.T, n *twoPoolSignNode) error {
				return n.first.reloadEveryPool(0, true, noLedgerCheck, false)
			},
		},
		{
			name: "changed certificate",
			reload: func(t *testing.T, n *twoPoolSignNode) error {
				n.second.rotateTo(t, 2, nil)
				return n.first.reloadEveryPool(0, true, noLedgerCheck, false)
			},
		},
		{
			name: "agent offer",
			reload: func(t *testing.T, n *twoPoolSignNode) error {
				return n.first.reloadPoolProducerAgentOpCert(
					n.pp,
					n.second.opCertCBOR(t, 2),
				)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := newTwoPoolSignNode(t)
			firstID := n.first.live.GetPoolID().String()
			require.NotNil(t, n.first.reloadFailureCounter())
			writeFreshVRFKey(t, n.second.config.shelleyVRFKey)

			err := tc.reload(t, n)

			require.ErrorContains(t, err, "pool "+n.pp.poolID)
			require.ErrorContains(t, err, "VRF key file no longer matches")
			require.NotErrorIs(t, err, errCredentialsUnchanged)
			require.Empty(t, n.first.builtClients(),
				"a VRF change must be refused before the agent is contacted")
			n.second.requireLiveCounter(t, 1)
			n.first.requireLiveCounter(t, 1)
			require.Same(t, n.second.initial, n.pp.kesAgentClient)
			require.Equal(t, 1.0, testutil.ToFloat64(
				n.first.multiPool.reloadFailures.WithLabelValues(n.pp.poolID),
			))
			require.Equal(t, 0.0, testutil.ToFloat64(
				n.first.multiPool.reloadFailures.WithLabelValues(firstID),
			))
		})
	}
}
