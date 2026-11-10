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
	"testing"

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

	err := n.first.reloadEveryPool(0, true, noLedgerCheck)
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
		n.first.reloadEveryPool(0, true, noLedgerCheck),
		errCredentialsUnchanged,
	)
	require.Empty(t, n.first.builtClients())
}
