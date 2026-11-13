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

package kesagent

import (
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// startPlainAgents starts n plain-TCP fake agents serving the same key.
func startPlainAgents(
	t *testing.T,
	n int,
	skeyData, vkey, opCert []byte,
) []*fakeRemoteAgent {
	t.Helper()
	agents := make([]*fakeRemoteAgent, n)
	for i := range agents {
		agents[i] = startFakeRemoteAgentOn(
			t, fakePlainTCP, testCert{}, skeyData, vkey, opCert,
		)
	}
	return agents
}

func newTestFailoverClient(
	t *testing.T,
	agents []*fakeRemoteAgent,
	vkey, opCert []byte,
	metrics *Metrics,
) *FailoverClient {
	t.Helper()
	addresses := make([]string, len(agents))
	for i, a := range agents {
		addresses[i] = a.addr
	}
	f, err := NewFailoverClient(addresses, RemoteConfig{
		Plaintext:   true,
		KESVKey:     vkey,
		OpCert:      opCert,
		SignTimeout: 2 * time.Second,
		Metrics:     metrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestFailoverClientMovesOnAfterALinkFailureToAPreparedStandby(
	t *testing.T,
) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	agents := startPlainAgents(t, 2, skeyData, vkey, opCert)
	reg := prometheus.NewRegistry()
	f := newTestFailoverClient(t, agents, vkey, opCert, NewMetrics(reg))
	primary, standby := agents[0], agents[1]

	require.NoError(t, f.Prepare())
	require.Equal(t, primary.addr, f.Active())
	// Prepare admits the standby in the background.
	require.Eventually(t, func() bool {
		return standby.accepts.Load() == 1
	}, 5*time.Second, 10*time.Millisecond)

	primary.dropAfterRequest.Store(true)
	_, err := f.Sign(0, []byte("header-1"))
	require.Error(t, err)
	require.Equal(t, int32(0), standby.signs.Load(),
		"a failed sign must not be retried on another agent")
	require.Equal(t, standby.addr, f.Active(),
		"a link failure moves the next attempt to the next agent")

	sig, err := f.Sign(0, []byte("header-2"))
	require.NoError(t, err)
	require.True(t, kes.VerifySignedKES(vkey, 0, []byte("header-2"), sig))
	require.Equal(t, int32(1), standby.accepts.Load(),
		"the standby's prepared session carries the sign: no new dial")
	require.Equal(t, 1.0, testutil.ToFloat64(
		f.metrics.agentFailovers,
	))
	require.Equal(t, 1.0, testutil.ToFloat64(
		f.metrics.activeAgent.WithLabelValues(standby.addr),
	))
	require.Equal(t, 0.0, testutil.ToFloat64(
		f.metrics.activeAgent.WithLabelValues(primary.addr),
	))
}

func TestFailoverClientKeepsTheAgentOnARefusal(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	agents := startPlainAgents(t, 2, skeyData, vkey, opCert)
	f := newTestFailoverClient(t, agents, vkey, opCert, nil)

	agents[0].refuse.Store(true)
	_, err := f.Sign(0, []byte("header"))
	require.ErrorIs(t, err, ErrAgentRefused)
	require.Equal(t, agents[0].addr, f.Active(),
		"an agent that answered with a refusal is working; stay on it")
}

func TestFailoverClientPrepareUsesTheFirstAgentThatAdmits(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	agents := startPlainAgents(t, 3, skeyData, vkey, opCert)
	agents[0].admitFix = func(r *attestationResponse) {
		r.Error = "no key installed"
	}
	f := newTestFailoverClient(t, agents, vkey, opCert, nil)

	require.NoError(t, f.Prepare())
	require.Equal(t, agents[1].addr, f.Active())
	sig, err := f.Sign(0, []byte("header"))
	require.NoError(t, err)
	require.True(t, kes.VerifySignedKES(vkey, 0, []byte("header"), sig))
	require.Equal(t, int32(1), agents[1].signs.Load())
}

func TestFailoverClientPrepareReportsEveryAgentWhenNoneAdmits(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	agents := startPlainAgents(t, 2, skeyData, vkey, opCert)
	for _, a := range agents {
		a.admitFix = func(r *attestationResponse) { r.Error = "no key" }
	}
	f := newTestFailoverClient(t, agents, vkey, opCert, nil)

	err := f.Prepare()
	require.ErrorIs(t, err, ErrRemoteAdmission)
	require.ErrorContains(t, err, agents[0].addr)
	require.ErrorContains(t, err, agents[1].addr)
	require.Equal(t, agents[0].addr, f.Active())
}

func TestFailoverClientCheckReadyAndClose(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	agents := startPlainAgents(t, 2, skeyData, vkey, opCert)
	f := newTestFailoverClient(t, agents, vkey, opCert, nil)

	require.NoError(t, f.Prepare())
	require.NoError(t, f.CheckReady())
	require.NoError(t, f.Close())
	require.ErrorIs(t, f.CheckReady(), ErrClosed)
	_, err := f.Sign(0, []byte("header"))
	require.ErrorIs(t, err, ErrClosed)
	require.Equal(t, agents[0].addr, f.Active(),
		"a closed client does not move between agents")
}

func TestNewFailoverClientRefusesAnEmptyList(t *testing.T) {
	t.Parallel()
	_, vkey, opCert := testKESMaterial(t)
	_, err := NewFailoverClient(nil, RemoteConfig{
		Plaintext: true,
		KESVKey:   vkey,
		OpCert:    opCert,
	})
	require.Error(t, err)
}

func TestFailoverClientPrepareKeepsANewerMove(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	agents := startPlainAgents(t, 3, skeyData, vkey, opCert)
	f := newTestFailoverClient(t, agents, vkey, opCert, nil)

	// A Sign moved the client from agent 0 to agent 2 while a Prepare that
	// started at agent 0 was admitting agent 1.
	f.mu.Lock()
	f.active = 2
	f.mu.Unlock()
	f.adoptPrepared(0, 1, nil)
	require.Equal(t, agents[2].addr, f.Active())

	f.adoptPrepared(2, 1, nil)
	require.Equal(t, agents[1].addr, f.Active())
}

func TestFailoverMetricsReportADuplicatedActiveAddress(t *testing.T) {
	t.Parallel()
	m := NewMetrics(prometheus.NewRegistry())
	m.setActiveAgent([]string{"a:1", "b:1", "a:1"}, 0)
	require.InDelta(t, 1, testutil.ToFloat64(m.activeAgent.WithLabelValues("a:1")), 0)
	require.InDelta(t, 0, testutil.ToFloat64(m.activeAgent.WithLabelValues("b:1")), 0)
}

func TestFailoverConnectedGaugeFollowsTheActiveAgent(t *testing.T) {
	t.Parallel()
	_, vkey, opCert := testKESMaterial(t)
	m := NewMetrics(prometheus.NewRegistry())
	f, err := NewFailoverClient(
		[]string{"127.0.0.1:1", "127.0.0.1:2"},
		RemoteConfig{
			Plaintext: true,
			KESVKey:   vkey,
			OpCert:    opCert,
			Metrics:   m,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	connected := func() float64 { return testutil.ToFloat64(m.connected) }

	f.clients[0].setConnected(true)
	f.clients[1].setConnected(true)
	f.clients[1].setConnected(false)
	require.InDelta(t, 1, connected(), 0,
		"a standby dropping its session does not clear the active agent's state")

	f.moveFrom(0, errors.New("link failed"))
	require.InDelta(t, 0, connected(), 0,
		"after a move the gauge reports the new active agent")
	f.clients[1].setConnected(true)
	require.InDelta(t, 1, connected(), 0)
	f.clients[0].setConnected(false)
	require.InDelta(t, 1, connected(), 0)
}
