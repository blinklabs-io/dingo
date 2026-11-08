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
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// testVRFPool proves with a real VRF key and counts its proofs.
type testVRFPool struct {
	sk     []byte
	proofs atomic.Int32
	err    error
}

func newTestVRFPool(t *testing.T, seed byte) *testVRFPool {
	t.Helper()
	_, sk, err := vrf.KeyGen(bytes.Repeat([]byte{seed}, 32))
	require.NoError(t, err)
	return &testVRFPool{sk: sk}
}

func (p *testVRFPool) VRFProve(alpha []byte) ([]byte, []byte, error) {
	p.proofs.Add(1)
	if p.err != nil {
		return nil, nil, p.err
	}
	return vrf.Prove(p.sk, alpha)
}

func (p *testVRFPool) output(t *testing.T, alpha []byte) []byte {
	t.Helper()
	_, out, err := vrf.Prove(p.sk, alpha)
	require.NoError(t, err)
	return out
}

// testLeader leads exactly the slots it lists.
type testLeader map[uint64]bool

func (l testLeader) NextLeaderSlot(from uint64) (uint64, bool) {
	if l[from] {
		return from, true
	}
	return 0, false
}

// testArbiterView is one epoch of one era with a fixed nonce.
type testArbiterView struct {
	era   uint
	nonce []byte
}

func (v testArbiterView) EpochForSlot(uint64) (uint64, error) { return 7, nil }

func (v testArbiterView) EraIDForEpoch(
	uint64,
) (uint, error) {
	return v.era, nil
}
func (v testArbiterView) EpochNonce(context.Context, uint64) []byte {
	return v.nonce
}

func newTestArbiter(view arbiterChainView) *poolSlotArbiter {
	a := newPoolSlotArbiter(slog.New(slog.NewTextHandler(io.Discard, nil)))
	a.setView(view)
	return a
}

// rankPools returns the pool IDs ordered as the chain would rank a block from
// each: lower leader VRF output first.
func rankPools(
	t *testing.T,
	alpha []byte,
	a, b *testVRFPool,
	aID, bID string,
) (winner, loser string) {
	t.Helper()
	if bytes.Compare(a.output(t, alpha), b.output(t, alpha)) < 0 {
		return aID, bID
	}
	return bID, aID
}

func TestSlotArbiterKeepsTheLowerVRFOutput(t *testing.T) {
	t.Parallel()
	const slot = 1000
	nonce := bytes.Repeat([]byte{0x42}, 32)
	poolA, poolB := newTestVRFPool(t, 1), newTestVRFPool(t, 2)
	a := newTestArbiter(testArbiterView{era: conway.EraIdConway, nonce: nonce})
	a.add(arbiterMember{"pool-a", poolA, testLeader{slot: true}, true})
	a.add(arbiterMember{"pool-b", poolB, testLeader{slot: true}, true})

	alpha, err := vrf.MkInputVrf(slot, nonce)
	require.NoError(t, err)
	winner, loser := rankPools(t, alpha, poolA, poolB, "pool-a", "pool-b")

	yield, got := a.forPool(loser).YieldSlot(slot)
	require.True(t, yield, "the pool with the higher VRF output must yield")
	require.Equal(t, winner, got)
	yield, _ = a.forPool(winner).YieldSlot(slot)
	require.False(t, yield, "the pool with the lower VRF output forges")

	// Both asked; the decision was made once.
	require.Equal(t, int32(1), poolA.proofs.Load())
	require.Equal(t, int32(1), poolB.proofs.Load())
}

func TestSlotArbiterUsesTheTPraosLeaderSeedBeforeBabbage(t *testing.T) {
	t.Parallel()
	nonce := bytes.Repeat([]byte{0x17}, 32)
	poolA, poolB := newTestVRFPool(t, 3), newTestVRFPool(t, 4)
	praosAlpha := func(slot uint64) []byte {
		alpha, err := vrf.MkInputVrf(
			int64(slot),
			nonce,
		) // #nosec G115 -- test slots are small
		require.NoError(t, err)
		return alpha
	}
	tpraosAlpha := func(slot uint64) []byte {
		alpha, err := vrf.MkSeedTPraos(
			int64(slot),
			nonce,
			vrf.SeedL(),
		) // #nosec G115 -- test slots are small
		require.NoError(t, err)
		return alpha
	}
	// Find a slot where the two inputs rank the pools differently, so the
	// test fails if the arbiter used the Praos input in a TPraos era.
	var slot uint64
	for s := uint64(1); s < 200; s++ {
		pw, _ := rankPools(t, praosAlpha(s), poolA, poolB, "a", "b")
		tw, _ := rankPools(t, tpraosAlpha(s), poolA, poolB, "a", "b")
		if pw != tw {
			slot = s
			break
		}
	}
	require.NotZero(t, slot, "no slot ranks the pools differently")

	a := newTestArbiter(testArbiterView{era: alonzo.EraIdAlonzo, nonce: nonce})
	a.add(arbiterMember{"a", poolA, testLeader{slot: true}, true})
	a.add(arbiterMember{"b", poolB, testLeader{slot: true}, true})
	want, _ := rankPools(t, tpraosAlpha(slot), poolA, poolB, "a", "b")
	require.Equal(t, want, a.winner(slot))
}

func TestSlotArbiterForgesEverySlotItCannotDecide(t *testing.T) {
	t.Parallel()
	const slot = 50
	nonce := bytes.Repeat([]byte{0x01}, 32)
	tests := []struct {
		name  string
		view  arbiterChainView
		praos bool
		err   error
	}{
		{"no nonce", testArbiterView{era: conway.EraIdConway}, true, nil},
		{
			"a schedule without VRF",
			testArbiterView{era: conway.EraIdConway, nonce: nonce},
			false,
			nil,
		},
		{
			"a VRF failure",
			testArbiterView{era: conway.EraIdConway, nonce: nonce},
			true,
			errors.New("VRF key not loaded"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			poolA, poolB := newTestVRFPool(t, 5), newTestVRFPool(t, 6)
			poolB.err = tc.err
			a := newTestArbiter(tc.view)
			a.add(arbiterMember{"a", poolA, testLeader{slot: true}, true})
			a.add(arbiterMember{"b", poolB, testLeader{slot: true}, tc.praos})
			for _, id := range []string{"a", "b"} {
				yield, _ := a.forPool(id).YieldSlot(slot)
				require.False(t, yield, "pool %s must forge", id)
			}
		})
	}
}

func TestSlotArbiterLeavesASlotOnePoolLeads(t *testing.T) {
	t.Parallel()
	nonce := bytes.Repeat([]byte{0x09}, 32)
	poolA, poolB := newTestVRFPool(t, 7), newTestVRFPool(t, 8)
	a := newTestArbiter(testArbiterView{era: conway.EraIdConway, nonce: nonce})
	a.add(arbiterMember{"a", poolA, testLeader{10: true}, true})
	a.add(arbiterMember{"b", poolB, testLeader{11: true}, true})

	yield, _ := a.forPool("a").YieldSlot(10)
	require.False(t, yield)
	require.Zero(t, poolA.proofs.Load()+poolB.proofs.Load(),
		"a slot one pool leads needs no VRF proof")

	// A pool removed from the arbiter is no longer weighed.
	a.add(arbiterMember{"c", newTestVRFPool(t, 9), testLeader{10: true}, true})
	a.remove("c")
	yield, _ = a.forPool("a").YieldSlot(10)
	require.False(t, yield)
}

func TestOwnForgedBlocksAnswersForEveryPool(t *testing.T) {
	t.Parallel()
	newForger := func() *forging.BlockForger {
		f, err := forging.NewBlockForger(forging.ForgerConfig{
			Mode:   forging.ModeDev,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		require.NoError(t, err)
		return f
	}
	first, second := newForger(), newForger()
	first.SlotTracker().RecordForgedBlock(10, []byte{0x0a})
	second.SlotTracker().RecordForgedBlock(20, []byte{0x14})

	own := &ownForgedBlocks{}
	own.set([]*forging.BlockForger{first, second})

	hash, ok := own.WasForgedByUs(20)
	require.True(t, ok, "a block of the second pool is the node's own")
	require.Equal(t, []byte{0x14}, hash)
	_, ok = own.WasForgedByUs(30)
	require.False(t, ok)
	hash, ok = own.WasForgedByUs(10)
	require.True(t, ok)
	require.Equal(t, []byte{0x0a}, hash)
}

func TestPoolMetricsRegistryKeepsPoolsApart(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	n := &Node{config: Config{
		promRegistry: reg,
		blockProducerPools: []BlockProducerPool{
			{ShelleyOperationalCertificate: "/a"},
			{ShelleyOperationalCertificate: "/b"},
		},
	}}
	for _, poolID := range []string{"pool1aaa", "pool1bbb"} {
		_, err := forging.NewBlockForger(forging.ForgerConfig{
			Mode:         forging.ModeDev,
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			PromRegistry: n.poolMetricsRegistry(poolID),
		})
		require.NoError(t, err, "a second pool's forger metrics must register")
	}
	families, err := reg.Gather()
	require.NoError(t, err)
	seen := map[string]bool{}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "pool_id" {
					seen[l.GetValue()] = true
				}
			}
		}
	}
	require.Equal(t, map[string]bool{"pool1aaa": true, "pool1bbb": true}, seen)

	single := &Node{config: Config{promRegistry: reg}}
	require.Same(
		t,
		prometheus.Registerer(reg),
		single.poolMetricsRegistry("pool1aaa"),
		"one pool keeps the unlabelled series it always had",
	)
}

func TestPoolIDFromOpCertFileMatchesTheCredentials(t *testing.T) {
	t.Parallel()
	path := filepath.Join("internal", "devnetkeys", "keys", "opcert.cert")
	got, err := poolIDFromOpCertFile(path)
	require.NoError(t, err)
	opCert, err := forging.LoadOperationalCertificateFile(path)
	require.NoError(t, err)
	want := lcommon.PoolId(lcommon.Blake2b224Hash(opCert.OpCertColdVKey)).
		String()
	require.Equal(t, want, got)
}

// TestReloadBlockProducerCredentialsReloadsEachPoolOnItsOwn rotates a second
// pool while the first pool's certificate is unreadable: the second still
// rotates, the first keeps its keys, and the failure is named and counted.
func TestReloadBlockProducerCredentialsReloadsEachPoolOnItsOwn(t *testing.T) {
	t.Parallel()
	start := time.Now().Add(-time.Minute)
	started := newStartupCleanupProducerNodeWithGenesisStart(t, &start)
	first := newReloadTestNode(t, 1)
	second := newReloadTestNode(t, 1)
	first.ledgerState = started.ledgerState
	reg := prometheus.NewRegistry()
	first.config.promRegistry = reg
	pool := func(r *reloadTestNode) BlockProducerPool {
		return BlockProducerPool{
			ShelleyVRFKey:                 r.config.shelleyVRFKey,
			ShelleyKESKey:                 r.config.shelleyKESKey,
			ShelleyOperationalCertificate: r.opcertPath,
		}
	}
	first.config.blockProducerPools = []BlockProducerPool{
		pool(first),
		pool(second),
	}
	secondID := second.live.GetPoolID().String()
	first.multiPool.producers = []*poolProducer{{
		index:  1,
		keys:   producerKeysFromPool(pool(second)),
		poolID: secondID,
		creds:  second.live,
	}}

	require.NoError(t, os.WriteFile(first.opcertPath, []byte("{"), 0o644))
	second.rotateTo(t, 2, nil)

	err := first.ReloadBlockProducerCredentials()
	firstID := first.live.GetPoolID().String()
	require.ErrorContains(t, err, "pool "+firstID)
	require.ErrorContains(t, err, "load pool credentials")
	require.NotContains(t, err.Error(), secondID)

	first.requireLiveCounter(t, 1)
	second.requireLiveCounter(t, 2)
	require.Contains(t, first.logs.String(), `"pool_id":"`+secondID+`"`)
	require.Equal(t, 1.0, testutil.ToFloat64(
		first.multiPool.reloadFailures.WithLabelValues(firstID),
	))
}

func TestPrepareMultiPoolRefusesLeiosWithSeveralPools(t *testing.T) {
	t.Parallel()
	n := &Node{config: Config{
		leiosVoteSigningKeyFile: "/keys/vote.skey",
		blockProducerPools: []BlockProducerPool{
			{ShelleyOperationalCertificate: "/a"},
			{ShelleyOperationalCertificate: "/b"},
		},
	}}
	require.ErrorContains(t, n.prepareMultiPool(), "Leios forging and voting")
}

// Not t.Parallel: it starts real forge loops and election workers, which the
// sequential startup tests in this package count by goroutine stack.
func TestStartBlockProducerSkipsAPoolWhoseKESAgentIsUnreachable(t *testing.T) {
	n := newStartupCleanupProducerNode(t)
	coldVKey, coldSKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	secondOpCert := opCertFixtureForColdKey(t, coldVKey, coldSKey, 0, nil)
	secondID, err := poolIDFromOpCertFile(secondOpCert)
	require.NoError(t, err)
	n.config.blockProducerPools = []BlockProducerPool{
		{
			ShelleyVRFKey:                 n.config.shelleyVRFKey,
			ShelleyKESKey:                 n.config.shelleyKESKey,
			ShelleyOperationalCertificate: n.config.shelleyOperationalCertificate,
		},
		{
			ShelleyVRFKey:                 n.config.shelleyVRFKey,
			ShelleyOperationalCertificate: secondOpCert,
			ShelleyKESAgentSocket: filepath.Join(
				t.TempDir(),
				"absent.sock",
			),
			ShelleyKESAgentMode: "serve-key",
		},
	}

	started, err := n.startBlockProducer(t.Context(), nil)
	require.NoError(
		t,
		err,
		"an unreachable KES agent must not stop the other pools",
	)
	t.Cleanup(func() { runStopsLIFO(started) })
	require.True(t, n.blockForger.IsRunning(), "the first pool forges")

	ready := n.poolProducersReady()
	require.ErrorContains(t, ready, "pool "+secondID)
	require.ErrorIs(t, ready, errKESSourceUnavailable)
	n.multiPool.mu.Lock()
	retrying := n.multiPool.retryCancel != nil
	n.multiPool.mu.Unlock()
	require.True(t, retrying, "the skipped pool is retried")

	runStopsLIFO(started)
	started = nil
	n.multiPool.mu.Lock()
	retrying = n.multiPool.retryCancel != nil
	n.multiPool.mu.Unlock()
	require.False(t, retrying, "stopping the producer stops the retry")
	require.False(t, n.blockForger.IsRunning())
}
