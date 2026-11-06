//go:build linux && devnet && devnet_conformance

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

package scenarios

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/stretchr/testify/require"
)

// soakDurationEnv selects how long TestChainAgreementSoak runs, as a Go
// duration (for example "10m"). The test is skipped when it is unset so the
// regular conformance run keeps its length.
const soakDurationEnv = "DEVNET_AGREEMENT_SOAK_DURATION"

// forkRelation classifies how two observed chains relate.
type forkRelation int

const (
	// relIndeterminate means the retained headers cannot show how the chains
	// relate: a chain has no headers yet, or the two retained windows cover
	// disjoint block numbers, so one node is further behind than the window.
	relIndeterminate forkRelation = iota
	// relCommon means the chains share a block; depth is blocks beyond it.
	relCommon
	// relBeyondWindow means both windows cover the same block numbers with
	// different hashes everywhere, so the fork is deeper than anything retained.
	relBeyondWindow
)

// forkDepth returns how many blocks the longer of two observed chains holds
// beyond their deepest common block, matched by hash at the same block
// number. The depth is 0 when one tip is an ancestor of the other, because a
// node that is merely behind on the same chain has not diverged.
func forkDepth(a, b devnet.ChainSnapshot) (uint64, forkRelation) {
	byBlock := make(map[uint64]string, len(b.Headers))
	for _, h := range b.Headers {
		byBlock[h.BlockNumber] = hex.EncodeToString(h.Hash)
	}
	var common uint64
	var found, overlap bool
	for _, h := range a.Headers {
		bh, have := byBlock[h.BlockNumber]
		if !have {
			continue
		}
		overlap = true
		if bh == hex.EncodeToString(h.Hash) && (!found || h.BlockNumber >= common) {
			common = h.BlockNumber
			found = true
		}
	}
	if !found {
		if overlap {
			return 0, relBeyondWindow
		}
		return 0, relIndeterminate
	}
	if common >= min(a.Tip.BlockNumber, b.Tip.BlockNumber) {
		return 0, relCommon
	}
	return max(a.Tip.BlockNumber, b.Tip.BlockNumber) - common, relCommon
}

// worstForkDepth compares every distinct pair of snapshots, so a lagging
// first node cannot hide a fork between two others. unmatched counts pairs
// whose relation could not be determined.
func worstForkDepth(snaps []devnet.ChainSnapshot, k uint64) (worst uint64, unmatched int) {
	for i := range snaps {
		for j := i + 1; j < len(snaps); j++ {
			d, rel := forkDepth(snaps[i], snaps[j])
			switch rel {
			case relBeyondWindow:
				d = k + 1
			case relIndeterminate:
				unmatched++
				continue
			case relCommon:
			}
			worst = max(worst, d)
		}
	}
	return worst, unmatched
}

// TestChainAgreementSoak follows every node's chain for
// DEVNET_AGREEMENT_SOAK_DURATION and fails if two nodes' chains ever diverge
// by more than the security parameter. A fork shallower than k is normal
// slot-battle behaviour; one deeper than k cannot be resolved and is a
// permanent split. Tips and the fork depth are logged every sample so the run
// is evidence of what the chains did, not only of the verdict.
//
// The Go test timeout (TEST_TIMEOUT in run-tests.sh, -timeout for direct runs)
// must exceed the soak duration plus setup and the other scenarios.
func TestChainAgreementSoak(t *testing.T) {
	raw := os.Getenv(soakDurationEnv)
	if raw == "" {
		t.Skipf("%s not set", soakDurationEnv)
	}
	duration, err := time.ParseDuration(raw)
	require.NoError(t, err, "invalid %s", soakDurationEnv)
	require.Positive(t, duration, "%s must be positive", soakDurationEnv)

	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")
	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints, devnet.WithNetworkMagic(cfg.NetworkMagic),
	)
	h.WaitForAllNodesReady(60 * time.Second)
	h.WaitForChainStart(devnet.ChainStartTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obs := devnet.StartObservers(ctx, endpoints, cfg.NetworkMagic, t.Logf)
	defer obs.Stop()

	k := cfg.SecurityParam
	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var samples, forked, unmatched, beyondK int
	var maxDepth uint64
	depthSamples := make(map[uint64]int)
	for time.Now().Before(deadline) {
		<-ticker.C
		snaps := obs.Group().Snapshots()
		line := ""
		for _, s := range snaps {
			// Observers have no tip until their first block arrives.
			short := hex.EncodeToString(s.Tip.Hash)
			if len(short) > 8 {
				short = short[:8]
			}
			line += fmt.Sprintf(
				" %s=slot %d/block %d/%s", s.Node, s.Tip.SlotNumber,
				s.Tip.BlockNumber, short,
			)
		}
		samples++
		worst, um := worstForkDepth(snaps, k)
		unmatched += um
		depthSamples[worst]++
		if worst > 0 {
			forked++
		}
		maxDepth = max(maxDepth, worst)
		t.Logf("sample %d: forkDepth=%d%s", samples, worst, line)
		// A fork deeper than k cannot be resolved by either side: the
		// nodes would have to roll back further than the security
		// parameter allows. Keep sampling after the first one so the log
		// shows whether the chains ever rejoin.
		if worst > k {
			beyondK++
			if beyondK == 1 {
				t.Errorf(
					"nodes hold chains that diverge by %d blocks (k=%d):%s",
					worst, k, line,
				)
			}
		}
	}
	for _, s := range obs.Group().Snapshots() {
		t.Logf(
			"final %s forwards=%d backwards=%d maxRollback=%d"+
				" connects=%d disconnects=%d",
			s.String(), s.RollForwards, s.RollBackwards,
			s.MaxRollbackDepth, s.Connects, s.Disconnects,
		)
		if s.MaxRollbackDepth > k {
			t.Errorf("%s observed a rollback deeper than k=%d", s.Node, k)
		}
	}
	depths := make([]uint64, 0, len(depthSamples))
	for d := range depthSamples {
		depths = append(depths, d)
	}
	sort.Slice(depths, func(i, j int) bool { return depths[i] < depths[j] })
	for _, d := range depths {
		t.Logf("fork depth %d: %d samples", d, depthSamples[d])
	}
	t.Logf(
		"soak finished: %d samples, %d forked, %d beyond k, %d unmatched,"+
			" max depth %d",
		samples, forked, beyondK, unmatched, maxDepth,
	)
}

func TestForkDepth(t *testing.T) {
	mk := func(blocks ...[2]byte) devnet.ChainSnapshot {
		var snap devnet.ChainSnapshot
		for _, b := range blocks {
			snap.Headers = append(snap.Headers, devnet.ObservedHeader{
				BlockNumber: uint64(b[0]), Hash: []byte{b[1]},
			})
			snap.Tip.BlockNumber = uint64(b[0])
		}
		return snap
	}
	long := mk([2]byte{1, 1}, [2]byte{2, 2}, [2]byte{3, 3}, [2]byte{4, 4})
	behind := mk([2]byte{1, 1}, [2]byte{2, 2})
	forked := mk([2]byte{1, 1}, [2]byte{2, 2}, [2]byte{3, 9}, [2]byte{4, 9})
	other := mk([2]byte{1, 7}, [2]byte{2, 7})
	far := mk([2]byte{50, 1})
	var empty devnet.ChainSnapshot

	d, rel := forkDepth(long, behind)
	require.Equal(t, relCommon, rel)
	require.Zero(t, d)
	d, rel = forkDepth(long, forked)
	require.Equal(t, relCommon, rel)
	require.EqualValues(t, 2, d)
	_, rel = forkDepth(long, other)
	require.Equal(t, relBeyondWindow, rel)
	_, rel = forkDepth(long, far)
	require.Equal(t, relIndeterminate, rel)
	_, rel = forkDepth(long, empty)
	require.Equal(t, relIndeterminate, rel)

	// A lagging first node must not hide a fork between the others.
	worst, _ := worstForkDepth([]devnet.ChainSnapshot{behind, long, forked}, 20)
	require.EqualValues(t, 2, worst)
	worst, _ = worstForkDepth([]devnet.ChainSnapshot{behind, long, other}, 20)
	require.EqualValues(t, 21, worst)
}
