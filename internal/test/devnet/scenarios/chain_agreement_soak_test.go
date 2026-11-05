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

// forkDepth returns how many blocks the longer of two observed chains holds
// beyond their deepest common block, matched by hash at the same block
// number. It is 0 when one tip is an ancestor of the other. ok is false when
// the retained headers share no block, so no depth can be stated.
func forkDepth(a, b devnet.ChainSnapshot) (depth uint64, ok bool) {
	byBlock := make(map[uint64]string, len(b.Headers))
	for _, h := range b.Headers {
		byBlock[h.BlockNumber] = hex.EncodeToString(h.Hash)
	}
	var common uint64
	for _, h := range a.Headers {
		if bh, have := byBlock[h.BlockNumber]; have &&
			bh == hex.EncodeToString(h.Hash) && h.BlockNumber >= common {
			common = h.BlockNumber
			ok = true
		}
	}
	if !ok {
		return 0, false
	}
	return max(a.Tip.BlockNumber, b.Tip.BlockNumber) - common, true
}

// TestChainAgreementSoak follows every node's chain for
// DEVNET_AGREEMENT_SOAK_DURATION and fails if two nodes' chains ever diverge
// by more than the security parameter. A fork shallower than k is normal
// slot-battle behaviour; one deeper than k cannot be resolved and is a
// permanent split. Tips and the fork depth are logged every sample so the run
// is evidence of what the chains did, not only of the verdict.
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
		// Every node is compared with the first one (dingo-producer).
		var worst uint64
		for _, s := range snaps[1:] {
			d, ok := forkDepth(snaps[0], s)
			if !ok {
				unmatched++
				continue
			}
			worst = max(worst, d)
		}
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
