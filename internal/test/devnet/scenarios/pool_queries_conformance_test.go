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
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/blinklabs-io/gouroboros/ledger"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// poolQuerySample is what one node answers to the pool queries at one point.
type poolQuerySample struct {
	provenance *localstatequery.RewardProvenanceResult
	poolState  *localstatequery.PoolStateResult
}

func queryPoolSample(
	ctx context.Context, addr string, magic uint32, point *pcommon.Point,
) (*poolQuerySample, error) {
	conn, err := nodeparity.Dial(ctx, addr, magic)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	client := conn.LocalStateQuery().Client
	if err := client.Acquire(point); err != nil {
		return nil, fmt.Errorf("acquire point: %w", err)
	}
	defer client.Release() //nolint:errcheck
	sample := &poolQuerySample{}
	if sample.provenance, err = client.GetRewardProvenance(); err != nil {
		return nil, fmt.Errorf("reward provenance: %w", err)
	}
	if sample.poolState, err = client.GetPoolState(nil); err != nil {
		return nil, fmt.Errorf("pool state: %w", err)
	}
	return sample, nil
}

// poolStateSummary renders each pool's registered parameters for comparison.
// The reward account and relays are left out: the devnet's pools are
// registered by genesis, and Dingo's pinned genesis parser reads those two
// fields of a cardano-node genesis pool differently from the node, so they
// cannot be compared until it reads them alike. Every other field must match.
func poolStateSummary(
	params map[ledger.Blake2b224]*localstatequery.PoolStateParams,
) map[string]string {
	out := make(map[string]string, len(params))
	for pool, p := range params {
		if p == nil {
			out[fmt.Sprintf("%x", pool[:])] = "<nil>"
			continue
		}
		out[fmt.Sprintf("%x", pool[:])] = fmt.Sprintf(
			"operator=%x vrf=%x pledge=%d cost=%d margin=%s owners=%x metadata=%v",
			p.Operator[:], p.VrfKeyHash[:], p.Pledge, p.Cost,
			p.Margin.String(), p.PoolOwners, p.PoolMetadata,
		)
	}
	return out
}

// TestPoolQueriesMatchReference compares dingo-producer's answers to
// GetRewardProvenance and GetPoolState with cardano-producer's at one block
// both chains contain.
func TestPoolQueriesMatchReference(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")
	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)
	dingoEP := h.DingoNode()
	cardanoEP, ok := h.ReferenceNode()
	require.True(t, ok, "conformance mode must have a reference node")
	h.WaitForAllNodesReady(60 * time.Second)

	// Past the first epoch the reward machinery has run at least once on both
	// nodes, so a default provenance is distinguishable from an unanswered one.
	target := cfg.EpochLength + 15
	timeout := cfg.EpochDuration() + 30*time.Second
	h.WaitForNodeSlot(dingoEP, target, timeout)
	h.WaitForNodeSlot(cardanoEP, target, timeout)

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var dingoSample, cardanoSample *poolQuerySample
	require.Eventually(t, func() bool {
		tip, err := trailingChainTip(h, dingoEP, cardanoEP)
		if err != nil {
			return false
		}
		point := pcommon.NewPoint(tip.SlotNumber, tip.Hash)
		dingoSample, err = queryPoolSample(
			ctx, devnet.DingoProducerNtcAddr(), cfg.NetworkMagic, &point,
		)
		if err != nil {
			t.Logf("dingo-producer at slot %d: %v", tip.SlotNumber, err)
			return false
		}
		cardanoSample, err = queryPoolSample(
			ctx, devnet.CardanoProducerNtcAddr(), cfg.NetworkMagic, &point,
		)
		if err != nil {
			t.Logf("cardano-producer at slot %d: %v", tip.SlotNumber, err)
			return false
		}
		return true
	}, timeout, 2*time.Second, "no common point answered by both nodes")

	require.Equal(t, cardanoSample.provenance, dingoSample.provenance,
		"GetRewardProvenance diverged")
	require.NotEmpty(t, cardanoSample.poolState.PState)
	require.Equal(
		t,
		poolStateSummary(cardanoSample.poolState.PState),
		poolStateSummary(dingoSample.poolState.PState),
		"GetPoolState parameters diverged",
	)
	require.Equal(t, cardanoSample.poolState.Deposits, dingoSample.poolState.Deposits,
		"GetPoolState deposits diverged")
	require.Equal(t, cardanoSample.poolState.Retiring, dingoSample.poolState.Retiring,
		"GetPoolState retirements diverged")
	require.Equal(
		t,
		poolStateSummary(cardanoSample.poolState.FState),
		poolStateSummary(dingoSample.poolState.FState),
		"GetPoolState future parameters diverged",
	)
}
