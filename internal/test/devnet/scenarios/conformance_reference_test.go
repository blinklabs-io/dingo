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
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/blinklabs-io/dingo/internal/test/devnet"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// TestCardanoProducerChainAdvances verifies the cardano-node producer
// is also forging blocks, serving as a reference baseline. Conformance
// mode only.
func TestCardanoProducerChainAdvances(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	cardanoEndpoint, ok := h.ReferenceNode()
	require.True(t, ok, "conformance mode must have a reference node")

	h.WaitForNodeSlot(cardanoEndpoint, 0, 60*time.Second)

	initialTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get initial cardano-producer tip")

	const advanceSlots = 10
	targetSlot := initialTip.SlotNumber + advanceSlots
	timeout := time.Duration(advanceSlots)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	h.WaitForNodeSlot(cardanoEndpoint, targetSlot, timeout)

	newTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get new cardano-producer tip")
	require.Greater(t, newTip.BlockNumber, initialTip.BlockNumber,
		"cardano-producer should have forged new blocks",
	)

	t.Logf(
		"cardano-producer chain advanced from slot %d to %d"+
			" (blocks: %d -> %d)",
		initialTip.SlotNumber, newTip.SlotNumber,
		initialTip.BlockNumber, newTip.BlockNumber,
	)
}

// TestLedgerStateConsensus is Dingo's automated cross-node ledger-state
// comparison against cardano-node: it samples
// dingo-producer's and cardano-producer's ledger state (current protocol
// parameters, stake distribution, ADA pots, absolute stake snapshots, and
// the whole UTxO set) in epochs 1 and 2. This exercises both bootstrap reward
// updates instead of sampling several points within an arbitrary epoch.
//
// Each sample acquires the trailing node's tip as a specific point on both
// nodes' LocalStateQuery, so both answers describe one block both chains
// contain even while the nodes keep forging. It samples rather than visiting
// every block because each sample walks the whole UTxO set on both nodes.
func TestLedgerStateConsensus(t *testing.T) {
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

	dingoNtc := devnet.DingoProducerNtcAddr()
	cardanoNtc := devnet.CardanoProducerNtcAddr()

	initialTip, err := h.GetChainTip(dingoEP)
	require.NoError(t, err, "failed to get initial dingo-producer tip")

	const slotsBetweenSamples = 15
	sampleTimeout := time.Duration(slotsBetweenSamples)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*10
	require.Less(
		t,
		initialTip.SlotNumber,
		2*cfg.EpochLength,
		"bootstrap conformance needs a fresh devnet; run with -run TestLedgerStateConsensus",
	)

	var epoch1Pots localstatequery.AccountState
	for _, targetSlot := range []uint64{
		cfg.EpochLength + slotsBetweenSamples,
		2*cfg.EpochLength + slotsBetweenSamples,
		2*cfg.EpochLength + 2*slotsBetweenSamples,
	} {
		h.WaitForNodeSlot(
			dingoEP,
			targetSlot,
			cfg.EpochDuration()+sampleTimeout,
		)
		h.WaitForNodeSlot(cardanoEP, targetSlot, sampleTimeout)

		dingoState, cardanoState, tip := sampleLedgerStateAtCommonPoint(
			t, h, dingoEP, cardanoEP, dingoNtc, cardanoNtc, cfg.NetworkMagic,
			(targetSlot/cfg.EpochLength+1)*cfg.EpochLength,
			sampleTimeout,
		)

		epoch := tip.SlotNumber / cfg.EpochLength
		require.Equal(t, targetSlot/cfg.EpochLength, epoch,
			"stable sample missed the intended bootstrap epoch")
		diff := nodeparity.DiffSnapshots(dingoState.ledger, cardanoState.ledger)
		require.True(t, diff.Empty(),
			"ledger state diverged between dingo-producer and"+
				" cardano-producer at slot %d (block %d):\n%s",
			tip.SlotNumber, tip.BlockNumber, strings.Join(diff.Lines(), "\n"),
		)
		require.Equal(
			t,
			cardanoState.pots,
			dingoState.pots,
			"treasury/reserves diverged at epoch %d slot %d",
			epoch,
			tip.SlotNumber,
		)
		require.Equal(
			t,
			cardanoState.stake,
			dingoState.stake,
			"absolute stake snapshots diverged at epoch %d slot %d",
			epoch,
			tip.SlotNumber,
		)
		if epoch == 1 {
			require.Zero(t, cardanoState.pots.Treasury,
				"d=0 genesis has no expansion from empty previous block counts")
			epoch1Pots = cardanoState.pots
		} else {
			require.Positive(t, cardanoState.pots.Treasury,
				"epoch 0 blocks must fund the update applied in epoch 2")
			require.Less(t, cardanoState.pots.Reserves, epoch1Pots.Reserves)
		}

		t.Logf(
			"ledger state matched at slot %d (block %d):"+
				" %d utxos, %d pools in stake distribution",
			tip.SlotNumber,
			tip.BlockNumber,
			len(
				dingoState.ledger.UTxOEntries,
			),
			len(dingoState.ledger.StakeDistribution),
		)
	}
}

// sampleLedgerStateAtCommonPoint acquires one block on both nodes and samples
// their ledger state there, so both answers describe the same canonical block
// however far either node advances while the queries run.
//
// The candidate is the trailing node's tip: the leading node has normally
// applied it already, and when the two have forked its Acquire fails with
// point-not-on-chain, so two successful Acquires prove a common block. A
// candidate at or past endSlot belongs to a later epoch than the caller
// intends, and every later candidate would too, so the sample fails rather
// than retrying. Queries and retries share the sample timeout.
func sampleLedgerStateAtCommonPoint(
	t *testing.T,
	h *devnet.TestHarness,
	dingoEP, cardanoEP devnet.NodeEndpoint,
	dingoNtc, cardanoNtc string,
	magic uint32,
	endSlot uint64,
	timeout time.Duration,
) (dingoState, cardanoState *ledgerConsensusSample, tip devnet.ChainTip) {
	t.Helper()

	const pollInterval = 2 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	for attempt := 1; ; attempt++ {
		candidate, err := trailingChainTip(h, dingoEP, cardanoEP)
		if err == nil {
			require.Less(t, candidate.SlotNumber, endSlot,
				"trailing tip crossed the intended epoch before a common"+
					" point could be sampled")
			point := pcommon.NewPoint(candidate.SlotNumber, candidate.Hash)
			var ds, cs *ledgerConsensusSample
			ds, err = queryLedgerConsensusSample(ctx, dingoNtc, magic, &point)
			if err != nil {
				err = fmt.Errorf("dingo-producer at slot %d: %w",
					candidate.SlotNumber, err)
			} else {
				cs, err = queryLedgerConsensusSample(
					ctx, cardanoNtc, magic, &point,
				)
				if err != nil {
					err = fmt.Errorf("cardano-producer at slot %d: %w",
						candidate.SlotNumber, err)
				}
			}
			if err == nil {
				return ds, cs, candidate
			}
		}
		t.Logf("sampleLedgerStateAtCommonPoint: attempt %d: %v", attempt, err)
		select {
		case <-ctx.Done():
			require.FailNowf(t,
				"no common point sampled",
				"dingo-producer and cardano-producer could not both acquire"+
					" a common point within %s: %v",
				timeout, err,
			)
		case <-time.After(pollInterval):
		}
	}
}

// trailingChainTip returns the tip of whichever node is behind.
func trailingChainTip(
	h *devnet.TestHarness, dingoEP, cardanoEP devnet.NodeEndpoint,
) (devnet.ChainTip, error) {
	dingoTip, err := h.GetChainTip(dingoEP)
	if err != nil {
		return devnet.ChainTip{}, fmt.Errorf("dingo-producer tip: %w", err)
	}
	cardanoTip, err := h.GetChainTip(cardanoEP)
	if err != nil {
		return devnet.ChainTip{}, fmt.Errorf("cardano-producer tip: %w", err)
	}
	if cardanoTip.SlotNumber < dingoTip.SlotNumber {
		return cardanoTip, nil
	}
	return dingoTip, nil
}

type ledgerConsensusSample struct {
	ledger *nodeparity.Snapshot
	pots   localstatequery.AccountState
	stake  *localstatequery.StakeSnapshotsResult
}

// Both acquires name point, so the snapshot and the account and stake queries
// describe the same block.
func queryLedgerConsensusSample(
	ctx context.Context, addr string, magic uint32, point *pcommon.Point,
) (*ledgerConsensusSample, error) {
	conn, err := nodeparity.Dial(ctx, addr, magic)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	snapshot, err := nodeparity.QuerySnapshot(conn, point)
	if err != nil {
		return nil, err
	}
	client := conn.LocalStateQuery().Client
	if err := client.Acquire(point); err != nil {
		return nil, fmt.Errorf("acquire point: %w", err)
	}
	defer client.Release() //nolint:errcheck
	pots, err := client.GetAccountState()
	if err != nil {
		return nil, fmt.Errorf("account state: %w", err)
	}
	stake, err := client.GetStakeSnapshots(nil)
	if err != nil {
		return nil, fmt.Errorf("stake snapshots: %w", err)
	}
	return &ledgerConsensusSample{
		ledger: snapshot,
		pots:   pots.State,
		stake:  stake,
	}, nil
}
