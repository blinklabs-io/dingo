//go:build devnet && !devnet_conformance

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
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/stretchr/testify/require"
)

// TestCIP50PledgeLeverageRewardEffect verifies that enabling CIP-50 pledge
// leverage on a zero-pledge all-dingo network drives member rewards to zero,
// by comparing two independent runs against the same network configuration
// (testnet-dingo.yaml, poolPledge=0):
//
//   - baseline (leverage off): rewards flow normally, so total member rewards
//     across delegated stake credentials must be greater than zero.
//   - leveraged (DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED=true): the reward
//     formula caps reward-eligible stake at L*pledgeFraction = L*0 = 0, so
//     every pool earns zero rewards and no delegated stake credential
//     accrues a reward. See ledger/rewards/rewards.go:1157-1166.
//
// A single-run, leverage-only assertion is vacuous: dingo applies rewards as
// a delayed update from the stake snapshot three epochs back (see
// ledger/chainsync.go), so member rewards are still zero at epoch 1
// regardless of whether leverage is enabled. This test instead waits into
// epoch 4 so the delayed update has had a chance to credit rewards, and
// requires a baseline (leverage-off) pass to show rewards are non-zero
// before treating a leveraged all-zero result as meaningful.
//
// Because each pass takes multiple epochs (~35 minutes at epochLength=500,
// slotLength=1s) this test is gated behind DEVNET_CIP50_TEST=1 and never
// runs as part of the default suite. Run it twice, once per network
// configuration - see the README for exact invocations:
//
//	# baseline: network launched with leverage off
//	DEVNET_CIP50_TEST=1 DEVNET_STAKE_KEYS_DIR=... \
//	  go test -tags devnet -run TestCIP50PledgeLeverageRewardEffect ...
//
//	# leveraged: network launched with DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED=true
//	DEVNET_CIP50_TEST=1 DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED=true \
//	  DEVNET_STAKE_KEYS_DIR=... \
//	  go test -tags devnet -run TestCIP50PledgeLeverageRewardEffect ...
func TestCIP50PledgeLeverageRewardEffect(t *testing.T) {
	if os.Getenv("DEVNET_CIP50_TEST") != "1" {
		t.Skip("long-running CIP-50 effect test; set DEVNET_CIP50_TEST=1 and run against a freshly brought-up network (see doc comment). Run a baseline (leverage off) pass and a leveraged (DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED=true) pass.")
	}
	leveraged := os.Getenv("DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED") == "true"

	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config")

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(t, endpoints, devnet.WithNetworkMagic(cfg.NetworkMagic))
	h.WaitForAllNodesReady(60 * time.Second)
	observed := h.DingoNode()

	// Wait into epoch 4 so the delayed reward update (three epochs back) has
	// credited genesis-delegated stake in a non-leveraged network.
	targetSlot := 4*cfg.EpochLength + cfg.EpochLength/5
	boundaryTimeout := time.Duration(targetSlot)*cfg.SlotDuration() + cfg.ExpectedBlockTime()*30
	t.Logf("waiting for slot %d (into epoch 4), timeout %s", targetSlot, boundaryTimeout)
	h.WaitForNodeSlot(observed, targetSlot, boundaryTimeout)

	tip, err := h.GetChainTip(observed)
	require.NoError(t, err, "failed to read observed tip")
	require.Greater(t, tip.BlockNumber, uint64(0), "network must have forged blocks")

	stakeDir := os.Getenv("DEVNET_STAKE_KEYS_DIR")
	require.NotEmpty(t, stakeDir, "set DEVNET_STAKE_KEYS_DIR to the exposed genesis stake key dir")
	creds, err := devnet.LoadGenesisStakeCredentials(stakeDir)
	require.NoError(t, err, "failed to load genesis stake credentials")
	require.NotEmpty(t, creds, "expected at least one delegated stake credential")

	ntcAddr := devnet.DingoNtcAddrs()[observed.Name]
	require.NotEmpty(t, ntcAddr, "no NtC address for %s", observed.Name)

	var rewards map[string]devnet.RewardResult
	require.Eventually(t, func() bool {
		rewards, err = devnet.RewardAccountsByNtcForCreds(ntcAddr, cfg.NetworkMagic, creds)
		if err != nil {
			t.Logf("reward query error (retrying): %v", err)
			return false
		}
		return true
	}, 60*time.Second, 3*time.Second, "reward account query never succeeded")

	delegated := 0
	var total uint64
	for _, sr := range rewards {
		if sr.Delegated {
			delegated++
		}
		total += sr.Reward
	}
	require.Greater(t, delegated, 0, "expected genesis to have delegated stake to pools")

	if leveraged {
		for k, sr := range rewards {
			require.Equalf(t, uint64(0), sr.Reward,
				"pledge leverage with zero pledge must zero member rewards; cred %s has %d", k, sr.Reward)
		}
		t.Logf("CIP-50 leveraged: all %d delegated credentials have zero rewards into epoch 4 (cap engaged)", delegated)
	} else {
		require.Greater(t, total, uint64(0),
			"baseline (leverage off): zero-pledge delegated stake must earn positive rewards by epoch 4")
		t.Logf("CIP-50 baseline: total member rewards %d across %d delegated credentials (rewards flow without cap)", total, delegated)
	}
}
