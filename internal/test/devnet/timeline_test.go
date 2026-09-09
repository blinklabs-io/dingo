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

package devnet

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// acceleratedTestConfig mirrors testnet-dingo-accelerated.yaml. The
// on-disk specs are checked separately by TestCheckedInSpecsAreValid.
func acceleratedTestConfig() *DevNetConfig {
	return &DevNetConfig{
		PoolCount:        3,
		NetworkMagic:     42,
		EpochLength:      120,
		SlotLength:       0.5,
		ActiveSlotsCoeff: 0.4,
		SecurityParam:    10,
	}
}

func canonicalTestConfig() *DevNetConfig {
	return &DevNetConfig{
		PoolCount:        3,
		NetworkMagic:     42,
		EpochLength:      500,
		SlotLength:       1,
		ActiveSlotsCoeff: 0.4,
		SecurityParam:    40,
	}
}

func TestStabilityWindowsDerivedFromSecurityParam(t *testing.T) {
	cfg := acceleratedTestConfig()
	// 4k/f and 3k/f, the windows cardano-node derives from k and f.
	require.Equal(t, uint64(100), cfg.NonceStabilityWindowSlots())
	require.Equal(t, uint64(75), cfg.BlockFetchStabilityWindowSlots())
	require.Equal(t, 60*time.Second, cfg.EpochDuration())
}

func TestValidateRejectsStabilityWindowOverrunningEpoch(t *testing.T) {
	cfg := acceleratedTestConfig()
	require.NoError(t, cfg.Validate())

	// 4k/f = 100 slots; an epoch shorter than that cannot freeze the
	// candidate nonce before the boundary.
	cfg.EpochLength = 90
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonce stability window")

	cfg = acceleratedTestConfig()
	// 3k/f = 75 slots, so an 80-slot epoch clears blockfetch but not
	// the nonce window; check the blockfetch bound on its own.
	cfg.SecurityParam = 20 // 3k/f = 150, 4k/f = 200
	cfg.EpochLength = 175  // above blockfetch, below nonce
	err = cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonce stability window")

	cfg.EpochLength = 120 // below both
	err = cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "blockfetch stability window")
}

func TestValidateRejectsDegenerateParameters(t *testing.T) {
	for name, mutate := range map[string]func(*DevNetConfig){
		"zero pools":     func(c *DevNetConfig) { c.PoolCount = 0 },
		"zero magic":     func(c *DevNetConfig) { c.NetworkMagic = 0 },
		"zero slot":      func(c *DevNetConfig) { c.SlotLength = 0 },
		"zero k":         func(c *DevNetConfig) { c.SecurityParam = 0 },
		"zero epoch":     func(c *DevNetConfig) { c.EpochLength = 0 },
		"coeff above 1":  func(c *DevNetConfig) { c.ActiveSlotsCoeff = 1.5 },
		"coeff at zero":  func(c *DevNetConfig) { c.ActiveSlotsCoeff = 0 },
		"negative coeff": func(c *DevNetConfig) { c.ActiveSlotsCoeff = -0.1 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := acceleratedTestConfig()
			mutate(cfg)
			require.Error(t, cfg.Validate())
		})
	}
}

func TestNextEpochBoundary(t *testing.T) {
	cfg := acceleratedTestConfig() // epochLength 120

	// From inside epoch 0 the next boundary is the start of epoch 1.
	require.Equal(t, uint64(120), cfg.NextEpochBoundary(0))
	require.Equal(t, uint64(120), cfg.NextEpochBoundary(1))
	require.Equal(t, uint64(120), cfg.NextEpochBoundary(119))
	// Standing exactly on a boundary, the next one is a full epoch out,
	// so a scenario attaching to a long-running network still crosses a
	// transition rather than trivially passing on the current slot.
	require.Equal(t, uint64(240), cfg.NextEpochBoundary(120))
	require.Equal(t, uint64(360), cfg.NextEpochBoundary(250))
}

func TestScenarioPlanPhasesAreOrderedAndBounded(t *testing.T) {
	plan, err := NewScenarioPlan(acceleratedTestConfig())
	require.NoError(t, err)

	names := make([]string, 0, len(plan.Phases))
	var last time.Duration
	for _, p := range plan.Phases {
		require.Greater(t, p.Deadline, last,
			"phase %q deadline must advance the shared timeline", p.Name)
		last = p.Deadline
		names = append(names, p.Name)
	}
	require.Equal(t, []string{
		PhaseReadiness,
		PhasePropagation,
		PhaseAgreement,
		PhaseEpochTransition,
		PhasePeerInterruption,
		PhaseRelayRestart,
	}, names)

	// Every assertion hangs off one clock, so the last deadline is the
	// scenario length rather than a sum of independent waits.
	require.Equal(t, last, plan.Total())
	require.Less(t, plan.Total(), plan.HardTimeout,
		"the plan must finish inside its own hard timeout")
}

// The five-minute reference-runner budget in the issue is the whole point
// of the accelerated spec: assert the accelerated parameters fit and the
// canonical ones do not, so nobody can quietly point the fast scenario at
// the soak configuration.
func TestAcceleratedPlanFitsReferenceBudgetAndCanonicalDoesNot(t *testing.T) {
	fast, err := NewScenarioPlan(acceleratedTestConfig())
	require.NoError(t, err)
	require.LessOrEqual(t, fast.Total(), ReferenceRunnerBudget,
		"accelerated scenario must fit the documented runner budget")
	require.Equal(t, ReferenceRunnerBudget, fast.HardTimeout)

	slow, err := NewScenarioPlan(canonicalTestConfig())
	require.NoError(t, err)
	require.Greater(t, slow.Total(), ReferenceRunnerBudget,
		"the canonical timing config cannot meet the fast budget; it "+
			"stays available for soak and canary runs")
}

func TestScenarioPlanPhaseLookup(t *testing.T) {
	plan, err := NewScenarioPlan(acceleratedTestConfig())
	require.NoError(t, err)

	epoch, ok := plan.Phase(PhaseEpochTransition)
	require.True(t, ok)
	require.Equal(t, PhaseEpochTransition, epoch.Name)

	_, ok = plan.Phase("no-such-phase")
	require.False(t, ok)
}

// A stopped producer must be brought back well inside the k-block window;
// holding it down longer risks a recovery the security parameter cannot
// cover, which would make the scenario flaky rather than diagnostic.
func TestInterruptionHoldStaysInsideSecurityWindow(t *testing.T) {
	cfg := acceleratedTestConfig()
	plan, err := NewScenarioPlan(cfg)
	require.NoError(t, err)

	kWindow := time.Duration(cfg.SecurityParam) * cfg.ExpectedBlockTime()
	require.Less(t, plan.InterruptionHold, kWindow,
		"interruption must stay under k blocks of wall clock")
	require.Positive(t, plan.RecoveryBudget)
	require.Greater(t, plan.RecoveryBudget, plan.InterruptionHold,
		"recovery needs more room than the outage itself")
}

func TestNewScenarioPlanRejectsInvalidConfig(t *testing.T) {
	cfg := acceleratedTestConfig()
	cfg.EpochLength = 10 // far below both stability windows
	_, err := NewScenarioPlan(cfg)
	require.Error(t, err)
}

func TestOutageBlocksStayInsideSecurityParam(t *testing.T) {
	cfg := acceleratedTestConfig()
	plan, err := NewScenarioPlan(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(5), plan.OutageBlocks)
	require.Less(t, plan.OutageBlocks, cfg.SecurityParam,
		"the network must not outrun what k can reconcile while a node "+
			"is down")
	require.Equal(t,
		time.Duration(plan.OutageBlocks)*cfg.ExpectedBlockTime(),
		plan.InterruptionHold,
	)

	// A tiny k still yields a disruption worth making.
	cfg.SecurityParam = 2
	cfg.EpochLength = 120
	plan, err = NewScenarioPlan(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(2), plan.OutageBlocks)
}
