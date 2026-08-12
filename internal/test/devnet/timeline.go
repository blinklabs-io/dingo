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
	"errors"
	"fmt"
	"strings"
	"time"
)

// Phase names on the shared scenario timeline.
const (
	PhaseReadiness        = "readiness"
	PhasePropagation      = "propagation"
	PhaseAgreement        = "agreement"
	PhaseEpochTransition  = "epoch-transition"
	PhasePeerInterruption = "peer-interruption"
	PhaseRelayRestart     = "relay-restart"
)

// ReferenceRunnerBudget is the wall-clock ceiling for one accelerated
// scenario, excluding image build, on the reference runner documented in
// internal/test/devnet/README.md. It is a hard timeout, not a target: the
// scenario is event-driven and normally finishes well inside it.
const ReferenceRunnerBudget = 5 * time.Minute

// startupBudget is the allowance for every node to accept a ChainSync
// session and send its first header once the compose health checks have
// passed. It is wall-clock rather than slot-derived because it covers
// container and database start, which the network's timing parameters
// say nothing about.
const startupBudget = 45 * time.Second

// TimelinePhase is one stage of the shared scenario timeline.
//
// Deadline is measured from the scenario's start, not from the end of the
// previous phase. That is what makes this one timeline rather than a
// series of independent waits: a phase that completes early hands its
// slack to everything after it, and no assertion adds its own relative
// slot window on top of the phases before it.
type TimelinePhase struct {
	Name     string
	Deadline time.Duration
}

// ScenarioPlan is the deterministic schedule an accelerated DevNet run
// follows. Every budget is derived from the network's own timing
// parameters, so changing the spec moves the schedule with it instead of
// silently invalidating hardcoded waits.
type ScenarioPlan struct {
	Config *DevNetConfig
	Phases []TimelinePhase

	// HardTimeout bounds the whole scenario.
	HardTimeout time.Duration

	// InterruptionHold is how long a node stays down during the
	// interruption and restart phases.
	InterruptionHold time.Duration

	// RecoveryBudget is the allowance for a restarted node to rejoin and
	// catch back up to the network's chain.
	RecoveryBudget time.Duration

	// OutageBlocks is how far the rest of the network must advance while
	// a node is stopped. Holding the outage to observed chain progress
	// rather than wall clock keeps the disruption meaningful on a fast
	// or slow runner alike, and keeps it inside what k can reconcile.
	OutageBlocks uint64

	// EpochMargin is how far past an epoch boundary the scenario runs
	// before asserting agreement, so the assertion covers headers built
	// on the new epoch nonce rather than the boundary block alone.
	EpochMargin uint64
}

// NewScenarioPlan derives the schedule for a network spec. It rejects a
// spec that is not internally consistent, since a plan built on one would
// describe a network that cannot run.
func NewScenarioPlan(cfg *DevNetConfig) (*ScenarioPlan, error) {
	if cfg == nil {
		return nil, errors.New("devnet: nil config")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("devnet: invalid network spec: %w", err)
	}

	blockTime := cfg.ExpectedBlockTime()

	// A stopped node must come back well inside the k-block window: the
	// point is to exercise recovery, not to push the chain past what the
	// security parameter can reconcile. Half of k leaves the recovering
	// node a chain the others can still serve from.
	outageBlocks := max(2, cfg.SecurityParam/2)
	hold := SlotsDuration(outageBlocks, blockTime)
	recovery := 30 * blockTime

	plan := &ScenarioPlan{
		Config:           cfg,
		HardTimeout:      ReferenceRunnerBudget,
		InterruptionHold: hold,
		RecoveryBudget:   recovery,
		OutageBlocks:     outageBlocks,
		// A quarter of an epoch past the boundary is comfortably more
		// than the 4k/f freeze window's tail while staying cheap.
		EpochMargin: cfg.EpochLength / 4,
	}

	// Deadlines accumulate along one clock.
	readiness := startupBudget
	propagation := readiness + 20*blockTime
	agreement := propagation + 10*blockTime
	// Worst case the scenario attaches immediately after a boundary and
	// has to wait a full epoch for the next one.
	epoch := agreement + cfg.EpochDuration() +
		SlotsDuration(plan.EpochMargin, cfg.SlotDuration()) + 10*blockTime
	peerInterruption := epoch + hold + recovery
	relayRestart := peerInterruption + hold + recovery

	plan.Phases = []TimelinePhase{
		{Name: PhaseReadiness, Deadline: readiness},
		{Name: PhasePropagation, Deadline: propagation},
		{Name: PhaseAgreement, Deadline: agreement},
		{Name: PhaseEpochTransition, Deadline: epoch},
		{Name: PhasePeerInterruption, Deadline: peerInterruption},
		{Name: PhaseRelayRestart, Deadline: relayRestart},
	}
	return plan, nil
}

// Phase returns the named phase.
func (p *ScenarioPlan) Phase(name string) (TimelinePhase, bool) {
	for _, ph := range p.Phases {
		if ph.Name == name {
			return ph, true
		}
	}
	return TimelinePhase{}, false
}

// Total returns the scenario length: the last phase's deadline on the
// shared clock.
func (p *ScenarioPlan) Total() time.Duration {
	if len(p.Phases) == 0 {
		return 0
	}
	return p.Phases[len(p.Phases)-1].Deadline
}

// FitsReferenceBudget reports whether the plan fits the documented
// reference-runner budget.
func (p *ScenarioPlan) FitsReferenceBudget() bool {
	return p.Total() <= ReferenceRunnerBudget
}

// String renders the schedule for the scenario log, so a run records the
// timeline it was held to.
func (p *ScenarioPlan) String() string {
	var out strings.Builder
	fmt.Fprintf(&out,
		"scenario plan (epoch %s, block ~%s, hard timeout %s):",
		p.Config.EpochDuration(), p.Config.ExpectedBlockTime(),
		p.HardTimeout,
	)
	for _, ph := range p.Phases {
		fmt.Fprintf(&out, " %s<=%s", ph.Name, ph.Deadline)
	}
	return out.String()
}
