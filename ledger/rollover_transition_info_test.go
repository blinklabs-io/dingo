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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/assert"
)

func TestApplyRolloverTransitionInfo(t *testing.T) {
	t.Parallel()
	hardForkInfo := &HardForkInfo{
		OldVersion: ProtocolVersion{Major: 8},
		NewVersion: ProtocolVersion{Major: 9},
	}
	rollover := func(epoch uint64, hardFork *HardForkInfo) *EpochRolloverResult {
		return &EpochRolloverResult{
			NewCurrentEpoch: models.Epoch{EpochId: epoch},
			HardFork:        hardFork,
		}
	}
	tests := []struct {
		name        string
		start       hardfork.TransitionInfo
		eraChanges  int
		result      *EpochRolloverResult
		wantState   hardfork.TransitionState
		wantKnownAt uint64
	}{
		{
			name:      "plain rollover resets a stale TransitionImpossible",
			start:     hardfork.NewTransitionImpossible(),
			result:    rollover(501, nil),
			wantState: hardfork.TransitionUnknown,
		},
		{
			name:      "plain rollover keeps TransitionUnknown",
			start:     hardfork.NewTransitionUnknown(),
			result:    rollover(501, nil),
			wantState: hardfork.TransitionUnknown,
		},
		{
			name:        "version bump without an era change sets TransitionKnown",
			start:       hardfork.NewTransitionUnknown(),
			result:      rollover(500, hardForkInfo),
			wantState:   hardfork.TransitionKnown,
			wantKnownAt: 500,
		},
		{
			name:      "no rollover result leaves the state alone",
			start:     hardfork.NewTransitionImpossible(),
			result:    nil,
			wantState: hardfork.TransitionImpossible,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ls := &LedgerState{transitionInfo: tt.start}
			ls.applyRolloverTransitionInfo(tt.eraChanges, tt.result)
			assert.Equal(t, tt.wantState, ls.transitionInfo.State)
			assert.Equal(t, tt.wantKnownAt, ls.transitionInfo.KnownEpoch)
		})
	}
}

// An era change clears transitionInfo through applyEraTransition, and the
// rollover step must not set TransitionKnown again for the same boundary even
// when the rollover carries a version bump.
func TestEraTransitionRolloverClearsTransitionInfo(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionKnown(499),
	}
	eraTransitions := []*EraTransitionResult{{
		NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewPParams: babbagePParams(9),
	}}
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{EpochId: 500},
		HardFork: &HardForkInfo{
			OldVersion: ProtocolVersion{Major: 8},
			NewVersion: ProtocolVersion{Major: 9},
		},
	}
	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	ls.applyRolloverTransitionInfo(len(eraTransitions), rolloverResult)
	ls.Unlock()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}
