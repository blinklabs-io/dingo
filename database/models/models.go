// Copyright 2025 Blink Labs Software
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

package models

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ImportCheckpoint tracks the progress of a Mithril snapshot import
// so that it can be resumed after a failure without re-importing
// already completed phases.
type ImportCheckpoint struct {
	ID        uint
	ImportKey string // "{digest}:{slot}"
	Phase     string // last completed phase
}

// Import phases in execution order.
const (
	ImportPhaseUTxO            = "utxo"
	ImportPhaseCertState       = "certstate"
	ImportPhaseSnapshots       = "snapshots"
	ImportPhasePParams         = "pparams"
	ImportPhaseGovState        = "gov_state"
	ImportPhaseRewardLiveStake = "reward_live_stake"
	ImportPhaseTip             = "tip"
)

// ImportPhaseOrder defines the sequential order of import phases.
var ImportPhaseOrder = []string{
	ImportPhaseUTxO,
	ImportPhaseCertState,
	ImportPhaseSnapshots,
	ImportPhasePParams,
	ImportPhaseGovState,
	ImportPhaseRewardLiveStake,
	ImportPhaseTip,
}

const importRewardLiveStakeCursorSeparator = ":"

// RewardLiveStakeImportCheckpoint formats an in-progress reward live-stake
// import checkpoint. An empty cursor records that initialization completed but
// no credential range has committed yet.
func RewardLiveStakeImportCheckpoint(cursor []byte) string {
	return ImportPhaseRewardLiveStake + importRewardLiveStakeCursorSeparator +
		hex.EncodeToString(cursor)
}

// RewardLiveStakeImportCursor decodes an in-progress reward live-stake import
// checkpoint. The completed phase is not an in-progress checkpoint.
func RewardLiveStakeImportCursor(phase string) ([]byte, bool, error) {
	prefix := ImportPhaseRewardLiveStake + importRewardLiveStakeCursorSeparator
	if !strings.HasPrefix(phase, prefix) {
		return nil, false, nil
	}
	cursor, err := hex.DecodeString(strings.TrimPrefix(phase, prefix))
	if err != nil {
		return nil, true, fmt.Errorf("decode reward live stake import cursor: %w", err)
	}
	return cursor, true, nil
}

// IsPhaseCompleted returns true if the given phase was completed
// in a previous import run (i.e., the checkpoint phase is at or
// past the given phase).
func IsPhaseCompleted(checkpointPhase, queryPhase string) bool {
	normalizedCheckpoint := checkpointPhase
	_, rewardLiveStakeInProgress, err := RewardLiveStakeImportCursor(checkpointPhase)
	if err == nil && rewardLiveStakeInProgress {
		normalizedCheckpoint = ImportPhaseRewardLiveStake
	}
	cpIdx := -1
	qIdx := -1
	for i, p := range ImportPhaseOrder {
		if p == normalizedCheckpoint {
			cpIdx = i
		}
		if p == queryPhase {
			qIdx = i
		}
	}
	if cpIdx < 0 || qIdx < 0 {
		return false
	}
	if rewardLiveStakeInProgress && queryPhase == ImportPhaseRewardLiveStake {
		return false
	}
	return cpIdx >= qIdx
}
