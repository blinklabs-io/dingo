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

// MigrateModels contains a list of model objects that should have DB migrations applied
var MigrateModels = []any{
	&Account{},
	&Asset{},
	&AuthCommitteeHot{},
	&BlockNonce{},
	&Certificate{},
	&Constitution{},
	&Datum{},
	&Deregistration{},
	&DeregistrationDrep{},
	&Drep{},
	&Epoch{},
	&EpochSummary{},
	&GovernanceProposal{},
	&GovernanceVote{},
	&ImportCheckpoint{},
	&KeyWitness{},
	&MoveInstantaneousRewards{},
	&MoveInstantaneousRewardsReward{},
	&NetworkState{},
	&Pool{},
	&PoolRegistration{},
	&PoolRegistrationOwner{},
	&PoolRegistrationRelay{},
	&PoolRetirement{},
	&PoolStakeSnapshot{},
	&PParams{},
	&PParamUpdate{},
	&PlutusData{},
	&Registration{},
	&RegistrationDrep{},
	&Redeemer{},
	&ResignCommitteeCold{},
	&Script{},
	&StakeDelegation{},
	&StakeDeregistration{},
	&StakeRegistration{},
	&StakeRegistrationDelegation{},
	&StakeVoteDelegation{},
	&StakeVoteRegistrationDelegation{},
	&SyncState{},
	&Tip{},
	&Transaction{},
	&TransactionMetadataLabel{},
	&UpdateDrep{},
	&Utxo{},
	&VoteDelegation{},
	&VoteRegistrationDelegation{},
	&WitnessScripts{},
}

// ImportCheckpoint tracks the progress of a Mithril snapshot import
// so that it can be resumed after a failure without re-importing
// already completed phases.
type ImportCheckpoint struct {
	ID        uint   `gorm:"primarykey"`
	ImportKey string `gorm:"size:255;uniqueIndex;not null"` // "{digest}:{slot}"
	Phase     string `gorm:"not null"`                      // last completed phase
}

func (ImportCheckpoint) TableName() string {
	return "import_checkpoint"
}

// Import phases in execution order.
const (
	ImportPhaseUTxO      = "utxo"
	ImportPhaseCertState = "certstate"
	ImportPhaseSnapshots = "snapshots"
	ImportPhasePParams   = "pparams"
	ImportPhaseGovState  = "gov_state"
	ImportPhaseTip       = "tip"
)

// ImportPhaseOrder defines the sequential order of import phases.
var ImportPhaseOrder = []string{
	ImportPhaseUTxO,
	ImportPhaseCertState,
	ImportPhaseSnapshots,
	ImportPhasePParams,
	ImportPhaseGovState,
	ImportPhaseTip,
}

// IsPhaseCompleted returns true if the given phase was completed
// in a previous import run (i.e., the checkpoint phase is at or
// past the given phase).
func IsPhaseCompleted(checkpointPhase, queryPhase string) bool {
	cpIdx := -1
	qIdx := -1
	for i, p := range ImportPhaseOrder {
		if p == checkpointPhase {
			cpIdx = i
		}
		if p == queryPhase {
			qIdx = i
		}
	}
	if cpIdx < 0 || qIdx < 0 {
		return false
	}
	return cpIdx >= qIdx
}
