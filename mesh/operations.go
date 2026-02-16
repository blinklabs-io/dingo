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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

// Operation type constants matching the Cardano Foundation's
// cardano-rosetta-java implementation.
const (
	OpInput                    = "input"
	OpOutput                   = "output"
	OpStakeKeyRegistration     = "stakeKeyRegistration"
	OpStakeKeyDeregistration   = "stakeKeyDeregistration"
	OpStakeDelegation          = "stakeDelegation"
	OpWithdrawal               = "withdrawal"
	OpPoolRegistration         = "poolRegistration"
	OpPoolRegistrationWithCert = "poolRegistrationWithCert"
	OpPoolRetirement           = "poolRetirement"
	OpVoteDRepDelegation       = "dRepVoteDelegation"
	OpPoolGovernanceVote       = "poolGovernanceVote"
)

// Operation status constants.
const (
	StatusSuccess = "success"
	StatusInvalid = "invalid"
)

// OperationTypes returns all supported operation types.
func OperationTypes() []string {
	return []string{
		OpInput,
		OpOutput,
		OpStakeKeyRegistration,
		OpStakeKeyDeregistration,
		OpStakeDelegation,
		OpWithdrawal,
		OpPoolRegistration,
		OpPoolRegistrationWithCert,
		OpPoolRetirement,
		OpVoteDRepDelegation,
		OpPoolGovernanceVote,
	}
}

// OperationStatuses returns the supported operation statuses.
func OperationStatuses() []*OperationStatus {
	return []*OperationStatus{
		{Status: StatusSuccess, Successful: true},
		{Status: StatusInvalid, Successful: false},
	}
}
