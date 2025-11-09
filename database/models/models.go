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

// MigrateModels contains a list of model objects that should have DB migrations
// applied
var MigrateModels = []any{
	&Account{},
	&Asset{},
	&AuthCommitteeHot{},
	&BlockNonce{},
	&Certificate{},
	&Datum{},
	&Deregistration{},
	&DeregistrationDrep{},
	&Drep{},
	&Epoch{},
	&Pool{},
	&PoolRegistration{},
	&PoolRegistrationOwner{},
	&PoolRegistrationRelay{},
	&PoolRetirement{},
	&PParams{},
	&PParamUpdate{},
	&Registration{},
	&RegistrationDrep{},
	&ResignCommitteeCold{},
	&StakeDelegation{},
	&StakeDeregistration{},
	&StakeRegistration{},
	&StakeRegistrationDelegation{},
	&StakeVoteDelegation{},
	&StakeVoteRegistrationDelegation{},
	&Tip{},
	&Transaction{},
	&UpdateDrep{},
	&Utxo{},
	&VoteDelegation{},
	&VoteRegistrationDelegation{},
}
