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

package nodesettings

import "strconv"

// Family names one group of ledger-state records the live UTxO set holds and
// a v0.94.2 metadata store kept in SQL tables.
type Family uint8

// The ledger-state families.
const (
	FamilyNonces Family = iota
	FamilyEpochs
	FamilySummaries
	FamilyEraSwitch
	FamilyNetworkDonations
	FamilyPParamsUpdates
	FamilyCommitteeConstitution
	FamilyOpCerts
	FamilyPoolsHistory
	FamilyAccounts
	FamilyDReps
	FamilyMIRGenesis
	FamilyGovernance
	FamilyRewardInputs
	FamilyLiveStake
	FamilyRewardBlocksFees
	FamilyRewardOutputs
	familyEnd
)

var familyNames = [familyEnd]string{
	FamilyNonces:                "nonces",
	FamilyEpochs:                "epochs",
	FamilySummaries:             "summaries",
	FamilyEraSwitch:             "era-switch",
	FamilyNetworkDonations:      "network-donations",
	FamilyPParamsUpdates:        "pparams-updates",
	FamilyCommitteeConstitution: "committee-constitution",
	FamilyOpCerts:               "op-certs",
	FamilyPoolsHistory:          "pools-history",
	FamilyAccounts:              "accounts",
	FamilyDReps:                 "dreps",
	FamilyMIRGenesis:            "mir-genesis",
	FamilyGovernance:            "governance",
	FamilyRewardInputs:          "reward-inputs",
	FamilyLiveStake:             "live-stake",
	FamilyRewardBlocksFees:      "reward-blocks-fees",
	FamilyRewardOutputs:         "reward-outputs",
}

// String names the family for logs and errors.
func (f Family) String() string {
	if f < familyEnd {
		return familyNames[f]
	}
	return "family-" + strconv.Itoa(int(f))
}

// Families returns every family this binary knows, in bit order.
func Families() []Family {
	out := make([]Family, 0, familyEnd)
	for f := range familyEnd {
		out = append(out, f)
	}
	return out
}
