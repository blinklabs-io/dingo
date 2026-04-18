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

package governance

import (
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// govActionPurpose groups action types that share a chain root per
// CIP-1694. NoConfidence and UpdateCommittee both chain off the same
// "committee" root; treating them as distinct action types at the
// storage layer would lose the chain after a NoConfidence is enacted,
// letting a subsequent UpdateCommittee bypass parent validation.
type govActionPurpose int

const (
	purposeNone govActionPurpose = iota
	purposeParameterChange
	purposeHardFork
	purposeCommittee
	purposeConstitution
)

// govActionPurposeOf maps an action type to its chain-root purpose.
// Actions without a chain (Info, TreasuryWithdrawal) return purposeNone.
func govActionPurposeOf(t lcommon.GovActionType) govActionPurpose {
	switch t {
	case lcommon.GovActionTypeParameterChange:
		return purposeParameterChange
	case lcommon.GovActionTypeHardForkInitiation:
		return purposeHardFork
	case lcommon.GovActionTypeNoConfidence,
		lcommon.GovActionTypeUpdateCommittee:
		return purposeCommittee
	case lcommon.GovActionTypeNewConstitution:
		return purposeConstitution
	case lcommon.GovActionTypeTreasuryWithdrawal,
		lcommon.GovActionTypeInfo:
		return purposeNone
	default:
		return purposeNone
	}
}

// purposeActionTypes returns the action type discriminators that share
// the given purpose's chain root.
func purposeActionTypes(p govActionPurpose) []uint8 {
	switch p {
	case purposeParameterChange:
		return []uint8{uint8(lcommon.GovActionTypeParameterChange)}
	case purposeHardFork:
		return []uint8{uint8(lcommon.GovActionTypeHardForkInitiation)}
	case purposeCommittee:
		return []uint8{
			uint8(lcommon.GovActionTypeNoConfidence),
			uint8(lcommon.GovActionTypeUpdateCommittee),
		}
	case purposeConstitution:
		return []uint8{uint8(lcommon.GovActionTypeNewConstitution)}
	case purposeNone:
		return nil
	default:
		return nil
	}
}

// chainedPurposes enumerates the purposes whose roots must be looked
// up at the start of the RATIFY loop. Info and TreasuryWithdrawal are
// omitted because they don't chain.
var chainedPurposes = []govActionPurpose{
	purposeParameterChange,
	purposeHardFork,
	purposeCommittee,
	purposeConstitution,
}
