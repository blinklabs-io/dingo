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
	"fmt"

	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// FamilyLoaded reports whether set holds every marker of family's records.
func FamilyLoaded(
	f nodesettings.Family,
	set UtxoSetWriter,
) (bool, error) {
	//nolint:exhaustive // the default case refuses an unknown family
	switch f {
	case nodesettings.FamilyNonces:
		r, ok := set.(BlockNonceReader)
		if !ok {
			return false, nil
		}
		return r.NoncesLoaded()
	case nodesettings.FamilyNetworkDonations:
		r, ok := set.(NetworkStateReader)
		if !ok {
			return false, nil
		}
		return r.NetworkStateLoaded()
	case nodesettings.FamilyEpochs, nodesettings.FamilyEraSwitch:
		r, ok := set.(EpochStateReader)
		if !ok {
			return false, nil
		}
		return r.EpochStateLoaded()
	case nodesettings.FamilySummaries:
		r, ok := set.(EpochSummaryReader)
		if !ok {
			return false, nil
		}
		return r.EpochSummariesLoaded()
	case nodesettings.FamilyLiveStake:
		return liveStakeLoaded(set)
	case nodesettings.FamilyCommitteeConstitution:
		r, ok := set.(CommitteeReader)
		if !ok {
			return false, nil
		}
		return r.CommitteeLoaded()
	case nodesettings.FamilyOpCerts:
		r, ok := set.(OpCertReader)
		if !ok {
			return false, nil
		}
		return r.OpCertsLoaded()
	case nodesettings.FamilyGovernance:
		r, ok := set.(GovernanceReader)
		m, ok2 := set.(StateMarkReader)
		if !ok || !ok2 {
			return false, nil
		}
		loaded, err := r.GovernanceLoaded()
		if err != nil || !loaded {
			return false, err
		}
		return m.RatifyPendingLoaded()
	case nodesettings.FamilyPParamsUpdates:
		pr, ok := set.(PParamsReader)
		ur, ok2 := set.(PParamUpdateReader)
		mr, ok3 := set.(SyncMarkerReader)
		if !ok || !ok2 || !ok3 {
			return false, nil
		}
		for _, loaded := range []func() (bool, error){
			pr.PParamsLoaded, ur.PParamUpdatesLoaded, mr.SyncMarkersLoaded,
		} {
			ok, err := loaded()
			if err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	case nodesettings.FamilyMIRGenesis:
		r, ok := set.(CertHistoryReader)
		if !ok {
			return false, nil
		}
		return r.GenesisMIRLoaded()
	case nodesettings.FamilyDReps:
		r, ok := set.(DRepReader)
		if !ok {
			return false, nil
		}
		return r.DrepStateLoaded()
	case nodesettings.FamilyAccounts:
		return accountsLoaded(set)
	case nodesettings.FamilyPoolsHistory:
		state, ok := set.(PoolReader)
		hist, ok2 := set.(PoolHistoryReader)
		if !ok || !ok2 {
			return false, nil
		}
		if _, ok := set.(PoolHistoryLister); !ok {
			return false, nil
		}
		stateLoaded, err := state.PoolsLoaded()
		if err != nil || !stateLoaded {
			return false, err
		}
		return hist.PoolHistoryLoaded()
	case nodesettings.FamilyRewardInputs:
		r, ok := set.(RewardInputsReader)
		if !ok {
			return false, nil
		}
		return r.RewardInputsLoaded()
	case nodesettings.FamilyRewardBlocksFees:
		r, ok := set.(RewardBlocksReader)
		if !ok {
			return false, nil
		}
		loaded, err := r.RewardBlocksLoaded()
		if err != nil || !loaded {
			return false, err
		}
		imported, ok := set.(ImportedBlocksReader)
		if !ok {
			return false, nil
		}
		return imported.ImportedBlocksLoaded()
	case nodesettings.FamilyRewardOutputs:
		derived, ok := set.(RewardDerivedReader)
		if !ok {
			return false, nil
		}
		loaded, err := derived.RewardDerivedLoaded()
		if err != nil || !loaded {
			return false, err
		}
		pending, ok := set.(RewardOutputReader)
		if !ok {
			return false, nil
		}
		return pending.RewardOutputsLoaded()
	default:
		return false, fmt.Errorf("no load markers for family %s", f)
	}
}

// accountsLoaded reports whether the set holds every marker of the accounts
// family: the accounts, the import baselines, the witness slots and the
// registration history.
func accountsLoaded(set UtxoSetWriter) (bool, error) {
	a, ok := set.(AccountReader)
	if !ok {
		return false, nil
	}
	b, ok := set.(ImportBaselineReader)
	if !ok {
		return false, nil
	}
	w, ok := set.(WitnessReader)
	if !ok {
		return false, nil
	}
	h, ok := set.(RegistrationHistoryReader)
	if !ok {
		return false, nil
	}
	m, ok := set.(StateMarkReader)
	if !ok {
		return false, nil
	}
	for _, loaded := range []func() (bool, error){
		a.AccountsLoaded, b.ImportBaselinesLoaded, w.WitnessSlotsLoaded,
		h.RegistrationHistoryLoaded, m.InactivityLoaded,
	} {
		ok, err := loaded()
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// liveStakeLoaded reports whether set holds the accounts and the reward rows
// that its live stake is derived from.
func liveStakeLoaded(set UtxoSetWriter) (bool, error) {
	accounts, ok := set.(AccountReader)
	if !ok {
		return false, nil
	}
	loaded, err := accounts.AccountsLoaded()
	if err != nil || !loaded {
		return false, err
	}
	rewards, ok := set.(RewardOutputReader)
	if !ok {
		return false, nil
	}
	return rewards.RewardOutputsLoaded()
}
