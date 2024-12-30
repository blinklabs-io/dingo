// Copyright 2024 Blink Labs Software
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

package state

import (
	"fmt"

	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

func (ls *LedgerState) Query(query any) (any, error) {
	switch q := query.(type) {
	case *olocalstatequery.BlockQuery:
		return ls.queryBlock(q)
	// TODO
	/*
		case *olocalstatequery.SystemStartQuery:
		case *olocalstatequery.ChainBlockNoQuery:
		case *olocalstatequery.ChainPointQuery:
	*/
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

func (ls *LedgerState) queryBlock(query *olocalstatequery.BlockQuery) (any, error) {
	switch q := query.Query.(type) {
	case *olocalstatequery.HardForkQuery:
		return ls.queryHardFork(q)
	case *olocalstatequery.ShelleyQuery:
		return ls.queryShelley(q)
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

func (ls *LedgerState) queryHardFork(query *olocalstatequery.HardForkQuery) (any, error) {
	switch q := query.Query.(type) {
	case *olocalstatequery.HardForkCurrentEraQuery:
		return ls.currentEra.Id, nil
	// TODO
	//case *olocalstatequery.HardForkEraHistoryQuery:
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

func (ls *LedgerState) queryShelley(query *olocalstatequery.ShelleyQuery) (any, error) {
	// TODO: make these era-specific
	switch q := query.Query.(type) {
	case *olocalstatequery.ShelleyCurrentProtocolParamsQuery:
		return []any{ls.currentPParams}, nil
	// TODO
	/*
		case *olocalstatequery.ShelleyLedgerTipQuery:
		case *olocalstatequery.ShelleyEpochNoQuery:
		case *olocalstatequery.ShelleyNonMyopicMemberRewardsQuery:
		case *olocalstatequery.ShelleyCurrentProtocolParamsQuery:
		case *olocalstatequery.ShelleyProposedProtocolParamsUpdatesQuery:
		case *olocalstatequery.ShelleyStakeDistributionQuery:
		case *olocalstatequery.ShelleyUtxoByAddressQuery:
		case *olocalstatequery.ShelleyUtxoWholeQuery:
		case *olocalstatequery.ShelleyDebugEpochStateQuery:
		case *olocalstatequery.ShelleyCborQuery:
		case *olocalstatequery.ShelleyFilteredDelegationAndRewardAccountsQuery:
		case *olocalstatequery.ShelleyGenesisConfigQuery:
		case *olocalstatequery.ShelleyDebugNewEpochStateQuery:
		case *olocalstatequery.ShelleyDebugChainDepStateQuery:
		case *olocalstatequery.ShelleyRewardProvenanceQuery:
		case *olocalstatequery.ShelleyUtxoByTxinQuery:
		case *olocalstatequery.ShelleyStakePoolsQuery:
		case *olocalstatequery.ShelleyStakePoolParamsQuery:
		case *olocalstatequery.ShelleyRewardInfoPoolsQuery:
		case *olocalstatequery.ShelleyPoolStateQuery:
		case *olocalstatequery.ShelleyStakeSnapshotsQuery:
		case *olocalstatequery.ShelleyPoolDistrQuery:
	*/
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}
