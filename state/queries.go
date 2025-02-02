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

	"github.com/blinklabs-io/dingo/state/models"

	"github.com/blinklabs-io/gouroboros/ledger"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

func (ls *LedgerState) Query(query any) (any, error) {
	switch q := query.(type) {
	case *olocalstatequery.BlockQuery:
		return ls.queryBlock(q)
	case *olocalstatequery.SystemStartQuery:
		return ls.querySystemStart()
	case *olocalstatequery.ChainBlockNoQuery:
		return ls.queryChainBlockNo()
	case *olocalstatequery.ChainPointQuery:
		return ls.queryChainPoint()
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

func (ls *LedgerState) queryBlock(
	query *olocalstatequery.BlockQuery,
) (any, error) {
	switch q := query.Query.(type) {
	case *olocalstatequery.HardForkQuery:
		return ls.queryHardFork(q)
	case *olocalstatequery.ShelleyQuery:
		return ls.queryShelley(q)
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

func (ls *LedgerState) querySystemStart() (any, error) {
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	ret := olocalstatequery.SystemStartResult{
		Year:        shelleyGenesis.SystemStart.Year(),
		Day:         shelleyGenesis.SystemStart.YearDay(),
		Picoseconds: uint64(shelleyGenesis.SystemStart.Nanosecond() * 1000),
	}
	return ret, nil
}

func (ls *LedgerState) queryChainBlockNo() (any, error) {
	ret := []any{
		1, // TODO: figure out what this value is (#393)
		ls.currentTip.BlockNumber,
	}
	return ret, nil
}

func (ls *LedgerState) queryChainPoint() (any, error) {
	return ls.currentTip.Point, nil
}

func (ls *LedgerState) queryHardFork(
	query *olocalstatequery.HardForkQuery,
) (any, error) {
	switch q := query.Query.(type) {
	case *olocalstatequery.HardForkCurrentEraQuery:
		return ls.currentEra.Id, nil
	// TODO (#321)
	//case *olocalstatequery.HardForkEraHistoryQuery:
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

func (ls *LedgerState) queryShelley(
	query *olocalstatequery.ShelleyQuery,
) (any, error) {
	switch q := query.Query.(type) {
	case *olocalstatequery.ShelleyEpochNoQuery:
		return []any{ls.currentEpoch.EpochId}, nil
	case *olocalstatequery.ShelleyCurrentProtocolParamsQuery:
		return []any{ls.currentPParams}, nil
	case *olocalstatequery.ShelleyGenesisConfigQuery:
		return ls.queryShelleyGenesisConfig()
	case *olocalstatequery.ShelleyUtxoByAddressQuery:
		return ls.queryShelleyUtxoByAddress(q.Addrs)
	case *olocalstatequery.ShelleyUtxoByTxinQuery:
		return ls.queryShelleyUtxoByTxIn(q.TxIns)
	// TODO (#394)
	/*
		case *olocalstatequery.ShelleyLedgerTipQuery:
		case *olocalstatequery.ShelleyNonMyopicMemberRewardsQuery:
		case *olocalstatequery.ShelleyProposedProtocolParamsUpdatesQuery:
		case *olocalstatequery.ShelleyStakeDistributionQuery:
		case *olocalstatequery.ShelleyUtxoWholeQuery:
		case *olocalstatequery.ShelleyDebugEpochStateQuery:
		case *olocalstatequery.ShelleyCborQuery:
		case *olocalstatequery.ShelleyFilteredDelegationAndRewardAccountsQuery:
		case *olocalstatequery.ShelleyDebugNewEpochStateQuery:
		case *olocalstatequery.ShelleyDebugChainDepStateQuery:
		case *olocalstatequery.ShelleyRewardProvenanceQuery:
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

func (ls *LedgerState) queryShelleyGenesisConfig() (any, error) {
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	return []any{shelleyGenesis}, nil
}

func (ls *LedgerState) queryShelleyUtxoByAddress(
	addrs []ledger.Address,
) (any, error) {
	ret := make(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	// TODO: support multiple addresses (#391)
	utxos, err := models.UtxosByAddress(ls.db, addrs[0])
	if err != nil {
		return nil, err
	}
	for _, utxo := range utxos {
		txOut, err := utxo.Decode()
		if err != nil {
			return nil, err
		}
		utxoId := olocalstatequery.UtxoId{
			Hash: ledger.NewBlake2b256(utxo.TxId),
			Idx:  int(utxo.OutputIdx),
		}
		ret[utxoId] = txOut
	}
	return []any{ret}, nil
}

func (ls *LedgerState) queryShelleyUtxoByTxIn(
	txIns []ledger.ShelleyTransactionInput,
) (any, error) {
	ret := make(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	// TODO: support multiple TxIns (#392)
	utxo, err := models.UtxoByRef(
		ls.db,
		txIns[0].Id().Bytes(),
		txIns[0].Index(),
	)
	if err != nil {
		return nil, err
	}
	txOut, err := utxo.Decode()
	if err != nil {
		return nil, err
	}
	utxoId := olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(utxo.TxId),
		Idx:  int(utxo.OutputIdx),
	}
	ret[utxoId] = txOut
	return []any{ret}, nil
}
