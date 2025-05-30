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

package ledger

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
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
	if shelleyGenesis == nil {
		return nil, errors.New(
			"unable to get shelley era genesis for system start",
		)
	}
	ret := olocalstatequery.SystemStartResult{
		Year:        *big.NewInt(int64(shelleyGenesis.SystemStart.Year())),
		Day:         shelleyGenesis.SystemStart.YearDay(),
		Picoseconds: *big.NewInt(int64(shelleyGenesis.SystemStart.Nanosecond() * 1000)),
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
	case *olocalstatequery.HardForkEraHistoryQuery:
		return ls.queryHardForkEraHistory()
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

func (ls *LedgerState) queryHardForkEraHistory() (any, error) {
	retData := []any{}
	timespan := big.NewInt(0)
	var epochs []database.Epoch
	var era eras.EraDesc
	var err error
	var tmpStart, tmpEnd []any
	var tmpEpoch database.Epoch
	var tmpEra, tmpParams []any
	var epochSlotLength, epochLength uint
	var idx int
	for _, era = range eras.Eras {
		epochSlotLength, epochLength, err = era.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return nil, err
		}
		epochs, err = ls.db.GetEpochsByEra(era.Id, nil)
		if err != nil {
			return nil, err
		}
		tmpStart = []any{0, 0, 0}
		tmpEnd = tmpStart
		tmpParams = []any{
			epochLength,
			epochSlotLength,
			[]any{
				0,
				0,
				[]any{0},
			},
			0,
		}
		for idx, tmpEpoch = range epochs {
			// Update era start
			if idx == 0 {
				tmpStart = []any{
					new(big.Int).Set(timespan),
					tmpEpoch.StartSlot,
					tmpEpoch.EpochId,
				}
			}
			// Add epoch length in picoseconds to timespan
			timespan.Add(
				timespan,
				new(big.Int).SetUint64(
					uint64(
						tmpEpoch.SlotLength*tmpEpoch.LengthInSlots*1_000_000_000,
					),
				),
			)
			// Update era end
			if idx == len(epochs)-1 {
				tmpEnd = []any{
					new(big.Int).Set(timespan),
					tmpEpoch.StartSlot + uint64(tmpEpoch.LengthInSlots),
					tmpEpoch.EpochId + 1,
				}
			}
		}
		tmpEra = []any{
			tmpStart,
			tmpEnd,
			tmpParams,
		}
		retData = append(retData, tmpEra)
	}
	return cbor.IndefLengthList(retData), nil
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
	utxos, err := ls.db.UtxosByAddress(addrs[0], nil)
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
	utxo, err := ls.db.UtxoByRef(
		txIns[0].Id().Bytes(),
		txIns[0].Index(),
		nil,
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
