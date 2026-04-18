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
	"math"
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
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
	// Picoseconds is the total elapsed time since midnight (start of the
	// day) in picoseconds.  time.Time.Nanosecond() returns only the
	// sub-second component, so we must include hours, minutes, and seconds.
	utc := shelleyGenesis.SystemStart.UTC()
	h, m, sec := utc.Clock()
	dayPicoseconds := (int64(h)*3600+
		int64(m)*60+
		int64(sec))*1_000_000_000_000 +
		int64(utc.Nanosecond())*1000
	ret := olocalstatequery.SystemStartResult{
		Year:        *big.NewInt(int64(utc.Year())),
		Day:         utc.YearDay(),
		Picoseconds: *big.NewInt(dayPicoseconds),
	}
	return ret, nil
}

func (ls *LedgerState) queryChainBlockNo() (any, error) {
	ls.RLock()
	blockNumber := ls.currentTip.BlockNumber
	ls.RUnlock()
	ret := []any{
		1, // TODO: figure out what this value is (#393)
		blockNumber,
	}
	return ret, nil
}

func (ls *LedgerState) queryChainPoint() (any, error) {
	ls.RLock()
	point := ls.currentTip.Point
	ls.RUnlock()
	return point, nil
}

func (ls *LedgerState) queryHardFork(
	query *olocalstatequery.HardForkQuery,
) (any, error) {
	switch q := query.Query.(type) {
	case *olocalstatequery.HardForkCurrentEraQuery:
		ls.RLock()
		eraId := ls.currentEra.Id
		ls.RUnlock()
		return eraId, nil
	case *olocalstatequery.HardForkEraHistoryQuery:
		return ls.queryHardForkEraHistory()
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

// epochPicoseconds computes the duration of an epoch
// in picoseconds: slotLength * lengthInSlots * 1e9.
// It uses big.Int arithmetic to prevent overflow when
// the uint product would exceed math.MaxUint64.
func epochPicoseconds(
	slotLength, lengthInSlots uint,
) *big.Int {
	result := new(big.Int).SetUint64(uint64(slotLength))
	result.Mul(
		result,
		new(big.Int).SetUint64(uint64(lengthInSlots)),
	)
	result.Mul(result, big.NewInt(1_000_000_000))
	return result
}

// checkedSlotAdd adds startSlot + length with overflow
// detection. Returns an error if the result would
// exceed math.MaxUint64.
func checkedSlotAdd(
	startSlot, length uint64,
) (uint64, error) {
	if startSlot > math.MaxUint64-length {
		return 0, fmt.Errorf(
			"era history overflow: start slot %d + length %d",
			startSlot,
			length,
		)
	}
	return startSlot + length, nil
}

func (ls *LedgerState) queryHardForkEraHistory() (any, error) {
	// Snapshot the tip, current era, and transition info under the read lock
	// so we can use them without holding the lock during the (potentially
	// slow) DB queries below.
	ls.RLock()
	tipSlot := ls.currentTip.Point.Slot
	currentEraId := ls.currentEra.Id
	transitionInfo := ls.transitionInfo
	ls.RUnlock()

	retData := []any{}
	timespan := big.NewInt(0)
	var epochs []models.Epoch
	var era eras.EraDesc
	var err error
	var tmpStart, tmpEnd []any
	var tmpEpoch models.Epoch
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
				epochPicoseconds(
					tmpEpoch.SlotLength,
					tmpEpoch.LengthInSlots,
				),
			)
			// Update era end
			if idx == len(epochs)-1 {
				endSlot, slotErr := checkedSlotAdd(
					tmpEpoch.StartSlot,
					uint64(tmpEpoch.LengthInSlots),
				)
				if slotErr != nil {
					return nil, fmt.Errorf(
						"epoch %d (start=%d, length=%d): %w",
						tmpEpoch.EpochId,
						tmpEpoch.StartSlot,
						tmpEpoch.LengthInSlots,
						slotErr,
					)
				}
				tmpEnd = []any{
					new(big.Int).Set(timespan),
					endSlot,
					tmpEpoch.EpochId + 1,
				}
			}
		}
		// For the current (open) era, cap EraEnd at a safe forecast horizon.
		//
		// When TransitionKnown is set the epoch-rollover logic has confirmed
		// an upcoming hard fork: use the transition epoch's StartSlot as the
		// exact era boundary (mirrors Haskell's TransitionKnown branch).
		//
		// Otherwise, cap at ledgerTip + safeZone (StandardSafeZone): clients
		// must not attempt slot↔time conversions beyond this bound because a
		// hard fork could invalidate them.
		if era.Id == currentEraId && len(epochs) > 0 {
			foundTransition := false
			if transitionInfo.State == TransitionKnown {
				// Find the transition epoch in the committed epoch list.
				// It was created with the old era's EraId, so it appears
				// in epochs.  Its StartSlot is the exact era end boundary.
				for _, ep := range epochs {
					if ep.EpochId == transitionInfo.KnownEpoch {
						// Compute the accumulated relative time up to ep.StartSlot.
						// timespan currently reflects the end of tmpEpoch (last epoch).
						// Walk back to the start of ep, then forward to ep.StartSlot.
						timespanAtEpochStart := new(big.Int).Sub(
							timespan,
							epochPicoseconds(ep.SlotLength, ep.LengthInSlots),
						)
						// The era ends at ep.StartSlot; no partial-slot offset needed.
						tmpEnd = []any{
							timespanAtEpochStart,
							ep.StartSlot,
							ep.EpochId,
						}
						foundTransition = true
						break
					}
				}
			}
			if transitionInfo.State != TransitionKnown || !foundTransition {
				// No confirmed transition: cap at tipSlot + safeZone.
				safeZone := ls.calculateStabilityWindowForEra(era.Id)
				safeEndSlot, addErr := checkedSlotAdd(tipSlot, safeZone)
				epochEndSlot, slotErr := checkedSlotAdd(
					tmpEpoch.StartSlot,
					uint64(tmpEpoch.LengthInSlots),
				)
				if addErr == nil && slotErr == nil &&
					safeEndSlot >= tmpEpoch.StartSlot &&
					safeEndSlot < epochEndSlot {
					// Work backward from the accumulated timespan (which now
					// points to epochEndSlot) to find the relative time at
					// the start of the last epoch, then advance it by the
					// partial number of slots to safeEndSlot.
					timespanAtEpochStart := new(big.Int).Sub(
						timespan,
						epochPicoseconds(tmpEpoch.SlotLength, tmpEpoch.LengthInSlots),
					)
					slotsIntoEpoch := safeEndSlot - tmpEpoch.StartSlot
					safeRelTime := new(big.Int).Add(
						timespanAtEpochStart,
						new(big.Int).Mul(
							new(big.Int).SetUint64(slotsIntoEpoch),
							new(big.Int).Mul(
								new(big.Int).SetUint64(uint64(tmpEpoch.SlotLength)),
								big.NewInt(1_000_000_000),
							),
						),
					)
					// safeEndSlot is within tmpEpoch, so the epoch number is
					// tmpEpoch.EpochId (not +1).
					tmpEnd = []any{
						safeRelTime,
						safeEndSlot,
						tmpEpoch.EpochId,
					}
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
		ls.RLock()
		epochId := ls.currentEpoch.EpochId
		ls.RUnlock()
		return []any{epochId}, nil
	case *olocalstatequery.ShelleyCurrentProtocolParamsQuery:
		ls.RLock()
		pparams := ls.currentPParams
		ls.RUnlock()
		return []any{pparams}, nil
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
	if len(addrs) == 0 {
		return []any{ret}, nil
	}
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
	if len(txIns) == 0 {
		return []any{ret}, nil
	}
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
