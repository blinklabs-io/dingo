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
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
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

// eraBoundData captures the per-era bound info computed during the epoch
// walk. All three axes (relTime as picoseconds, slot, epoch) are tracked in
// the CBOR-output domain; hardfork.BuildSummary is used only to compute the
// current era's End under its TransitionInfo rules, then translated back.
type eraBoundData struct {
	epochs []models.Epoch
	start  []any // [picosecondsBigInt, slot, epoch] or nil if era is empty
	end    []any // same shape; nil for empty eras and populated current era
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

	cfg := ls.config.CardanoNodeConfig
	shape, err := eras.BuildShape(cfg)
	if err != nil {
		return nil, err
	}
	if len(shape.Eras) != len(eras.Eras) {
		return nil, fmt.Errorf(
			"era history: shape has %d eras, eras.Eras has %d",
			len(shape.Eras), len(eras.Eras),
		)
	}

	perEra := make([]eraBoundData, len(shape.Eras))
	timespan := big.NewInt(0)
	currentIdx := -1

	for i, entry := range shape.Eras {
		eraDesc := eras.Eras[i]
		epochs, dbErr := ls.db.GetEpochsByEra(entry.EraID, nil)
		if dbErr != nil {
			return nil, dbErr
		}
		perEra[i].epochs = epochs
		if len(epochs) == 0 {
			continue
		}

		firstEp := epochs[0]
		perEra[i].start = []any{
			new(big.Int).Set(timespan),
			firstEp.StartSlot,
			firstEp.EpochId,
		}
		for _, ep := range epochs {
			timespan.Add(
				timespan,
				epochPicoseconds(ep.SlotLength, ep.LengthInSlots),
			)
		}

		if entry.EraID == currentEraId {
			// Defer End to the BuildSummary step below.
			currentIdx = i
			continue
		}

		// Past era: default End = (accumulated timespan, lastEp end, epoch+1).
		lastEp := epochs[len(epochs)-1]
		endSlot, slotErr := checkedSlotAdd(
			lastEp.StartSlot,
			uint64(lastEp.LengthInSlots),
		)
		if slotErr != nil {
			return nil, fmt.Errorf(
				"epoch %d (start=%d, length=%d): %w",
				lastEp.EpochId, lastEp.StartSlot, lastEp.LengthInSlots, slotErr,
			)
		}
		perEra[i].end = []any{
			new(big.Int).Set(timespan),
			endSlot,
			lastEp.EpochId + 1,
		}

		// Transition-epoch detection: a past era whose last epoch's pparams
		// already carry a later-era protocol version is a TransitionKnown
		// window that completed. The confirmed boundary is at lastEp.StartSlot,
		// not lastEp.StartSlot + length; roll back timespan accordingly so the
		// next era's Start.relTime stays contiguous.
		if eraDesc.DecodePParamsFunc == nil {
			continue
		}
		pp, ppErr := ls.db.GetPParams(
			lastEp.EpochId, eraDesc.DecodePParamsFunc, nil,
		)
		if ppErr != nil {
			return nil, fmt.Errorf(
				"getting pparams for epoch %d: %w", lastEp.EpochId, ppErr,
			)
		}
		if pp == nil {
			continue
		}
		ver, verErr := GetProtocolVersion(pp)
		if verErr != nil {
			return nil, fmt.Errorf(
				"extracting protocol version for epoch %d: %w",
				lastEp.EpochId, verErr,
			)
		}
		pparamsEraId, ok := EraForVersion(ver.Major)
		if !ok || pparamsEraId <= entry.EraID {
			continue
		}
		epPc := epochPicoseconds(lastEp.SlotLength, lastEp.LengthInSlots)
		perEra[i].end = []any{
			new(big.Int).Sub(timespan, epPc),
			lastEp.StartSlot,
			lastEp.EpochId,
		}
		// In-place Sub so a later Add on timespan doesn't corrupt the value
		// already stored in perEra[i].end[0].
		timespan.Sub(timespan, epPc)
	}

	// Compute the current era's End via hardfork.BuildSummary, mirroring the
	// Haskell HFC TransitionKnown/Unknown/Impossible semantics.
	if currentIdx >= 0 {
		end, err := ls.currentEraEnd(
			shape, perEra[currentIdx], currentIdx, tipSlot, transitionInfo,
		)
		if err != nil {
			return nil, err
		}
		perEra[currentIdx].end = end
	}

	retData := make([]any, 0, len(shape.Eras))
	for i, entry := range shape.Eras {
		tmpParams := eraParamsCBOR(entry.Params)
		if perEra[i].start == nil || perEra[i].end == nil {
			retData = append(retData, []any{
				[]any{0, 0, 0},
				[]any{0, 0, 0},
				tmpParams,
			})
			continue
		}
		retData = append(retData, []any{
			perEra[i].start, perEra[i].end, tmpParams,
		})
	}
	return cbor.IndefLengthList(retData), nil
}

// currentEraEnd computes the open era's End tuple (picosecondRelTime, slot,
// epoch) from the HFC Summary, with a pre-check that falls back to
// TransitionUnknown when TransitionKnown's KnownEpoch is missing from the DB
// (e.g. a race or rollback). Without the fallback, serving the
// BuildSummary-computed boundary would over-claim certainty about an epoch
// the node hasn't actually seen.
func (ls *LedgerState) currentEraEnd(
	shape hardfork.Shape,
	era eraBoundData,
	idx int,
	tipSlot uint64,
	ti hardfork.TransitionInfo,
) ([]any, error) {
	firstEp := era.epochs[0]
	startRel, ok := era.start[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf(
			"current era start[0] has unexpected type %T", era.start[0],
		)
	}

	// dingo sets TransitionImpossible when evaluateTransitionImpossible has
	// confirmed the current epoch's end is within the safe-zone horizon
	// (safeEndSlot >= epochEndSlot). The intended answer is the current
	// epoch end. BuildSummary's TransitionImpossible branch, however,
	// applies the safe zone from current.Start (= the *first* epoch of
	// the era) and can return an EraEnd behind the tip for a long-running
	// era. Serve the confirmed epoch-end directly instead.
	if ti.State == hardfork.TransitionImpossible {
		endRel := new(big.Int).Set(startRel)
		for _, ep := range era.epochs {
			endRel.Add(endRel, epochPicoseconds(ep.SlotLength, ep.LengthInSlots))
		}
		lastEp := era.epochs[len(era.epochs)-1]
		endSlot, err := checkedSlotAdd(
			lastEp.StartSlot,
			uint64(lastEp.LengthInSlots),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"current era epoch %d (start=%d, length=%d): %w",
				lastEp.EpochId, lastEp.StartSlot, lastEp.LengthInSlots, err,
			)
		}
		return []any{
			endRel,
			endSlot,
			lastEp.EpochId + 1,
		}, nil
	}

	effectiveTI := ti
	if ti.State == hardfork.TransitionKnown {
		found := false
		for _, ep := range era.epochs {
			if ep.EpochId == ti.KnownEpoch &&
				ep.EpochId > firstEp.EpochId {
				found = true
				break
			}
		}
		if !found {
			effectiveTI = hardfork.NewTransitionUnknown()
		}
	}

	curr := hardfork.EraSummary{
		EraID: shape.Eras[idx].EraID,
		Start: hardfork.Bound{
			RelativeTime: picosecondsToDuration(startRel),
			Slot:         firstEp.StartSlot,
			Epoch:        firstEp.EpochId,
		},
		Params: shape.Eras[idx].Params,
	}
	summ, err := hardfork.BuildSummary(shape, nil, curr, tipSlot, effectiveTI)
	if err != nil {
		return nil, err
	}
	endBound := summ.Eras[0].End
	if endBound == nil {
		return nil, errors.New(
			"hardfork: current era End unbounded (SafeZoneSlots==0)",
		)
	}
	return []any{
		durationToPicoseconds(endBound.RelativeTime),
		endBound.Slot,
		endBound.Epoch,
	}, nil
}

// eraParamsCBOR builds the fixed 4-tuple params shape clients expect:
// [epochLength, slotLengthMs, [0,0,[0]], 0]. The third and fourth positions
// are placeholders preserved from the legacy encoding.
func eraParamsCBOR(p hardfork.EraParams) []any {
	return []any{
		uint(p.EpochSize),                     // #nosec G115
		uint(p.SlotLength / time.Millisecond), // #nosec G115
		[]any{0, 0, []any{0}},
		0,
	}
}

// durationToPicoseconds converts a time.Duration (ns) to picoseconds as a
// *big.Int, matching the CBOR relTime encoding used by callers.
func durationToPicoseconds(d time.Duration) *big.Int {
	return new(big.Int).Mul(big.NewInt(int64(d)), big.NewInt(1000))
}

// picosecondsToDuration converts a picosecond *big.Int to a time.Duration
// (ns). Loses no precision for inputs produced by epochPicoseconds (which
// are always multiples of 1_000 picoseconds).
func picosecondsToDuration(p *big.Int) time.Duration {
	ns := new(big.Int).Quo(p, big.NewInt(1000))
	return time.Duration(ns.Int64())
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
