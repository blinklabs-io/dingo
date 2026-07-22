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
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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
	tip := ls.loadTipSnapshot().currentTip
	// WithOrigin BlockNo: [0] at genesis, [1, blockNo] once a block exists.
	if len(tip.Point.Hash) == 0 {
		return []any{0}, nil
	}
	return []any{1, tip.BlockNumber}, nil
}

func (ls *LedgerState) queryChainPoint() (any, error) {
	return cloneTip(ls.loadTipSnapshot().currentTip).Point, nil
}

func (ls *LedgerState) queryHardFork(
	query *olocalstatequery.HardForkQuery,
) (any, error) {
	switch q := query.Query.(type) {
	case *olocalstatequery.HardForkCurrentEraQuery:
		return ls.loadConsensusSnapshot().currentEra.Id, nil
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
	// Read the tip, current era, and transition info from the lock-free
	// snapshots so this (potentially slow) DB-querying path never contends
	// with the ledger write lock.
	consensusState, tipState := ls.loadStateSnapshots()
	tipSlot := tipState.currentTip.Point.Slot
	currentEraId := consensusState.currentEra.Id
	transitionInfo := consensusState.transitionInfo

	shape := ls.eraShape()
	if len(shape.Eras) == 0 {
		return nil, errors.New("era history: shape unavailable")
	}
	activeEras := ls.eraList()
	if len(shape.Eras) != len(activeEras) {
		return nil, fmt.Errorf(
			"era history: shape has %d eras, active era table has %d",
			len(shape.Eras), len(activeEras),
		)
	}

	perEra := make([]eraBoundData, len(shape.Eras))
	timespan := big.NewInt(0)
	currentIdx := -1

	for i, entry := range shape.Eras {
		eraDesc := activeEras[i]
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
			lastEp.EpochId, eraDesc.Id, eraDesc.DecodePParamsFunc, nil,
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
		pparamsEraId, ok := ls.eraForVersion(ver.Major)
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
	return ls.queryShelleyLeaf(query.Query)
}

// queryShelleyLeaf dispatches a decoded Shelley block-query leaf and returns
// its result in the single-element MsgResult wire form (`[]any{value}`). It
// is shared by queryShelley and by the GetCBOR combinator handler, which
// re-runs the wrapped inner query through it.
func (ls *LedgerState) queryShelleyLeaf(query any) (any, error) {
	switch q := query.(type) {
	case *olocalstatequery.ShelleyCborQuery:
		return ls.queryShelleyCbor(q)
	case *olocalstatequery.ShelleyEpochNoQuery:
		return []any{ls.loadConsensusSnapshot().currentEpoch.EpochId}, nil
	case *olocalstatequery.ShelleyCurrentProtocolParamsQuery:
		return []any{ls.loadConsensusSnapshot().currentPParams}, nil
	case *olocalstatequery.ShelleyGenesisConfigQuery:
		return ls.queryShelleyGenesisConfig()
	case *olocalstatequery.ShelleyUtxoByAddressQuery:
		return ls.queryShelleyUtxoByAddress(q.Addrs)
	case *olocalstatequery.ShelleyUtxoByTxinQuery:
		return ls.queryShelleyUtxoByTxIn(q.TxIns)
	case *olocalstatequery.ShelleyFilteredDelegationAndRewardAccountsQuery:
		return ls.queryShelleyFilteredDelegationAndRewardAccounts(
			q.Creds.Items(),
		)
	case *olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery:
		return ls.queryLedgerPeerSnapshot(q.PeerKind)
	case *olocalstatequery.ShelleyStakePoolsQuery:
		return ls.queryShelleyStakePools()
	case *olocalstatequery.ShelleyDRepStateQuery:
		return ls.queryShelleyDRepState(q.Credentials.Items())
	case *olocalstatequery.ShelleyAccountStateQuery:
		return ls.queryShelleyAccountState()
	case *olocalstatequery.ShelleyStakeSnapshotsQuery:
		return ls.queryShelleyStakeSnapshots(q)
	// TODO (#394)
	/*
		case *olocalstatequery.ShelleyLedgerTipQuery:
		case *olocalstatequery.ShelleyNonMyopicMemberRewardsQuery:
		case *olocalstatequery.ShelleyProposedProtocolParamsUpdatesQuery:
		case *olocalstatequery.ShelleyStakeDistributionQuery:
		case *olocalstatequery.ShelleyUtxoWholeQuery:
		case *olocalstatequery.ShelleyDebugEpochStateQuery:
		case *olocalstatequery.ShelleyDebugNewEpochStateQuery:
		case *olocalstatequery.ShelleyDebugChainDepStateQuery:
		case *olocalstatequery.ShelleyRewardProvenanceQuery:
		case *olocalstatequery.ShelleyStakePoolParamsQuery:
		case *olocalstatequery.ShelleyRewardInfoPoolsQuery:
		case *olocalstatequery.ShelleyPoolStateQuery:
		case *olocalstatequery.ShelleyPoolDistrQuery:
	*/
	default:
		return nil, fmt.Errorf("unsupported query type: %T", q)
	}
}

// queryShelleyCbor answers the GetCBOR query combinator. It runs the wrapped
// inner query and returns its result re-encoded as raw serialised CBOR
// (CBOR-in-CBOR, tag 24), matching cardano-node. cardano-cli wraps several
// queries this way (e.g. `query stake-snapshot`), so the whole class of
// GetCBOR-wrapped queries flows through here. See issue #2917.
func (ls *LedgerState) queryShelleyCbor(
	q *olocalstatequery.ShelleyCborQuery,
) (any, error) {
	inner, err := ls.queryShelleyLeaf(q.Query)
	if err != nil {
		return nil, err
	}
	// Inner handlers return the result in the single-element MsgResult wire
	// form ([]any{value}); GetCBOR serialises just the wrapped value.
	values, ok := inner.([]any)
	if !ok || len(values) != 1 {
		return nil, fmt.Errorf(
			"unexpected inner query result shape for GetCBOR: %T",
			inner,
		)
	}
	encoded, err := cbor.Encode(values[0])
	if err != nil {
		return nil, err
	}
	return []any{cbor.Tag{Number: cbor.CborTagCbor, Content: encoded}}, nil
}

// queryShelleyStakeSnapshots answers GetStakeSnapshots. It returns the
// mark/set/go stake for each requested pool (or every pool with a snapshot
// when the query carries no pool filter) plus the mark/set/go totals.
//
// In Ouroboros the current epoch's boundary snapshot is "mark"; "set" is the
// previous epoch's snapshot and "go" the one before that (the active stake
// used for the current epoch's rewards/leader election). dingo persists each
// boundary snapshot under type "mark" keyed by its epoch, so set/go for the
// current epoch are read from the mark snapshots at epoch-1 and epoch-2.
func (ls *LedgerState) queryShelleyStakeSnapshots(
	q *olocalstatequery.ShelleyStakeSnapshotsQuery,
) (any, error) {
	consensus := ls.loadConsensusSnapshot()
	epoch := consensus.currentEpoch.EpochId
	setEpoch, hasSet := priorEpoch(epoch, 1)
	goEpoch, hasGo := priorEpoch(epoch, 2)

	// From protocol version 11, GetStakeSnapshots omits any pool whose
	// mark/set/go stake are all zero, regardless of the reason (unregistered,
	// no delegations, or zero stake) and regardless of whether the pool was
	// explicitly requested (cardano-ledger issue 5581). Below PV11 an
	// explicitly requested pool is always returned, even with zero stake.
	omitZeroPools := false
	if pv, err := GetProtocolVersion(consensus.currentPParams); err == nil {
		omitZeroPools = pv.Major >= 11
	}

	// Read the mark/set/go snapshots under a single read transaction so all
	// three epochs come from one consistent view even if an epoch boundary
	// fires mid-query.
	txn := ls.db.Transaction(false)
	defer txn.Release()
	metaTxn := txn.Metadata()

	pools, all := q.PoolFilter()

	var poolSnapshots map[ledger.Blake2b224]*olocalstatequery.PoolStakeSnapshot
	if all {
		// Bulk-load each epoch's mark snapshot once (one read per epoch, not
		// one per pool) and report every pool that appears in any of the
		// three: a pool that has retired keeps historical set/go stake that
		// must still be returned, so the pool set is the union of all three
		// snapshots rather than the mark snapshot alone.
		mark, err := ls.markStakeByPool(epoch, true, metaTxn)
		if err != nil {
			return nil, err
		}
		set, err := ls.markStakeByPool(setEpoch, hasSet, metaTxn)
		if err != nil {
			return nil, err
		}
		snapshotGo, err := ls.markStakeByPool(goEpoch, hasGo, metaTxn)
		if err != nil {
			return nil, err
		}
		poolSnapshots = make(
			map[ledger.Blake2b224]*olocalstatequery.PoolStakeSnapshot,
		)
		for _, byPool := range []map[string]uint64{mark, set, snapshotGo} {
			for hash := range byPool {
				key := ledger.NewBlake2b224([]byte(hash))
				if _, ok := poolSnapshots[key]; ok {
					continue
				}
				markStake, setStake, goStake := mark[hash], set[hash], snapshotGo[hash]
				if omitZeroPools && markStake == 0 && setStake == 0 && goStake == 0 {
					continue
				}
				poolSnapshots[key] = &olocalstatequery.PoolStakeSnapshot{
					StakeMark: markStake,
					StakeSet:  setStake,
					StakeGo:   goStake,
				}
			}
		}
	} else {
		// A pool filter is bounded by the caller, so read only the requested
		// pools' snapshots rather than every pool's.
		poolSnapshots = make(
			map[ledger.Blake2b224]*olocalstatequery.PoolStakeSnapshot,
			len(pools),
		)
		for _, pool := range pools {
			hash := make([]byte, len(pool))
			copy(hash, pool[:])
			mark, err := ls.poolSnapshotStake(epoch, true, hash, metaTxn)
			if err != nil {
				return nil, err
			}
			set, err := ls.poolSnapshotStake(setEpoch, hasSet, hash, metaTxn)
			if err != nil {
				return nil, err
			}
			snapshotGo, err := ls.poolSnapshotStake(goEpoch, hasGo, hash, metaTxn)
			if err != nil {
				return nil, err
			}
			if omitZeroPools && mark == 0 && set == 0 && snapshotGo == 0 {
				continue
			}
			poolSnapshots[ledger.NewBlake2b224(hash)] = &olocalstatequery.PoolStakeSnapshot{
				StakeMark: mark,
				StakeSet:  set,
				StakeGo:   snapshotGo,
			}
		}
	}

	markTotal, err := ls.totalActiveStake(epoch, true, metaTxn)
	if err != nil {
		return nil, err
	}
	setTotal, err := ls.totalActiveStake(setEpoch, hasSet, metaTxn)
	if err != nil {
		return nil, err
	}
	goTotal, err := ls.totalActiveStake(goEpoch, hasGo, metaTxn)
	if err != nil {
		return nil, err
	}

	result := olocalstatequery.StakeSnapshotsResult{
		PoolSnapshots:  poolSnapshots,
		TotalStakeMark: markTotal,
		TotalStakeSet:  setTotal,
		TotalStakeGo:   goTotal,
	}
	return []any{result}, nil
}

// snapshotTypeMark is the physical snapshot type dingo persists at each
// epoch boundary; set/go are derived from earlier epochs' mark snapshots.
const snapshotTypeMark = "mark"

// priorEpoch returns epoch-n and true when that prior epoch exists, or
// (0, false) when it would underflow (epoch < n) — i.e. the chain does not
// yet have n epochs of snapshot history behind the current one.
func priorEpoch(epoch, n uint64) (uint64, bool) {
	if epoch < n {
		return 0, false
	}
	return epoch - n, true
}

// markStakeByPool bulk-loads the mark snapshot for the given epoch and indexes
// each pool's stake by pool key hash (string). It returns nil when the epoch
// does not exist (exists=false).
func (ls *LedgerState) markStakeByPool(
	epoch uint64,
	exists bool,
	txn types.Txn,
) (map[string]uint64, error) {
	if !exists {
		return nil, nil
	}
	snapshots, err := ls.db.Metadata().GetPoolStakeSnapshotsByEpoch(
		epoch,
		snapshotTypeMark,
		txn,
	)
	if err != nil {
		return nil, err
	}
	byPool := make(map[string]uint64, len(snapshots))
	for _, snapshot := range snapshots {
		byPool[string(snapshot.PoolKeyHash)] = uint64(snapshot.TotalStake)
	}
	return byPool, nil
}

// poolSnapshotStake returns a single pool's mark-snapshot stake at the given
// epoch, or 0 when the epoch does not exist (exists=false) or has no snapshot
// row for the pool. Used for the bounded pool-filter path; the all-pools path
// uses markStakeByPool to avoid a per-pool query.
func (ls *LedgerState) poolSnapshotStake(
	epoch uint64,
	exists bool,
	poolKeyHash []byte,
	txn types.Txn,
) (uint64, error) {
	if !exists {
		return 0, nil
	}
	snapshot, err := ls.db.Metadata().GetPoolStakeSnapshot(
		epoch,
		snapshotTypeMark,
		poolKeyHash,
		txn,
	)
	if err != nil {
		return 0, err
	}
	if snapshot == nil {
		return 0, nil
	}
	return uint64(snapshot.TotalStake), nil
}

// totalActiveStake returns the total mark-snapshot stake at the given epoch,
// clamped to a minimum of 1.
//
// The three StakeSnapshots totals are decoded by cardano clients as NonZero
// values: cardano-node emits 1 for an empty snapshot total (verified against
// cardano-node 11.0.1 on a fresh devnet, where every pool's set/go stake is 0
// yet the set/go totals are reported as 1). Emitting a literal 0 makes
// cardano-cli fail with "Encountered zero while trying to construct a NonZero
// value". Per-pool stakes are plain Coin and are left un-clamped. See issue
// #2917.
func (ls *LedgerState) totalActiveStake(
	epoch uint64,
	exists bool,
	txn types.Txn,
) (uint64, error) {
	if !exists {
		return 1, nil
	}
	total, err := ls.db.Metadata().GetTotalActiveStake(epoch, snapshotTypeMark, txn)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		return 1, nil
	}
	return total, nil
}

func (ls *LedgerState) queryShelleyGenesisConfig() (any, error) {
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	return []any{shelleyGenesis}, nil
}

// queryShelleyStakePools answers GetStakePools: the set of currently
// registered (active, non-retired) stake pool IDs. cardano-cli issues this
// query unconditionally while balancing a transaction (e.g. `transaction
// build`), so leaving it unhandled tears down the local-state-query
// connection.
func (ls *LedgerState) queryShelleyStakePools() (any, error) {
	keyHashes, err := ls.db.GetActivePoolKeyHashes(nil)
	if err != nil {
		return nil, err
	}
	return stakePoolsResult(keyHashes), nil
}

// queryShelleyDRepState answers GetDRepState: the registration state of the
// requested DReps, or of all DReps when the credential set is empty (matching
// the Haskell ledger's "empty set means all" semantics). cardano-cli issues
// this while balancing a transaction, so leaving it unhandled tears down the
// connection. The result is a bare CBOR map of stake credential -> {expiry
// epoch, optional anchor, deposit}.
func (ls *LedgerState) queryShelleyDRepState(
	creds []lcommon.Credential,
) (any, error) {
	result := make(olocalstatequery.DRepStateResult)
	var dreps []*models.Drep
	if len(creds) == 0 {
		all, err := ls.db.GetActiveDreps(nil)
		if err != nil {
			return nil, err
		}
		dreps = all
	} else {
		for _, cred := range creds {
			credentialTag, err := models.CredentialTagFromUint(cred.CredType)
			if err != nil {
				return nil, err
			}
			drep, err := ls.db.GetDrepByCredential(
				credentialTag,
				cred.Credential[:],
				false,
				nil,
			)
			if err != nil {
				if errors.Is(err, models.ErrDrepNotFound) {
					continue
				}
				return nil, err
			}
			dreps = append(dreps, drep)
		}
	}
	// Every DRep locks the dRepDeposit protocol parameter at registration.
	deposit := ls.drepDeposit()
	for _, drep := range dreps {
		if drep == nil {
			continue
		}
		delegators, err := ls.drepDelegators(drep)
		if err != nil {
			return nil, err
		}
		key := olocalstatequery.StakeCredential{
			Tag:   uint64(drep.CredentialTag),
			Bytes: ledger.NewBlake2b224(drep.Credential),
		}
		result[key] = olocalstatequery.DRepStateEntry{
			Expiry:     drep.ExpiryEpoch,
			Anchor:     drepAnchor(drep),
			Deposit:    deposit,
			Delegators: delegators,
		}
	}
	// The result map is wrapped in the single-element result array cardano-cli
	// expects (verified against cardano-node: an empty result is the CBOR
	// `81 a0`, i.e. [ {} ]).
	return []any{result}, nil
}

// queryShelleyAccountState answers GetAccountState: the chain's treasury and
// reserves pots. cardano-cli issues this while balancing a transaction, so
// leaving it unhandled tears down the connection. The account state is wrapped
// in the single-element result array, so the wire shape is
// [ [treasury, reserves] ] (both are signed; a misconfigured network can drive
// reserves negative).
func (ls *LedgerState) queryShelleyAccountState() (any, error) {
	state, err := ls.db.Metadata().GetNetworkState(nil)
	if err != nil {
		return nil, err
	}
	var treasury, reserves int64
	if state != nil {
		treasury = int64(state.Treasury) //nolint:gosec // pot values fit in int64
		reserves = int64(state.Reserves) //nolint:gosec // pot values fit in int64
	}
	return []any{
		olocalstatequery.AccountState{
			Treasury: treasury,
			Reserves: reserves,
		},
	}, nil
}

// drepDeposit returns the current dRepDeposit protocol parameter, which is the
// deposit every DRep locks at registration. Returns 0 outside Conway.
func (ls *LedgerState) drepDeposit() uint64 {
	pparams := ls.loadConsensusSnapshot().currentPParams
	if cpp, ok := pparams.(*conway.ConwayProtocolParameters); ok &&
		cpp != nil {
		return cpp.DRepDeposit
	}
	return 0
}

// drepDelegators returns the stake credentials currently delegating their
// voting power to the given DRep, as the wire type, in canonical (tag, hash)
// order so the resulting CBOR set (tag 258) is canonical — cardano clients
// reject an unsorted set with "Canonicity violation".
func (ls *LedgerState) drepDelegators(
	drep *models.Drep,
) ([]olocalstatequery.StakeCredential, error) {
	refs, err := ls.db.GetDRepDelegators(drep.CredentialTag, drep.Credential, nil)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}
	out := make([]olocalstatequery.StakeCredential, len(refs))
	for i, ref := range refs {
		out[i] = olocalstatequery.StakeCredential{
			Tag:   uint64(ref.Tag),
			Bytes: ledger.NewBlake2b224(ref.Key),
		}
	}
	return out, nil
}

// drepAnchor maps a stored DRep's anchor metadata to the wire type, or nil
// when the DRep has no anchor.
func drepAnchor(drep *models.Drep) *lcommon.GovAnchor {
	if drep.AnchorURL == "" && len(drep.AnchorHash) == 0 {
		return nil
	}
	anchor := &lcommon.GovAnchor{Url: drep.AnchorURL}
	copy(anchor.DataHash[:], drep.AnchorHash)
	return anchor
}

// stakePoolsResult builds the GetStakePools wire result from a list of pool
// key hashes: a CBOR set (tag 258) of pool IDs wrapped in the single-element
// array the query's StructAsArray result type decodes from. cardano-cli is
// strict about both: a plain (untagged) array fails to decode with "expected
// tag", and an unsorted set fails with "Canonicity violation while decoding
// Set". The hashes are therefore emitted in ascending byte order.
func stakePoolsResult(keyHashes [][]byte) []any {
	slices.SortFunc(keyHashes, bytes.Compare)
	poolIds := make(cbor.Set, 0, len(keyHashes))
	for _, kh := range keyHashes {
		var id ledger.PoolId
		copy(id[:], kh)
		poolIds = append(poolIds, id)
	}
	return []any{poolIds}
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

// queryShelleyFilteredDelegationAndRewardAccounts answers
// GetFilteredDelegationsAndRewardAccounts: given a set of stake credentials,
// return their current pool delegations and reward balances.
//
// Wire shape: array(1)[array(2)[
//
//	map[StakeCredential]PoolId,    // delegations: only registered + delegated
//	map[StakeCredential]uint64,    // rewards: every registered cred from input
//
// ]]
//
// Semantics (matches Haskell ledger): only registered (active) accounts
// appear in either map. Unknown or deregistered credentials are silently
// filtered out. The delegations map only contains accounts whose `Pool`
// is currently set; an account that is registered but undelegated will
// appear in the rewards map only.
func (ls *LedgerState) queryShelleyFilteredDelegationAndRewardAccounts(
	creds []olocalstatequery.StakeCredential,
) (any, error) {
	delegations := make(map[olocalstatequery.StakeCredential]ledger.Blake2b224)
	rewards := make(map[olocalstatequery.StakeCredential]uint64)
	if len(creds) == 0 {
		return []any{[]any{delegations, rewards}}, nil
	}
	stakeCreds := make([]models.StakeCredentialRef, 0, len(creds))
	seen := make(map[string]struct{}, len(creds))
	for _, cred := range creds {
		credentialTag, err := models.CredentialTagFromUint64(cred.Tag)
		if err != nil {
			return nil, err
		}
		ref := models.StakeCredentialRef{
			Tag: credentialTag,
			Key: cred.Bytes[:],
		}
		key := ref.MapKey()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		stakeCreds = append(stakeCreds, ref)
	}
	accounts, err := ls.db.GetAccountsByCredential(stakeCreds, false, nil)
	if err != nil {
		return nil, err
	}
	for _, cred := range creds {
		credentialTag, err := models.CredentialTagFromUint64(cred.Tag)
		if err != nil {
			return nil, err
		}
		account, ok := accounts[models.StakeCredentialRef{
			Tag: credentialTag,
			Key: cred.Bytes[:],
		}.MapKey()]
		if !ok {
			continue
		}
		rewards[cred] = uint64(account.Reward)
		if len(account.Pool) > 0 {
			delegations[cred] = ledger.NewBlake2b224(account.Pool)
		}
	}
	return []any{[]any{delegations, rewards}}, nil
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
