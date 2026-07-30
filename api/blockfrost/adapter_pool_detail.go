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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// noSlotUpperBound stands in for "no upper bound" in slot-range queries
// that want everything synced so far. math.MaxInt64, not math.MaxUint64:
// slot columns are bound through database/sql as signed 64-bit integers,
// and a uint64 value with the high bit set (like math.MaxUint64) is
// rejected by the sqlite driver. MaxInt64 slots are billions of years past
// any real chain tip, so it is effectively unbounded for this purpose.
const noSlotUpperBound = uint64(math.MaxInt64)

// PoolDetail returns the OpenAPI pool detail object for the requested pool
// (bech32 or hex ID). Epoch-sensitive aggregates (active stake, saturation,
// blocks_epoch) are computed for the current epoch.
func (a *NodeAdapter) PoolDetail(poolID string) (PoolDetailInfo, error) {
	poolKeyHash, err := parsePoolID(poolID)
	if err != nil {
		return PoolDetailInfo{}, err
	}
	pkh := lcommon.PoolKeyHash(poolKeyHash)

	db := a.ledgerState.Database()
	txn := db.Transaction(false)
	defer txn.Release()

	pool, err := db.Metadata().GetPool(pkh, true, txn.Metadata())
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get pool %x: %w", poolKeyHash, err,
		)
	}
	if pool == nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"pool %x: %w", poolKeyHash, models.ErrPoolNotFound,
		)
	}
	if len(pool.Registration) == 0 {
		// A pool row with no registration at all should not exist in
		// practice (GetPool preloads the latest one), but guard against it
		// rather than indexing into an empty slice below.
		return PoolDetailInfo{}, fmt.Errorf(
			"pool %x has no registration: %w",
			poolKeyHash, models.ErrPoolNotFound,
		)
	}
	reg := pool.Registration[0]

	networkID := a.networkID()

	rewardCredType := uint(lcommon.CredentialTypeAddrKeyHash)
	if pool.RewardAccountCredentialTag == 1 {
		rewardCredType = uint(lcommon.CredentialTypeScriptHash)
	}
	rewardAccount, err := stakeAddressFromCredential(
		lcommon.Credential{
			CredType: rewardCredType,
			Credential: lcommon.CredentialHash(
				lcommon.NewBlake2b224(pool.RewardAccount),
			),
		},
		networkID,
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"encode reward account for pool %x: %w", poolKeyHash, err,
		)
	}

	// Owners are always key-hash credentials per the pool registration
	// certificate spec (unlike the reward account, which may be a script).
	owners := make([]string, 0, len(reg.Owners))
	ownerKeyHashes := make([][]byte, 0, len(reg.Owners))
	for _, owner := range reg.Owners {
		addr, err := stakeAddressFromCredential(
			lcommon.Credential{
				CredType: uint(lcommon.CredentialTypeAddrKeyHash),
				Credential: lcommon.CredentialHash(
					lcommon.NewBlake2b224(owner.KeyHash),
				),
			},
			networkID,
		)
		if err != nil {
			return PoolDetailInfo{}, fmt.Errorf(
				"encode owner address for pool %x: %w", poolKeyHash, err,
			)
		}
		owners = append(owners, addr)
		ownerKeyHashes = append(ownerKeyHashes, owner.KeyHash)
	}

	marginCost := 0.0
	if pool.Margin != nil && pool.Margin.Rat != nil {
		marginCost, _ = pool.Margin.Float64()
	}

	// Live stake and delegator count.
	liveStakeByPool, delegatorsByPool, err := db.Metadata().GetStakeByPools(
		[][]byte{pool.PoolKeyHash}, txn.Metadata(),
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get live stake for pool %x: %w", poolKeyHash, err,
		)
	}
	liveStake := liveStakeByPool[string(pool.PoolKeyHash)]
	liveDelegators := delegatorsByPool[string(pool.PoolKeyHash)]

	// Active stake: the Mark snapshot at currentEpoch-2, matching
	// PoolsExtended and Network. A single targeted row lookup rather than
	// GetPoolStakeSnapshotsByEpoch's full-network fetch-then-linear-scan:
	// on mainnet that call returns ~3,000 pools' rows to find the one that
	// matches.
	currentEpoch := a.ledgerState.CurrentEpoch()
	activeStakeEpoch := uint64(0)
	if currentEpoch >= 2 {
		activeStakeEpoch = currentEpoch - 2
	}
	snapshot, err := db.Metadata().GetPoolStakeSnapshot(
		activeStakeEpoch, "mark", pool.PoolKeyHash, txn.Metadata(),
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get pool stake snapshot for epoch %d: %w",
			activeStakeEpoch, err,
		)
	}
	var activeStake uint64
	if snapshot != nil {
		activeStake = uint64(snapshot.TotalStake)
	}

	// Network-wide total active stake, for active_size. GetTotalActiveStake
	// is already a targeted aggregate (an EpochSummary fast path, falling
	// back to a single SUM query), not a per-pool scan, so nothing further
	// to avoid here.
	totalActiveStake, err := db.Metadata().GetTotalActiveStake(
		activeStakeEpoch, "mark", txn.Metadata(),
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get total active stake: %w", err,
		)
	}
	// active_size is a required, non-nullable float with no schema-
	// compatible "unknown" placeholder — the same constraint documented
	// on live_saturation below. 0.0 is a value a real pool can
	// legitimately have (no active stake), so it cannot double as a
	// placeholder for "no snapshot captured for this epoch yet".
	// activeStakeEpoch floors to 0 for currentEpoch < 2, and any node
	// missing that epoch's snapshot hits this through the normal path,
	// not a pathological one. Error instead of silently reporting every
	// pool as 0% active-saturated, consistent with the protocol-parameters
	// case below.
	if totalActiveStake == 0 {
		return PoolDetailInfo{}, fmt.Errorf(
			"get total active stake for epoch %d: no snapshot captured",
			activeStakeEpoch,
		)
	}

	// Network-wide total live stake, for live_size. Unlike active stake's
	// total, there is no pre-aggregated column to query directly here, so
	// this genuinely requires summing every pool's delegated stake — the
	// same cost PoolsExtended and Network already pay for the same
	// number. liveStake reads within txn so this stays in the same
	// snapshot as the reads above (see liveStake's doc comment for the
	// one read that still can't).
	totalLiveStake, err := a.liveStake(txn.Metadata())
	if err != nil {
		return PoolDetailInfo{}, err
	}

	// nOpt drives live_saturation, which is a required, non-nullable float
	// in the OpenAPI schema: there is no schema-compatible way to signal
	// "unknown" for it, and 0.0 is a legitimate saturation value (a pool
	// with no live stake), so it cannot double as a placeholder for
	// "protocol parameters aren't loaded yet". Propagate the error instead
	// of guessing.
	//
	// CurrentProtocolParams reads the ledger state's own cached current
	// parameters rather than a value scoped to txn — the same convention
	// PoolsExtended and Network already rely on for their own out-of-txn
	// reads (see liveStake). It matters less here in practice than it
	// would for a live-stake read: protocol parameters only change at
	// epoch boundaries via governance action, not every block, so the
	// window in which this could disagree with the rest of this
	// snapshot is far narrower.
	protocolParams, err := a.CurrentProtocolParams()
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get protocol parameters: %w", err,
		)
	}

	// live_saturation's denominator is total circulating supply, not
	// total active stake: see totalCirculation's doc comment.
	totalCirculation, err := a.totalCirculation(txn.Metadata())
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get total circulation: %w", err,
		)
	}
	liveSize, activeSize, liveSaturation := poolSizeSaturation(
		liveStake, activeStake, totalLiveStake, totalActiveStake,
		totalCirculation, protocolParams.NOpt,
	)

	// Live pledge: the live stake currently delegated to this pool by its
	// declared owner credentials (CIP-50 "current pledge"), as of now.
	// noSlotUpperBound and expiryEpoch/inactivityPeriod = 0 mean "as of the
	// synced tip, with the CIP-0163 inactivity gate off", matching every
	// other at-tip caller of this store method.
	var livePledge uint64
	if len(ownerKeyHashes) > 0 {
		ownerStake, err := db.Metadata().GetPoolOwnerStakeAtSlot(
			ownerKeyHashes, noSlotUpperBound, 0, 0, txn.Metadata(),
		)
		if err != nil {
			return PoolDetailInfo{}, fmt.Errorf(
				"get live pledge for pool %x: %w", poolKeyHash, err,
			)
		}
		for _, ownerKeyHash := range ownerKeyHashes {
			livePledge += ownerStake[dbtypes.PoolCredentialStakeKey(
				pool.PoolKeyHash, 0, ownerKeyHash,
			)]
		}
	}

	// Lifetime blocks minted: every observed op-cert-sequence row for this
	// pool. That table is written once per block the pool has produced and
	// is never pruned by epoch (see UpdatePoolOpCertSequence in
	// database/pool.go and DATABASE.md's pool_opcert_sequence entry), so a
	// full-range count is a genuine lifetime total, not an approximation.
	// blocks_epoch narrows the same indexed query to the current epoch's
	// slot range.
	blocksMintedByPool, _, err := db.Metadata().CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{pkh}, 0, noSlotUpperBound, txn.Metadata(),
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"count lifetime blocks for pool %x: %w", poolKeyHash, err,
		)
	}

	// blocks_epoch narrows the lifetime query above to the current epoch's
	// slot range. If GetEpoch returns nil for the current epoch, the state
	// needed to do that narrowing isn't there: defaulting epochStartSlot
	// to 0 would silently reproduce the blocks_minted query above
	// (0..noSlotUpperBound) and report the pool's entire history as
	// current-epoch blocks, so this errors instead.
	//
	// The upper bound is noSlotUpperBound, not the epoch's end slot — only
	// correct because this is always the CURRENT epoch, where no blocks
	// exist past the synced tip. Reusing this for a historical epoch would
	// need the epoch's actual end slot as the upper bound instead.
	epochRow, err := db.GetEpoch(currentEpoch, txn)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get epoch %d: %w", currentEpoch, err,
		)
	}
	if epochRow == nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get epoch %d: no epoch row found", currentEpoch,
		)
	}
	blocksEpochByPool, _, err := db.Metadata().CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{pkh}, epochRow.StartSlot, noSlotUpperBound, txn.Metadata(),
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"count epoch blocks for pool %x: %w", poolKeyHash, err,
		)
	}

	registrationTxHashes, retirementTxHashes, err := db.Metadata().
		GetPoolCertificateHistory(pkh, txn.Metadata())
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get certificate history for pool %x: %w", poolKeyHash, err,
		)
	}
	registration := make([]string, 0, len(registrationTxHashes))
	for _, h := range registrationTxHashes {
		registration = append(registration, hex.EncodeToString(h))
	}
	retirement := make([]string, 0, len(retirementTxHashes))
	for _, h := range retirementTxHashes {
		retirement = append(retirement, hex.EncodeToString(h))
	}

	return PoolDetailInfo{
		PoolID: lcommon.PoolId(
			lcommon.NewBlake2b224(poolKeyHash),
		).String(),
		Hex:            hex.EncodeToString(poolKeyHash),
		VrfKey:         hex.EncodeToString(pool.VrfKeyHash),
		BlocksMinted:   blocksMintedByPool[string(pool.PoolKeyHash)],
		BlocksEpoch:    blocksEpochByPool[string(pool.PoolKeyHash)],
		LiveStake:      strconv.FormatUint(liveStake, 10),
		LiveSize:       liveSize,
		LiveSaturation: liveSaturation,
		LiveDelegators: liveDelegators,
		ActiveStake:    strconv.FormatUint(activeStake, 10),
		ActiveSize:     activeSize,
		DeclaredPledge: strconv.FormatUint(uint64(pool.Pledge), 10),
		LivePledge:     strconv.FormatUint(livePledge, 10),
		MarginCost:     marginCost,
		FixedCost:      strconv.FormatUint(uint64(pool.Cost), 10),
		RewardAccount:  rewardAccount,
		Owners:         owners,
		Registration:   registration,
		Retirement:     retirement,
		CalidusKey:     nil,
	}, nil
}

// totalCirculation returns MaxLovelaceSupply minus Reserves: the sigma
// denominator the reward calculation uses for the per-pool saturation
// threshold. ledger/rewards/rewards.go:287 computes exactly this quantity
// (`totalCirculation := params.MaxLovelaceSupply - pots.Reserves`), and
// rewards.go:~1028-1034 passes it as totalStake into
// optimalPoolRewardChecked, where sigma = poolStake/totalStake is capped at
// z0 = 1/optimalPoolCount — a deliberately different denominator from
// totalActiveStake, which the same call passes to apparentPerformance one
// line above.
//
// This is NOT the same figure as Network()'s local "circulating" value
// (adapter.go), which further subtracts treasury and script-locked supply
// for display purposes. Callers computing a saturation threshold must use
// this function's result, never that one.
func (a *NodeAdapter) totalCirculation(txn dbtypes.Txn) (uint64, error) {
	nodeCfg := a.ledgerState.CardanoNodeConfig()
	if nodeCfg == nil || nodeCfg.ShelleyGenesis() == nil {
		return 0, errors.New("shelley genesis not available")
	}
	maxSupply := nodeCfg.ShelleyGenesis().MaxLovelaceSupply

	state, err := a.ledgerState.Database().Metadata().GetNetworkState(txn)
	if err != nil {
		return 0, fmt.Errorf("get network state: %w", err)
	}
	reserves := uint64(0)
	if state != nil {
		reserves = uint64(state.Reserves)
	}
	// An impossible ledger state, but returning 0 here would feed a zero
	// denominator into live_saturation and surface as a plausible-looking
	// 0.0 — the fabricated value every other unavailable input in this
	// function errors on rather than guessing.
	if reserves > maxSupply {
		return 0, fmt.Errorf(
			"reserves %d exceed max lovelace supply %d",
			reserves,
			maxSupply,
		)
	}
	return maxSupply - reserves, nil
}

// poolSizeSaturation computes a pool's live/active stake-size ratios and
// live-saturation fraction, matching Blockfrost's live_size, active_size,
// and live_saturation semantics. The three outputs each divide by a
// different network-wide total:
//   - live_size divides by totalLiveStake, the network's total delegated
//     (live) stake.
//   - active_size divides by totalActiveStake, the network's total Mark-
//     snapshot (active) stake for the relevant epoch.
//   - live_saturation divides live stake by the per-pool saturation
//     threshold, totalCirculation / nOpt. totalCirculation is deliberately
//     NOT totalActiveStake: see the totalCirculation doc comment above for
//     why the reward calculation requires this distinction.
//
// The caller is responsible for nOpt being a real, loaded protocol
// parameter value — this function only guards the pathological case of a
// zero denominator (e.g. a network with no active stake or circulation
// figure yet captured) to avoid dividing by zero; it does not stand in for
// "nOpt unavailable" or "totalCirculation unavailable".
func poolSizeSaturation(
	liveStake uint64,
	activeStake uint64,
	totalLiveStake uint64,
	totalActiveStake uint64,
	totalCirculation uint64,
	nOpt int,
) (liveSize float64, activeSize float64, liveSaturation float64) {
	if totalLiveStake > 0 {
		liveSize = float64(liveStake) / float64(totalLiveStake)
	}
	if totalActiveStake > 0 {
		activeSize = float64(activeStake) / float64(totalActiveStake)
	}
	if totalCirculation > 0 && nOpt > 0 {
		saturationThreshold := float64(totalCirculation) / float64(nOpt)
		if saturationThreshold > 0 {
			liveSaturation = float64(liveStake) / saturationThreshold
		}
	}
	return liveSize, activeSize, liveSaturation
}
