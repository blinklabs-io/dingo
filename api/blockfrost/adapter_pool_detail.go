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
	// PoolsExtended and Network.
	currentEpoch := a.ledgerState.CurrentEpoch()
	activeStakeEpoch := uint64(0)
	if currentEpoch >= 2 {
		activeStakeEpoch = currentEpoch - 2
	}
	snapshots, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
		activeStakeEpoch, "mark", txn.Metadata(),
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get pool stake snapshots for epoch %d: %w",
			activeStakeEpoch, err,
		)
	}
	var activeStake uint64
	for _, snapshot := range snapshots {
		if string(snapshot.PoolKeyHash) == string(pool.PoolKeyHash) {
			activeStake = uint64(snapshot.TotalStake)
			break
		}
	}

	// Network-wide totals for the live/active size ratios and saturation.
	totalActiveStake, err := db.Metadata().GetTotalActiveStake(
		activeStakeEpoch, "mark", txn.Metadata(),
	)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get total active stake: %w", err,
		)
	}
	totalLiveStake, err := a.liveStake()
	if err != nil {
		return PoolDetailInfo{}, err
	}

	// nOpt drives live_saturation. Protocol parameters may not be loaded
	// yet early in a node's life (before the first epoch boundary); that is
	// treated as nOpt = 0 (no saturation figure) rather than failing pool
	// detail entirely, the same way other pool queries in this package
	// degrade when startup-dependent state isn't ready yet (see
	// GetActivePoolRelays and GetPool's retirement check).
	nOpt := 0
	if protocolParams, ppErr := a.CurrentProtocolParams(); ppErr == nil {
		nOpt = protocolParams.NOpt
	}
	liveSize, activeSize, liveSaturation := poolSizeSaturation(
		liveStake, activeStake, totalLiveStake, totalActiveStake, nOpt,
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

	epochStartSlot := uint64(0)
	epochRow, err := db.GetEpoch(currentEpoch, txn)
	if err != nil {
		return PoolDetailInfo{}, fmt.Errorf(
			"get epoch %d: %w", currentEpoch, err,
		)
	}
	if epochRow != nil {
		epochStartSlot = epochRow.StartSlot
	}
	blocksEpochByPool, _, err := db.Metadata().CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{pkh}, epochStartSlot, noSlotUpperBound, txn.Metadata(),
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

// poolSizeSaturation computes a pool's live/active stake-size ratios and
// live-saturation fraction relative to network-wide totals and the nOpt (k)
// protocol parameter, matching Blockfrost's live_size, active_size, and
// live_saturation semantics: live_size and active_size are the pool's share
// of total live/active stake, and live_saturation is live stake relative to
// the per-pool saturation threshold (total active stake / nOpt). Each ratio
// is zero when its denominator is zero (network totals not yet available,
// or nOpt not yet configured) rather than dividing by zero.
func poolSizeSaturation(
	liveStake uint64,
	activeStake uint64,
	totalLiveStake uint64,
	totalActiveStake uint64,
	nOpt int,
) (liveSize float64, activeSize float64, liveSaturation float64) {
	if totalLiveStake > 0 {
		liveSize = float64(liveStake) / float64(totalLiveStake)
	}
	if totalActiveStake > 0 {
		activeSize = float64(activeStake) / float64(totalActiveStake)
		if nOpt > 0 {
			saturationThreshold := float64(totalActiveStake) / float64(nOpt)
			if saturationThreshold > 0 {
				liveSaturation = float64(liveStake) / saturationThreshold
			}
		}
	}
	return liveSize, activeSize, liveSaturation
}
