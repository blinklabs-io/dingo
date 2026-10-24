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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/bits"

	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database/models"
	gconsensus "github.com/blinklabs-io/gouroboros/consensus"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

const maxTachysDistributionCacheEntries = 3

type tachysDistributionCacheEntry struct {
	contextGeneration uint64
	snapshotEpoch     uint64
	distribution      *nodeconsensus.TachysDistribution
}

// TachysDistribution reads a complete mark-snapshot distribution and its
// denominator through one metadata transaction.
func (ls *LedgerState) TachysDistribution(
	ctx context.Context,
	snapshotEpoch uint64,
) (_ *nodeconsensus.TachysDistribution, err error) {
	if ls == nil || ls.db == nil {
		return nil, errors.New("ledger database unavailable")
	}
	snapshot := ls.loadConsensusSnapshot()
	if snapshot == nil {
		return nil, errors.New("ledger consensus context unavailable")
	}
	ls.tachysMu.Lock()
	defer ls.tachysMu.Unlock()
	for _, cached := range ls.tachysDistributions {
		if cached.contextGeneration == snapshot.contextGeneration &&
			cached.snapshotEpoch == snapshotEpoch {
			return cached.distribution, nil
		}
	}
	txn := ls.db.MetadataTxn(ctx, false)
	if txn == nil {
		return nil, errors.New("metadata transaction unavailable")
	}
	released := false
	defer func() {
		if released {
			return
		}
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"release Tachys distribution transaction: %w",
					rollbackErr,
				),
			)
		}
	}()
	view := ls.NewView(txn)
	dist, err := view.GetStakeDistribution(snapshotEpoch)
	if err != nil {
		return nil, err
	}
	totalStake, err := view.GetTotalActiveStake(snapshotEpoch)
	if err != nil {
		return nil, err
	}
	pools, err := tachysPoolsFromStakeDistribution(
		dist.PoolStakes,
		totalStake,
	)
	if err != nil {
		return nil, err
	}
	distribution, err := nodeconsensus.NewTachysDistribution(
		pools,
		totalStake,
	)
	if err != nil {
		return nil, err
	}
	if err := txn.Rollback(); err != nil {
		return nil, fmt.Errorf(
			"release Tachys distribution transaction: %w",
			err,
		)
	}
	released = true
	current := ls.loadConsensusSnapshot()
	if current == nil ||
		current.contextGeneration != snapshot.contextGeneration {
		return nil, errors.New("tachys consensus context changed while loading distribution")
	}
	ls.tachysDistributions = append(
		ls.tachysDistributions,
		tachysDistributionCacheEntry{
			contextGeneration: snapshot.contextGeneration,
			snapshotEpoch:     snapshotEpoch,
			distribution:      distribution,
		},
	)
	if len(ls.tachysDistributions) > maxTachysDistributionCacheEntries {
		ls.tachysDistributions = append(
			[]tachysDistributionCacheEntry(nil),
			ls.tachysDistributions[len(ls.tachysDistributions)-maxTachysDistributionCacheEntries:]...,
		)
	}
	return distribution, nil
}

func tachysPoolsFromStakeDistribution(
	poolStakes map[string]uint64,
	expectedTotal uint64,
) ([]gconsensus.TachysPool, error) {
	pools := make([]gconsensus.TachysPool, 0, len(poolStakes))
	var total uint64
	for encodedID, stake := range poolStakes {
		// Snapshot rotation preserves buckets for registered pools with no
		// delegated stake. They have no interval in CIP-0177.
		if stake == 0 {
			continue
		}
		poolBytes, err := hex.DecodeString(encodedID)
		if err != nil {
			return nil, fmt.Errorf(
				"decode Tachys pool ID %q: %w",
				encodedID,
				err,
			)
		}
		poolHash, err := common.NewBlake2b224Checked(poolBytes)
		if err != nil {
			return nil, fmt.Errorf(
				"validate Tachys pool ID %q: %w",
				encodedID,
				err,
			)
		}
		pools = append(pools, gconsensus.TachysPool{
			ID:    common.PoolKeyHash(poolHash),
			Stake: stake,
		})
		var carry uint64
		total, carry = bits.Add64(total, stake, 0)
		if carry != 0 {
			return nil, errors.New("tachys distribution stake overflows uint64")
		}
	}
	if total != expectedTotal {
		return nil, fmt.Errorf(
			"tachys distribution total mismatch: got %d want %d",
			total,
			expectedTotal,
		)
	}
	return pools, nil
}

// ValidateTachysHeader retains every Praos cryptographic and ledger-state
// check while replacing only private-threshold issuer authorization with the
// public CIP-0177 schedule.
func (ls *LedgerState) ValidateTachysHeader(
	ctx context.Context,
	block gledger.Block,
	allowEpochCacheAdvance bool,
	allowStateDefer bool,
) error {
	epoch, epochCache, err := ls.verifyBlockHeaderStatelessCryptoWithCache(
		ctx,
		block,
		allowEpochCacheAdvance,
	)
	if err != nil {
		return err
	}
	return ls.verifyTachysHeaderStateWithCache(
		ctx,
		block,
		epoch.EpochId,
		epochCache,
		allowStateDefer,
		epoch.Nonce,
	)
}

func (ls *LedgerState) verifyTachysHeaderStateWithCache(
	ctx context.Context,
	block gledger.Block,
	epochID uint64,
	epochCache []models.Epoch,
	allowStateDefer bool,
	epochNonce []byte,
) error {
	return ls.verifyBlockHeaderStateWithAuthorization(
		ctx,
		block,
		epochID,
		epochCache,
		allowStateDefer,
		func(
			ctx context.Context,
			block gledger.Block,
			epochID uint64,
			epochCache []models.Epoch,
			snap electingSnapshot,
		) error {
			return ls.verifyTachysLeaderEligibility(
				ctx,
				block,
				epochID,
				snap,
				epochNonce,
			)
		},
	)
}

func (ls *LedgerState) verifyTachysLeaderEligibility(
	ctx context.Context,
	block gledger.Block,
	epochID uint64,
	_ electingSnapshot,
	epochNonce []byte,
) error {
	eraID := block.Era().Id
	if eraID != babbage.EraIdBabbage && eraID != conway.EraIdConway {
		return fmt.Errorf(
			"tachys does not support block era %d at slot %d",
			eraID,
			block.SlotNumber(),
		)
	}
	if len(epochNonce) != common.Blake2b256Size {
		return fmt.Errorf(
			"%w: tachys epoch nonce unavailable for epoch %d",
			errHeaderVerificationDeferred,
			epochID,
		)
	}
	distribution, err := ls.TachysDistribution(
		ctx,
		praos.StakeSnapshotEpoch(epochID),
	)
	if err != nil {
		return fmt.Errorf(
			"%w: tachys election distribution for epoch %d: %w",
			errLeaderStakeSnapshotUnavailable,
			epochID,
			err,
		)
	}
	selector, err := distribution.Selector()
	if err != nil {
		return fmt.Errorf(
			"%w: prepare tachys election distribution for epoch %d: %w",
			errLeaderStakeSnapshotUnavailable,
			epochID,
			err,
		)
	}
	issuer := block.IssuerVkey()
	authorized, err := selector.AuthorizeIssuer(
		epochNonce,
		block.SlotNumber(),
		issuer[:],
	)
	if err != nil {
		return fmt.Errorf(
			"tachys issuer authorization at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	if !authorized {
		return fmt.Errorf(
			"block header verification rejected at slot %d: producer is not the scheduled Tachys leader",
			block.SlotNumber(),
		)
	}
	return nil
}
