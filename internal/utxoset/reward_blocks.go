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

package utxoset

import (
	"context"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/ledger"
)

func rewardBlockOps(lc ledger.LedgerChanges) []badgerutxo.RewardBlockOp {
	out := make([]badgerutxo.RewardBlockOp, 0, len(lc.RewardBlocks))
	for _, c := range lc.RewardBlocks {
		out = append(out, badgerutxo.RewardBlockOp{
			Slot:       c.Slot,
			Epoch:      c.Epoch,
			Open:       c.Open,
			Pool:       c.Pool,
			Overlay:    c.Overlay,
			Fee:        c.Fee,
			PruneBelow: c.PruneBelow,
		})
	}
	return out
}

// ensureRewardBlocks loads the block counts and fees of the newest epochs
// from the metadata store into a set from before it kept them.
func (s *Set) ensureRewardBlocks(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.RewardBlocksLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the reward block marker: %w", err)
	}
	if loaded {
		return nil
	}
	epochs, err := ledger.RewardBlocksFromStore(ctx, ldb)
	if err != nil {
		return fmt.Errorf("utxoset: read reward block counts: %w", err)
	}
	err = s.store.LoadRewardBlocks(func(put func(k, v []byte) error) error {
		for epoch, e := range epochs {
			if e.CountsKnown {
				err := put(
					badgerutxo.RewardBlockTotalKey(epoch),
					badgerutxo.EncodeRewardBlockTotal(e.Total, e.Next),
				)
				if err != nil {
					return err
				}
				for pool, n := range e.Pools {
					err := put(
						badgerutxo.RewardBlockCountKey(epoch, []byte(pool)),
						badgerutxo.EncodeRewardBlockCount(n),
					)
					if err != nil {
						return err
					}
				}
			}
			if e.AllKnown {
				err := put(
					badgerutxo.RewardBlockAllTotalKey(epoch),
					badgerutxo.EncodeRewardBlockTotal(e.AllTotal, e.AllNext),
				)
				if err != nil {
					return err
				}
				for pool, n := range e.AllPools {
					err := put(
						badgerutxo.RewardBlockAllCountKey(epoch, []byte(pool)),
						badgerutxo.EncodeRewardBlockCount(n),
					)
					if err != nil {
						return err
					}
				}
			}
			if e.FeesKnown {
				err := put(
					badgerutxo.RewardBlockFeesKey(epoch),
					badgerutxo.EncodeRewardBlockCount(e.Fees),
				)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load reward block counts: %w", err)
	}
	s.logger.Info(
		"loaded reward block counts into the live UTxO set",
		"epochs", len(epochs),
	)
	return nil
}

// RewardBlocksLoaded reports whether the set holds the block counts and fees
// of every epoch reward calculation can read.
func (s *Set) RewardBlocksLoaded() (bool, error) {
	return s.store.RewardBlocksLoaded()
}

// RewardBlocksEpoch returns epoch's block counts and fees, as a rollback to
// asOf would leave them when asOf is set. The set keeps a short window of
// epochs and no history before its load, so an epoch outside the window has
// neither known.
func (s *Set) RewardBlocksEpoch(
	epoch uint64,
	asOf *uint64,
) (ledger.RewardEpochBlocks, error) {
	out := ledger.RewardEpochBlocks{
		Pools: map[string]uint64{}, AllPools: map[string]uint64{},
	}
	fn := func(k, v []byte) error {
		_, kind, pool, err := badgerutxo.DecodeRewardBlockKey(k)
		if err != nil {
			return err
		}
		switch kind {
		case badgerutxo.RewardBlockPoolCount:
			n, err := badgerutxo.DecodeRewardBlockCount(v)
			if err != nil {
				return err
			}
			out.Pools[string(pool)] = n
		case badgerutxo.RewardBlockAllPoolCount:
			n, err := badgerutxo.DecodeRewardBlockCount(v)
			if err != nil {
				return err
			}
			out.AllPools[string(pool)] = n
		case badgerutxo.RewardBlockAllEpochTotal:
			out.AllKnown = true
			total, next, err := badgerutxo.DecodeRewardBlockTotal(v)
			out.AllTotal, out.AllNext = total, next
			return err
		case badgerutxo.RewardBlockEpochTotal:
			out.CountsKnown = true
			out.Total, out.Next, err = badgerutxo.DecodeRewardBlockTotal(v)
			return err
		case badgerutxo.RewardBlockEpochFees:
			out.FeesKnown = true
			out.Fees, err = badgerutxo.DecodeRewardBlockCount(v)
			return err
		}
		return nil
	}
	prefix := badgerutxo.RewardBlocksEpochPrefix(epoch)
	var err error
	if asOf == nil {
		err = s.store.Rows(prefix, fn)
	} else {
		err = s.store.RowsAt(prefix, *asOf, fn)
	}
	return out, err
}

// RewardBlocksEpochs lists, oldest first, the epochs the set holds a row of.
func (s *Set) RewardBlocksEpochs() ([]uint64, error) {
	var out []uint64
	err := s.store.Rows(
		[]byte{badgerutxo.RewardBlocksTag},
		func(k, _ []byte) error {
			epoch, _, _, err := badgerutxo.DecodeRewardBlockKey(k)
			if err != nil {
				return err
			}
			if len(out) == 0 || out[len(out)-1] != epoch {
				out = append(out, epoch)
			}
			return nil
		},
	)
	return slices.Clone(out), err
}
