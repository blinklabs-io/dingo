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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
)

const (
	outputSpendable byte = 1
	outputGuarded   byte = 2
	outputApparent  byte = 4
)

func encodePoolOutput(p *models.RewardPoolOutput) []byte {
	var perf []byte
	has := p.ApparentPerformance != nil && p.ApparentPerformance.Rat != nil
	if has {
		perf = []byte(p.ApparentPerformance.String())
	}
	v := []byte{flag(has, outputApparent)}
	for _, n := range []uint64{
		uint64(p.OptimalReward), uint64(p.TotalReward),
		uint64(p.LeaderReward), uint64(p.LeaderRewardDeficit),
		uint64(p.MemberRewardTotal), uint64(p.OwnerStake),
		uint64(p.Undistributed), uint64(p.Unspendable),
		p.CapturedSlot, p.BoundarySlot,
	} {
		v = appendU64(v, n)
	}
	return appendBytes(v, perf)
}

func decodePoolOutput(k, v []byte) (*models.RewardPoolOutput, error) {
	epoch, pool, err := badgerutxo.DecodeRewardPoolOutputKey(k)
	if err != nil {
		return nil, err
	}
	c := rewardCursor{b: v}
	f := c.u8()
	out := &models.RewardPoolOutput{
		PoolKeyHash: slices.Clone(pool),
		Epoch:       epoch,
	}
	out.OptimalReward = types.Uint64(c.u64())
	out.TotalReward = types.Uint64(c.u64())
	out.LeaderReward = types.Uint64(c.u64())
	out.LeaderRewardDeficit = types.Uint64(c.u64())
	out.MemberRewardTotal = types.Uint64(c.u64())
	out.OwnerStake = types.Uint64(c.u64())
	out.Undistributed = types.Uint64(c.u64())
	out.Unspendable = types.Uint64(c.u64())
	out.CapturedSlot, out.BoundarySlot = c.u64(), c.u64()
	perf := c.bytes()
	if f&outputApparent != 0 {
		r, ok := new(big.Rat).SetString(string(perf))
		if !ok {
			return nil, errRewardInputRow
		}
		out.ApparentPerformance = &types.Rat{Rat: r}
	}
	return out, c.err()
}

func encodeAccountOutput(a *models.RewardAccountOutput) []byte {
	v := []byte{
		flag(a.Spendable, outputSpendable) | flag(a.Guarded, outputGuarded),
	}
	v = appendU64(v, uint64(a.Amount))
	v = appendU64(v, a.CapturedSlot)
	return appendU64(v, a.BoundarySlot)
}

func decodeAccountOutput(k, v []byte) (*models.RewardAccountOutput, error) {
	key, err := badgerutxo.DecodeRewardAccountOutputKey(k)
	if err != nil {
		return nil, err
	}
	c := rewardCursor{b: v}
	f := c.u8()
	out := &models.RewardAccountOutput{
		StakingKey: key.Key, PoolKeyHash: key.Pool, RewardType: key.Type,
		Epoch: key.Epoch, CredentialTag: key.Tag,
		Spendable: f&outputSpendable != 0, Guarded: f&outputGuarded != 0,
	}
	out.Amount = types.Uint64(c.u64())
	out.CapturedSlot, out.BoundarySlot = c.u64(), c.u64()
	return out, c.err()
}

// rewardDerivedPuts converts the commit's derived reward changes. Every put
// of an epoch's rows repeats the range tombstone at the rows' captured slot,
// which every chunk of one precompute shares, so the journal holds one entry
// for the epoch however many chunks and rechecks write under it.
func rewardDerivedPuts(
	lc ledger.LedgerChanges,
	s *Set,
) ([]badgerutxo.RowPut, error) {
	var rows []badgerutxo.RowPut
	tombstoned := map[string]bool{}
	under := func(prefix []byte, slot uint64) {
		if tombstoned[string(prefix)] {
			return
		}
		tombstoned[string(prefix)] = true
		rows = append(rows, badgerutxo.RowPut{
			Slot: slot, Key: prefix, TombstonePrefix: true,
		})
	}
	for _, c := range lc.RewardDerived {
		switch c.Kind {
		case ledger.RewardDerivedPools:
			for _, p := range c.Pools {
				under(
					badgerutxo.RewardDerivedEpochPrefix(p.Epoch),
					p.CapturedSlot,
				)
				rows = append(rows, badgerutxo.RowPut{
					Slot: p.CapturedSlot,
					Key: badgerutxo.RewardPoolOutputKey(
						p.Epoch,
						p.PoolKeyHash,
					),
					Value:       encodePoolOutput(p),
					SkipJournal: true,
				})
			}
		case ledger.RewardDerivedAccounts:
			for _, a := range c.Accounts {
				if !badgerutxo.ValidRewardOutputKey(badgerutxo.RewardOutputKey{
					Key: a.StakingKey, Pool: a.PoolKeyHash, Type: a.RewardType,
				}) {
					return nil, errors.New(
						"utxoset: reward output key too long",
					)
				}
				under(
					badgerutxo.RewardDerivedEpochPrefix(a.Epoch),
					a.CapturedSlot,
				)
				rows = append(rows, badgerutxo.RowPut{
					Slot: a.CapturedSlot,
					Key: badgerutxo.RewardAccountOutputKey(
						badgerutxo.RewardOutputKey{
							Tag: a.CredentialTag, Key: a.StakingKey, Epoch: a.Epoch,
							Pool: a.PoolKeyHash, Type: a.RewardType,
						},
					),
					Value:       encodeAccountOutput(a),
					SkipJournal: true,
				})
			}
		case ledger.RewardDerivedCursor:
			under(badgerutxo.RewardCursorKey(c.Epoch), c.Slot)
			rows = append(rows, badgerutxo.RowPut{
				Slot: c.Slot, Key: badgerutxo.RewardCursorKey(c.Epoch),
				Value: slices.Clone(c.Cursor), SkipJournal: true,
			})
		case ledger.RewardDerivedReplace:
			for _, p := range [][]byte{
				badgerutxo.RewardDerivedEpochPrefix(c.Epoch),
				badgerutxo.RewardCursorKey(c.Epoch),
			} {
				rows = append(rows, badgerutxo.RowPut{Key: p, WipePrefix: true})
				delete(tombstoned, string(p))
			}
		case ledger.RewardDerivedPrune:
			pruned, err := s.pruneDerived(c.PruneBelow)
			if err != nil {
				return nil, err
			}
			for _, p := range pruned {
				rows = append(rows, badgerutxo.RowPut{Key: p, WipePrefix: true})
				delete(tombstoned, string(p))
			}
		default:
			return nil, fmt.Errorf(
				"utxoset: unknown derived reward change %d",
				c.Kind,
			)
		}
	}
	return rows, nil
}

// pruneDerived lists the epoch prefixes of derived rows below below.
func (s *Set) pruneDerived(below uint64) ([][]byte, error) {
	if below == 0 {
		return nil, nil
	}
	var out [][]byte
	for _, tag := range []byte{
		badgerutxo.RewardDerivedTag, badgerutxo.RewardCursorTag,
	} {
		limit := append(
			binary.BigEndian.AppendUint64([]byte{tag}, below-1),
			bytes.Repeat([]byte{0xff}, 80)...,
		)
		for {
			k, ok, err := s.store.LastKeyAtOrBelow([]byte{tag}, limit)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			epoch := binary.BigEndian.Uint64(k[1:9])
			out = append(out, binary.BigEndian.AppendUint64([]byte{tag}, epoch))
			if epoch == 0 {
				break
			}
			limit = append(
				binary.BigEndian.AppendUint64([]byte{tag}, epoch-1),
				bytes.Repeat([]byte{0xff}, 80)...,
			)
		}
	}
	return out, nil
}

// ensureRewardDerived loads the derived reward rows the reward round can
// read from the metadata store into a set from before it kept them: the pool
// and account outputs and the cursor of each epoch in the retention window.
func (s *Set) ensureRewardDerived(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.RewardDerivedLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the derived reward marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read epochs: %w", err)
	}
	var newest uint64
	for _, e := range epochs {
		newest = max(newest, e.EpochId)
	}
	var accounts int
	err = s.store.LoadRewardDerived(func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error {
		for _, e := range epochs {
			if e.EpochId+uint64(ledger.RewardInputEpochWindow) < newest {
				continue
			}
			n, err := loadDerivedEpoch(ldb, txn, e.EpochId, put, tombstone)
			if err != nil {
				return fmt.Errorf("epoch %d: %w", e.EpochId, err)
			}
			accounts += n
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load derived reward rows: %w", err)
	}
	s.logger.Info(
		"loaded derived reward rows into the live UTxO set",
		"epochs", len(epochs), "account_outputs", accounts,
	)
	return nil
}

func loadDerivedEpoch(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	epoch uint64,
	put func(k, v []byte) error,
	tombstone func(slot uint64, prefix []byte) error,
) (int, error) {
	// The rows of an epoch share the slot their precompute captured them at,
	// the slot the boundary apply tombstones the epoch under.
	rowSlot := uint64(math.MaxUint64)
	pools, err := ldb.GetRewardPoolOutputs(epoch, txn)
	if err != nil {
		return 0, err
	}
	for _, p := range pools {
		if err := put(
			badgerutxo.RewardPoolOutputKey(epoch, p.PoolKeyHash),
			encodePoolOutput(p),
		); err != nil {
			return 0, err
		}
		rowSlot = min(rowSlot, p.CapturedSlot)
	}
	accounts, err := ldb.GetRewardAccountOutputs(epoch, txn)
	if err != nil {
		return 0, err
	}
	for _, a := range accounts {
		err := put(
			badgerutxo.RewardAccountOutputKey(badgerutxo.RewardOutputKey{
				Tag: a.CredentialTag, Key: a.StakingKey, Epoch: epoch,
				Pool: a.PoolKeyHash, Type: a.RewardType,
			}),
			encodeAccountOutput(a),
		)
		if err != nil {
			return 0, err
		}
		rowSlot = min(rowSlot, a.CapturedSlot)
	}
	if len(pools)+len(accounts) > 0 {
		err := tombstone(rowSlot, badgerutxo.RewardDerivedEpochPrefix(epoch))
		if err != nil {
			return 0, err
		}
	}
	raw, err := ldb.GetSyncState(ledger.RewardPrecomputeCursorKey(epoch), txn)
	if err != nil {
		return 0, err
	}
	if raw != "" {
		if err := put(badgerutxo.RewardCursorKey(epoch), []byte(raw)); err != nil {
			return 0, err
		}
		var h struct {
			CapturedSlot uint64 `json:"captured_slot"`
		}
		// A cursor that does not parse has no slot to drop it at; the
		// boundary apply stages such a cursor at slot 0 too.
		_ = json.Unmarshal([]byte(raw), &h)
		err := tombstone(h.CapturedSlot, badgerutxo.RewardCursorKey(epoch))
		if err != nil {
			return 0, err
		}
	}
	return len(accounts), nil
}

// RewardDerivedLoaded reports whether the set holds the derived reward rows
// of every epoch reward calculation can read.
func (s *Set) RewardDerivedLoaded() (bool, error) {
	return s.store.RewardDerivedLoaded()
}

// RewardRollbackSeq returns the durable rollback counter.
func (s *Set) RewardRollbackSeq() (uint64, error) {
	return s.store.RollbackSeq()
}

// RewardPoolOutputRows returns the epoch's pool outputs ordered by pool.
func (s *Set) RewardPoolOutputRows(
	epoch uint64, asOf *uint64,
) ([]*models.RewardPoolOutput, error) {
	var out []*models.RewardPoolOutput
	err := s.rewardRows(
		badgerutxo.RewardPoolOutputPrefix(epoch), asOf,
		func(k, v []byte) error {
			p, err := decodePoolOutput(k, v)
			if err != nil {
				return err
			}
			out = append(out, p)
			return nil
		},
	)
	return out, err
}

func (s *Set) accountOutputRows(
	prefix []byte, asOf *uint64,
) ([]*models.RewardAccountOutput, error) {
	var out []*models.RewardAccountOutput
	err := s.rewardRows(prefix, asOf, func(k, v []byte) error {
		a, err := decodeAccountOutput(k, v)
		if err != nil {
			return err
		}
		out = append(out, a)
		return nil
	})
	return out, err
}

// RewardAccountOutputRows returns the epoch's account outputs.
func (s *Set) RewardAccountOutputRows(
	epoch uint64, asOf *uint64,
) ([]*models.RewardAccountOutput, error) {
	return s.accountOutputRows(
		badgerutxo.RewardAccountOutputPrefix(epoch),
		asOf,
	)
}

// RewardCredentialOutputRows returns one credential's outputs of epoch.
func (s *Set) RewardCredentialOutputRows(
	epoch uint64, tag uint8, key []byte, asOf *uint64,
) ([]*models.RewardAccountOutput, error) {
	return s.accountOutputRows(
		badgerutxo.RewardAccountCredentialPrefix(epoch, tag, key), asOf,
	)
}

// RewardCursorRow returns the epoch's precompute cursor.
func (s *Set) RewardCursorRow(
	epoch uint64, asOf *uint64,
) ([]byte, bool, error) {
	return s.rewardRow(badgerutxo.RewardCursorKey(epoch), asOf)
}

// RewardDerivedEpochs lists the epochs holding a pool or account output
// row, oldest first.
func (s *Set) RewardDerivedEpochs() ([]uint64, error) {
	var out []uint64
	var last uint64
	first := true
	err := s.store.Rows(
		[]byte{badgerutxo.RewardDerivedTag},
		func(k, _ []byte) error {
			e := binary.BigEndian.Uint64(k[1:9])
			if first || e != last {
				out = append(out, e)
				last, first = e, false
			}
			return nil
		},
	)
	return out, err
}

var _ ledger.RewardDerivedReader = (*Set)(nil)
