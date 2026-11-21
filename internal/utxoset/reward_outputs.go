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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
)

// rewardRowChunk bounds the reward rows one store transaction writes: each
// row is a put and a journal entry, and a store transaction holds a bounded
// number of writes.
const rewardRowChunk = 10_000

// postSnapshotLoaded and postSnapshotStaged are the kinds of a credit row.
const (
	postSnapshotLoaded = 0
	postSnapshotStaged = 1
)

func rewardOutputRef(o *models.RewardAccountOutput) badgerutxo.RewardOutputKey {
	return badgerutxo.RewardOutputKey{
		Tag: o.CredentialTag, Key: o.StakingKey, Epoch: o.Epoch,
		Pool: o.PoolKeyHash, Type: o.RewardType,
	}
}

// rewardRows converts a commit's reward output, round and post-snapshot
// credit changes to journaled rows, in order.
func rewardRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0,
		2*len(lc.RewardOutputs)+len(lc.RewardRounds)+len(lc.PostSnapshotCredits))
	for i := range lc.RewardOutputs {
		c := &lc.RewardOutputs[i]
		if c.Kind == ledger.RewardOutputWipe {
			out = append(out, badgerutxo.RowPut{
				Key:             binary.BigEndian.AppendUint64(nil, c.Output.Epoch),
				WipeRewardEpoch: true,
			})
			continue
		}
		ref := rewardOutputRef(&c.Output)
		if !badgerutxo.ValidRewardOutputKey(ref) {
			return nil, fmt.Errorf(
				"utxoset: reward output key of %d+%d+%d bytes",
				len(ref.Key), len(ref.Pool), len(ref.Type),
			)
		}
		primary := badgerutxo.RowPut{
			Slot: c.Slot, Key: badgerutxo.RewardOutputRowKey(ref),
		}
		index := badgerutxo.RowPut{
			Slot: c.Slot, Key: badgerutxo.RewardEpochRowKey(ref),
		}
		if c.Kind == ledger.RewardOutputDelete {
			primary.Delete, index.Delete = true, true
		} else {
			primary.Value = badgerutxo.RewardOutputValue(
				uint64(c.Output.Amount), c.Output.CapturedSlot, c.Output.BoundarySlot,
			)
		}
		out = append(out, primary, index)
	}
	for _, c := range lc.RewardRounds {
		out = append(out, badgerutxo.RowPut{
			Slot: c.Round.BoundarySlot,
			Key:  badgerutxo.RewardRoundRowKey(c.Round.SnapshotEpoch),
			Value: binary.BigEndian.AppendUint64(
				nil, c.Round.BoundarySlot,
			),
		})
	}
	for _, c := range lc.PostSnapshotCredits {
		if len(c.Ref.Key) > 0xff {
			return nil, fmt.Errorf("utxoset: credential key of %d bytes", len(c.Ref.Key))
		}
		out = append(out, badgerutxo.RowPut{
			Slot: c.Slot,
			Key: badgerutxo.PostSnapshotCreditRowKey(
				c.Slot, c.Ref.Tag, c.Ref.Key, postSnapshotStaged, c.Source,
			),
			Value: binary.BigEndian.AppendUint64(nil, c.Amount),
		})
	}
	return out, nil
}

// ensureRewardOutputs loads the credit-eligible unfolded reward outputs, the
// credited rounds and the post-snapshot credits from the metadata store into
// a set from before it kept them.
func (s *Set) ensureRewardOutputs(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.RewardOutputsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the reward outputs marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	rounds, err := ldb.GetPendingRewardCreditRounds(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read credited rounds: %w", err)
	}
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read epochs: %w", err)
	}
	outputs, credits := 0, 0
	err = s.store.LoadRewardOutputs(func(put func(k, v []byte) error) error {
		for _, r := range rounds {
			err := put(
				badgerutxo.RewardRoundRowKey(r.SnapshotEpoch),
				binary.BigEndian.AppendUint64(nil, r.BoundarySlot),
			)
			if err != nil {
				return err
			}
		}
		err := ldb.IterateUnfoldedRewardAccountOutputs(txn, func(o *models.RewardAccountOutput) error {
			ref := rewardOutputRef(o)
			if !badgerutxo.ValidRewardOutputKey(ref) {
				return fmt.Errorf("utxoset: reward output key of %d+%d+%d bytes",
					len(ref.Key), len(ref.Pool), len(ref.Type))
			}
			err := put(badgerutxo.RewardOutputRowKey(ref), badgerutxo.RewardOutputValue(
				uint64(o.Amount), o.CapturedSlot, o.BoundarySlot,
			))
			if err != nil {
				return err
			}
			outputs++
			return put(badgerutxo.RewardEpochRowKey(ref), nil)
		})
		if err != nil {
			return err
		}
		// A boundary credits after its SNAP point at its own slot, the
		// epoch's start slot, and the store indexes credits by slot.
		for _, e := range epochs {
			cs, err := ldb.GetPostSnapshotRewardCredits(e.StartSlot, txn)
			if err != nil {
				return err
			}
			for _, c := range cs {
				if len(c.StakingKey) > 0xff {
					return fmt.Errorf("utxoset: credential key of %d bytes", len(c.StakingKey))
				}
				err := put(
					badgerutxo.PostSnapshotCreditRowKey(
						e.StartSlot, c.CredentialTag, c.StakingKey,
						postSnapshotLoaded, nil,
					),
					binary.BigEndian.AppendUint64(nil, uint64(c.Amount)),
				)
				if err != nil {
					return err
				}
				credits++
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load reward outputs: %w", err)
	}
	s.logger.Info("loaded reward outputs into the live UTxO set",
		"rounds", len(rounds), "outputs", outputs, "post_snapshot_credits", credits)
	return nil
}

// RewardOutputsLoaded reports whether the set holds the reward rows.
func (s *Set) RewardOutputsLoaded() (bool, error) {
	return s.store.RewardOutputsLoaded()
}

func (s *Set) eachRewardRow(
	prefix []byte,
	ceiling *uint64,
	fn func(k, v []byte) error,
) error {
	if ceiling != nil {
		return s.store.RowsAt(prefix, *ceiling, fn)
	}
	return s.store.Rows(prefix, fn)
}

func decodeRewardRound(k, v []byte) (models.RewardCreditRound, error) {
	if len(k) != 9 || len(v) != 8 {
		return models.RewardCreditRound{}, errors.New("utxoset: malformed reward round row")
	}
	return models.RewardCreditRound{
		SnapshotEpoch: binary.BigEndian.Uint64(k[1:]),
		BoundarySlot:  binary.BigEndian.Uint64(v),
	}, nil
}

// RewardRounds returns every credited round, oldest first.
func (s *Set) RewardRounds(ceiling *uint64) ([]models.RewardCreditRound, error) {
	var out []models.RewardCreditRound
	err := s.eachRewardRow(badgerutxo.RewardRoundPrefix(), ceiling, func(k, v []byte) error {
		r, err := decodeRewardRound(k, v)
		out = append(out, r)
		return err
	})
	return out, err
}

// RewardRound returns the credited round of epoch.
func (s *Set) RewardRound(
	epoch uint64,
	ceiling *uint64,
) (models.RewardCreditRound, bool, error) {
	key := badgerutxo.RewardRoundRowKey(epoch)
	var v []byte
	var ok bool
	var err error
	if ceiling != nil {
		v, ok, err = s.store.RowAt(key, *ceiling)
	} else {
		v, ok, err = s.store.Row(key)
	}
	if err != nil || !ok {
		return models.RewardCreditRound{}, false, err
	}
	r, err := decodeRewardRound(key, v)
	return r, err == nil, err
}

// CredentialRewardOutputs returns every output row of one credential.
func (s *Set) CredentialRewardOutputs(
	tag uint8,
	key []byte,
	ceiling *uint64,
) ([]models.RewardAccountOutput, error) {
	var out []models.RewardAccountOutput
	err := s.eachRewardRow(
		badgerutxo.RewardCredentialPrefix(tag, key), ceiling,
		func(k, v []byte) error {
			ref, err := badgerutxo.DecodeRewardOutputRowKey(k)
			if err != nil {
				return err
			}
			amount, captured, boundary, err := badgerutxo.DecodeRewardOutputValue(v)
			if err != nil {
				return err
			}
			out = append(out, models.RewardAccountOutput{
				StakingKey: ref.Key, PoolKeyHash: ref.Pool, RewardType: ref.Type,
				Epoch: ref.Epoch, CredentialTag: ref.Tag,
				Amount: types.Uint64(amount), Spendable: true,
				CapturedSlot: captured, BoundarySlot: boundary,
			})
			return nil
		},
	)
	return out, err
}

// RewardOutputEpochs returns the epochs that have an output row.
func (s *Set) RewardOutputEpochs(ceiling *uint64) ([]uint64, error) {
	if ceiling == nil {
		return s.store.RewardEpochs()
	}
	var out []uint64
	err := s.eachRewardRow([]byte{badgerutxo.RewardEpochTag}, ceiling, func(k, _ []byte) error {
		ref, err := badgerutxo.DecodeRewardEpochRowKey(k)
		if err != nil {
			return err
		}
		if n := len(out); n == 0 || out[n-1] != ref.Epoch {
			out = append(out, ref.Epoch)
		}
		return nil
	})
	return out, err
}

var errStopRewardScan = errors.New("utxoset: stop reward scan")

// EpochRewardOutputs returns up to limit outputs of epoch.
func (s *Set) EpochRewardOutputs(
	epoch uint64,
	limit int,
	ceiling *uint64,
) ([]ledger.RewardOutputRef, error) {
	var out []ledger.RewardOutputRef
	if limit <= 0 {
		return out, nil
	}
	err := s.eachRewardRow(badgerutxo.RewardEpochPrefix(epoch), ceiling, func(k, _ []byte) error {
		ref, err := badgerutxo.DecodeRewardEpochRowKey(k)
		if err != nil {
			return err
		}
		out = append(out, ledger.RewardOutputRef{
			Tag: ref.Tag, Key: ref.Key, Epoch: ref.Epoch, Pool: ref.Pool, Type: ref.Type,
		})
		if len(out) >= limit {
			return errStopRewardScan
		}
		return nil
	})
	if errors.Is(err, errStopRewardScan) {
		err = nil
	}
	return out, err
}

// PostSnapshotCredits returns the credits at slot.
func (s *Set) PostSnapshotCredits(
	slot uint64,
	ceiling *uint64,
) ([]ledger.PostSnapshotCredit, error) {
	var out []ledger.PostSnapshotCredit
	err := s.eachRewardRow(
		badgerutxo.PostSnapshotCreditPrefix(slot), ceiling,
		func(k, v []byte) error {
			at, tag, key, kind, source, err := badgerutxo.DecodePostSnapshotCreditRowKey(k)
			if err != nil {
				return err
			}
			if len(v) != 8 {
				return errors.New("utxoset: malformed post-snapshot credit row")
			}
			out = append(out, ledger.PostSnapshotCredit{
				Slot: at, Ref: models.NewStakeCredentialRef(tag, key),
				Source: source, Amount: binary.BigEndian.Uint64(v),
				Loaded: kind == postSnapshotLoaded,
			})
			return nil
		},
	)
	return out, err
}

var _ ledger.RewardOutputReader = (*Set)(nil)
