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

// The only snapshot type reward calculation reads.
const rewardSnapshotMark = "mark"

var errRewardInputRow = errors.New("utxoset: malformed reward input row")

// rewardCursor reads a row's fields in order.
type rewardCursor struct {
	b   []byte
	bad bool
}

func (c *rewardCursor) take(n int) []byte {
	if n < 0 || len(c.b) < n {
		c.bad = true
		return make([]byte, n&0xffff)
	}
	out := c.b[:n]
	c.b = c.b[n:]
	return out
}

func (c *rewardCursor) u8() byte { return c.take(1)[0] }

func (c *rewardCursor) u64() uint64 {
	return binary.BigEndian.Uint64(c.take(8))
}

// bytes reads a 4-byte length and that many bytes.
func (c *rewardCursor) bytes() []byte {
	n := int(binary.BigEndian.Uint32(c.take(4)))
	return slices.Clone(c.take(n))
}

func (c *rewardCursor) err() error {
	if c.bad || len(c.b) != 0 {
		return errRewardInputRow
	}
	return nil
}

func appendU64(
	b []byte,
	v uint64,
) []byte {
	return binary.BigEndian.AppendUint64(b, v)
}

func appendBytes(b, v []byte) []byte {
	n := uint32(len(v)) //nolint:gosec // a row field is far below 4 GiB
	b = binary.BigEndian.AppendUint32(b, n)
	return append(b, v...)
}

func flag(b bool, bit byte) byte {
	if b {
		return bit
	}
	return 0
}

const (
	stakeOwner      byte = 1
	stakeRegistered byte = 2
)

func encodeRewardStake(s *models.RewardStakeInput) []byte {
	v := make([]byte, 0, 25)
	v = appendU64(v, uint64(s.Stake))
	v = append(v, flag(s.Owner, stakeOwner)|flag(s.Registered, stakeRegistered))
	v = appendU64(v, s.CapturedSlot)
	return appendU64(v, s.BoundarySlot)
}

func decodeRewardStake(k, v []byte) (*models.RewardStakeInput, error) {
	epoch, pool, tag, cred, err := badgerutxo.DecodeRewardStakeInputKey(k)
	if err != nil {
		return nil, err
	}
	c := rewardCursor{b: v}
	out := &models.RewardStakeInput{
		PoolKeyHash:   slices.Clone(pool),
		StakingKey:    slices.Clone(cred),
		Epoch:         epoch,
		CredentialTag: tag,
	}
	out.Stake = types.Uint64(c.u64())
	f := c.u8()
	out.Owner, out.Registered = f&stakeOwner != 0, f&stakeRegistered != 0
	out.CapturedSlot, out.BoundarySlot = c.u64(), c.u64()
	return out, c.err()
}

const (
	poolMargin byte = 1 << iota
	poolBlocks
	poolTotal
	poolAccount
)

func encodeRewardPool(p *models.RewardPoolInput) []byte {
	var margin []byte
	hasMargin := p.Margin != nil && p.Margin.Rat != nil
	if hasMargin {
		margin = []byte(p.Margin.String())
	}
	v := []byte{
		flag(
			hasMargin,
			poolMargin,
		) | flag(
			p.BlocksProduced != nil,
			poolBlocks,
		) |
			flag(
				p.TotalBlocksInEpoch != nil,
				poolTotal,
			) |
			flag(
				p.RewardAccount != nil,
				poolAccount,
			),
		p.RewardAccountCredentialTag,
	}
	for _, n := range []uint64{
		uint64(p.Pledge), uint64(p.DelegatedStake), uint64(p.OwnerStake),
		uint64(p.Cost), p.DelegatorCount, p.CapturedSlot, p.BoundarySlot,
	} {
		v = appendU64(v, n)
	}
	if p.BlocksProduced != nil {
		v = appendU64(v, *p.BlocksProduced)
	}
	if p.TotalBlocksInEpoch != nil {
		v = appendU64(v, *p.TotalBlocksInEpoch)
	}
	v = appendBytes(v, margin)
	return appendBytes(v, p.RewardAccount)
}

func decodeRewardPool(k, v []byte) (*models.RewardPoolInput, error) {
	epoch, pool, err := badgerutxo.DecodeRewardPoolInputKey(k)
	if err != nil {
		return nil, err
	}
	c := rewardCursor{b: v}
	f, tag := c.u8(), c.u8()
	out := &models.RewardPoolInput{
		PoolKeyHash: slices.Clone(pool), Epoch: epoch,
		RewardAccountCredentialTag: tag,
	}
	out.Pledge = types.Uint64(c.u64())
	out.DelegatedStake = types.Uint64(c.u64())
	out.OwnerStake = types.Uint64(c.u64())
	out.Cost = types.Uint64(c.u64())
	out.DelegatorCount = c.u64()
	out.CapturedSlot, out.BoundarySlot = c.u64(), c.u64()
	if f&poolBlocks != 0 {
		n := c.u64()
		out.BlocksProduced = &n
	}
	if f&poolTotal != 0 {
		n := c.u64()
		out.TotalBlocksInEpoch = &n
	}
	margin, account := c.bytes(), c.bytes()
	if f&poolMargin != 0 {
		r, ok := new(big.Rat).SetString(string(margin))
		if !ok {
			return nil, errRewardInputRow
		}
		out.Margin = &types.Rat{Rat: r}
	}
	if f&poolAccount != 0 {
		out.RewardAccount = account
	}
	return out, c.err()
}

const (
	snapshotExcluded byte = 1 << iota
	snapshotAuthoritative
	snapshotNonce
)

func encodeRewardSnapshot(s *models.RewardSnapshot) []byte {
	v := []byte{
		flag(s.ExcludedActiveStake != nil, snapshotExcluded) |
			flag(s.Authoritative, snapshotAuthoritative) |
			flag(s.EpochNonce != nil, snapshotNonce),
	}
	var excluded uint64
	if s.ExcludedActiveStake != nil {
		excluded = uint64(*s.ExcludedActiveStake)
	}
	for _, n := range []uint64{
		uint64(s.TotalActiveStake), excluded, s.TotalPoolCount,
		s.TotalDelegators, s.CapturedSlot, s.BoundarySlot,
		uint64(s.ProtocolVersion), uint64(s.CalculationVersion),
	} {
		v = appendU64(v, n)
	}
	return appendBytes(v, s.EpochNonce)
}

func decodeRewardSnapshot(k, v []byte) (*models.RewardSnapshot, error) {
	epoch, _, typ, err := badgerutxo.DecodeRewardSnapshotKey(k)
	if err != nil {
		return nil, err
	}
	c := rewardCursor{b: v}
	f := c.u8()
	out := &models.RewardSnapshot{Epoch: epoch, SnapshotType: typ}
	out.TotalActiveStake = types.Uint64(c.u64())
	excluded := c.u64()
	if f&snapshotExcluded != 0 {
		e := types.Uint64(excluded)
		out.ExcludedActiveStake = &e
	}
	out.TotalPoolCount, out.TotalDelegators = c.u64(), c.u64()
	out.CapturedSlot, out.BoundarySlot = c.u64(), c.u64()
	out.ProtocolVersion = uint(
		c.u64(),
	) //nolint:gosec // a protocol version fits
	out.CalculationVersion = uint(c.u64())
	out.Authoritative = f&snapshotAuthoritative != 0
	nonce := c.bytes()
	if f&snapshotNonce != 0 {
		out.EpochNonce = nonce
	}
	return out, c.err()
}

func encodeRewardPots(p *models.RewardAdaPots) []byte {
	v := []byte{flag(p.ImportedEpochFees != nil, 1)}
	var imported uint64
	if p.ImportedEpochFees != nil {
		imported = uint64(*p.ImportedEpochFees)
	}
	for _, n := range []uint64{
		uint64(p.Treasury), uint64(p.Reserves), uint64(p.Fees),
		uint64(p.Rewards), p.CapturedSlot, imported,
	} {
		v = appendU64(v, n)
	}
	return v
}

func decodeRewardPots(k, v []byte) (*models.RewardAdaPots, error) {
	epoch, err := badgerutxo.DecodeRewardPotsKey(k)
	if err != nil {
		return nil, err
	}
	c := rewardCursor{b: v}
	f := c.u8()
	out := &models.RewardAdaPots{Epoch: epoch}
	out.Treasury = types.Uint64(c.u64())
	out.Reserves = types.Uint64(c.u64())
	out.Fees = types.Uint64(c.u64())
	out.Rewards = types.Uint64(c.u64())
	out.CapturedSlot = c.u64()
	imported := c.u64()
	if f&1 != 0 {
		i := types.Uint64(imported)
		out.ImportedEpochFees = &i
	}
	return out, c.err()
}

func encodeRewardFailure(slot uint64, reason string) []byte {
	return append(appendU64(nil, slot), reason...)
}

func decodeRewardFailure(v []byte) (string, uint64, error) {
	if len(v) < 8 {
		return "", 0, errRewardInputRow
	}
	return string(v[8:]), binary.BigEndian.Uint64(v), nil
}

// rewardInputRows turns a commit's reward input changes into row puts. A
// change reads the rows the store holds as the changes before it left them,
// which the commit's own earlier puts are not yet in.
type rewardInputRows struct {
	s    *Set
	rows []badgerutxo.RowPut
	// written holds the rows an earlier change of the commit put, by key,
	// nil for a deleted row.
	written map[string][]byte
	// bulk marks the epochs whose stake rows are written under a tombstone.
	bulk map[uint64]bool
	// stakes counts the stake rows an earlier change of the commit put or
	// wiped, by epoch: -1 once wiped.
	wiped map[uint64]bool
}

func (b *rewardInputRows) get(k []byte) ([]byte, bool, error) {
	if v, ok := b.written[string(k)]; ok {
		return v, v != nil, nil
	}
	return b.s.store.Row(k)
}

func (b *rewardInputRows) put(slot uint64, k, v []byte) {
	b.rows = append(b.rows, badgerutxo.RowPut{Slot: slot, Key: k, Value: v})
	b.written[string(k)] = v
}

func (b *rewardInputRows) del(slot uint64, k []byte) {
	b.rows = append(
		b.rows, badgerutxo.RowPut{Slot: slot, Key: k, Delete: true},
	)
	b.written[string(k)] = nil
}

// keys lists the keys under prefix as the commit's earlier changes left
// them.
func (b *rewardInputRows) keys(prefix []byte) ([][]byte, error) {
	set := map[string]struct{}{}
	err := b.s.store.Rows(prefix, func(k, _ []byte) error {
		set[string(k)] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for k, v := range b.written {
		if !bytes.HasPrefix([]byte(k), prefix) {
			continue
		}
		if v == nil {
			delete(set, k)
		} else {
			set[k] = struct{}{}
		}
	}
	out := make([][]byte, 0, len(set))
	for k := range set {
		out = append(out, []byte(k))
	}
	slices.SortFunc(out, bytes.Compare)
	return out, nil
}

var errStop = errors.New("stop")

// hasStakeRows reports whether the stake rows of epoch exist after the
// commit's earlier changes.
func (b *rewardInputRows) hasStakeRows(epoch uint64) (bool, error) {
	if b.bulk[epoch] {
		return true, nil
	}
	if b.wiped[epoch] {
		return false, nil
	}
	found := false
	err := b.s.store.Rows(
		badgerutxo.RewardStakeInputEpochPrefix(epoch),
		func(_, _ []byte) error {
			found = true
			return errStop
		},
	)
	if err != nil && !errors.Is(err, errStop) {
		return false, err
	}
	return found, nil
}

func (b *rewardInputRows) apply(c ledger.RewardInputChange) error {
	switch c.Kind {
	case ledger.RewardInputPots:
		b.put(
			c.Pots.CapturedSlot, badgerutxo.RewardPotsKey(c.Pots.Epoch),
			encodeRewardPots(c.Pots),
		)
	case ledger.RewardInputSnapshot:
		b.put(
			ledger.RewardInputSlot(
				c.Snapshot.CapturedSlot,
				c.Snapshot.BoundarySlot,
			),
			badgerutxo.RewardSnapshotKey(
				c.Snapshot.Epoch, badgerutxo.RewardSnapshotHeader,
				c.Snapshot.SnapshotType,
			),
			encodeRewardSnapshot(c.Snapshot),
		)
	case ledger.RewardInputPools:
		for _, p := range c.Pools {
			k, err := badgerutxo.RewardPoolInputKey(p.Epoch, p.PoolKeyHash)
			if err != nil {
				return err
			}
			b.put(
				ledger.RewardInputSlot(p.CapturedSlot, p.BoundarySlot),
				k,
				encodeRewardPool(p),
			)
		}
	case ledger.RewardInputReplace:
		keys, err := b.keys(badgerutxo.RewardPoolInputEpochPrefix(c.Epoch))
		if err != nil {
			return err
		}
		for _, k := range keys {
			v, _, err := b.get(k)
			if err != nil {
				return err
			}
			p, err := decodeRewardPool(k, v)
			if err != nil {
				return err
			}
			b.del(ledger.RewardInputSlot(p.CapturedSlot, p.BoundarySlot), k)
		}
		b.rows = append(b.rows, badgerutxo.RowPut{
			Key:        badgerutxo.RewardStakeInputEpochPrefix(c.Epoch),
			WipePrefix: true,
		})
		b.wiped[c.Epoch] = true
		delete(b.bulk, c.Epoch)
	case ledger.RewardInputStakes:
		return b.stakes(c)
	case ledger.RewardInputPrune:
		return b.prune(c.PruneBelow)
	default:
		return fmt.Errorf("utxoset: unknown reward input change %d", c.Kind)
	}
	return nil
}

func (b *rewardInputRows) stakes(c ledger.RewardInputChange) error {
	for _, s := range c.Stakes {
		slot := ledger.RewardInputSlot(s.CapturedSlot, s.BoundarySlot)
		k, err := badgerutxo.RewardStakeInputKey(
			s.Epoch, s.PoolKeyHash, s.CredentialTag, s.StakingKey,
		)
		if err != nil {
			return err
		}
		if !b.bulk[s.Epoch] {
			has, err := b.hasStakeRows(s.Epoch)
			if err != nil {
				return err
			}
			if !has {
				b.rows = append(b.rows, badgerutxo.RowPut{
					Slot: slot,
					Key: badgerutxo.RewardStakeInputEpochPrefix(
						s.Epoch,
					),
					TombstonePrefix: true,
				})
				b.bulk[s.Epoch] = true
			}
		}
		// Rows an earlier commit left under the epoch have no tombstone
		// of this one, so a put over them keeps its own journal entry.
		b.rows = append(b.rows, badgerutxo.RowPut{
			Slot: slot, Key: k, Value: encodeRewardStake(s),
			SkipJournal: b.bulk[s.Epoch],
		})
	}
	return nil
}

// prune drops the stake rows of every epoch below below, newest first,
// without journal entries: a rollback does not bring them back. The pool and
// snapshot rows stay, as the metadata store keeps them, so the stake rows of
// an epoch can be rebuilt from them.
func (b *rewardInputRows) prune(below uint64) error {
	if below == 0 {
		return nil
	}
	for _, tag := range []byte{badgerutxo.RewardStakeInputTag} {
		limit := append(
			binary.BigEndian.AppendUint64([]byte{tag}, below-1),
			bytes.Repeat([]byte{0xff}, 80)...,
		)
		for {
			k, ok, err := b.s.store.LastKeyAtOrBelow([]byte{tag}, limit)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			epoch := binary.BigEndian.Uint64(k[1:9])
			prefix := binary.BigEndian.AppendUint64([]byte{tag}, epoch)
			b.rows = append(
				b.rows,
				badgerutxo.RowPut{Key: prefix, WipePrefix: true},
			)
			if tag == badgerutxo.RewardStakeInputTag {
				b.wiped[epoch] = true
				delete(b.bulk, epoch)
			}
			if epoch == 0 {
				break
			}
			limit = append(
				binary.BigEndian.AppendUint64([]byte{tag}, epoch-1),
				bytes.Repeat([]byte{0xff}, 80)...,
			)
		}
	}
	return nil
}

// rewardInputPuts converts the commit's reward input changes.
func (s *Set) rewardInputPuts(
	lc ledger.LedgerChanges,
) ([]badgerutxo.RowPut, error) {
	if len(lc.RewardInputs) == 0 {
		return nil, nil
	}
	b := &rewardInputRows{
		s: s, written: map[string][]byte{},
		bulk: map[uint64]bool{}, wiped: map[uint64]bool{},
	}
	for _, c := range lc.RewardInputs {
		if err := b.apply(c); err != nil {
			return nil, err
		}
	}
	return b.rows, nil
}

// ensureRewardInputs loads the reward snapshot inputs the reward round can
// read from the metadata store into a set from before it kept them: every
// epoch's pots, snapshot, pool inputs and imported basis failure, and the
// stake inputs of the newest epochs.
func (s *Set) ensureRewardInputs(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.RewardInputsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the reward input marker: %w", err)
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
	var stakes int
	err = s.store.LoadRewardInputs(func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error {
		for _, e := range epochs {
			pots, err := ldb.GetRewardAdaPots(e.EpochId, txn)
			if err != nil {
				return err
			}
			if pots != nil {
				err = put(
					badgerutxo.RewardPotsKey(e.EpochId),
					encodeRewardPots(pots),
				)
				if err != nil {
					return err
				}
				err = tombstone(
					pots.CapturedSlot, badgerutxo.RewardPotsKey(e.EpochId),
				)
				if err != nil {
					return err
				}
			}
			n, err := s.loadRewardEpoch(
				ldb, txn, e.EpochId,
				s.retainRewards || e.EpochId+uint64(ledger.RewardInputEpochWindow) >= newest,
				put, tombstone,
			)
			if err != nil {
				return fmt.Errorf("epoch %d: %w", e.EpochId, err)
			}
			stakes += n
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load reward inputs: %w", err)
	}
	s.logger.Info(
		"loaded reward inputs into the live UTxO set",
		"epochs", len(epochs), "stake_rows", stakes,
	)
	return nil
}

// rewardSeedFailure reads the imported basis failure with the slot it was
// captured at. A LedgerDB that cannot report the slot falls back to the
// epoch's snapshot slot, or 0.
func rewardSeedFailure(
	ldb ledgerdb.LedgerDB,
	epoch uint64,
	snap *models.RewardSnapshot,
	txn ledgerdb.Txn,
) (string, uint64, error) {
	if r, ok := ldb.(ledgerdb.RewardSeedFailureSlotReader); ok {
		return r.GetRewardSeedFailureSlot(epoch, rewardSnapshotMark, txn)
	}
	reason, err := ldb.GetRewardSeedFailure(epoch, rewardSnapshotMark, txn)
	if err != nil || reason == "" {
		return "", 0, err
	}
	var slot uint64
	if snap != nil {
		slot = snap.CapturedSlot
	}
	return reason, slot, nil
}

func (s *Set) loadRewardEpoch(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	epoch uint64,
	withStakes bool,
	put func(k, v []byte) error,
	tombstone func(slot uint64, prefix []byte) error,
) (int, error) {
	snap, err := ldb.GetRewardSnapshot(epoch, rewardSnapshotMark, txn)
	if err != nil {
		return 0, err
	}
	if snap != nil {
		k := badgerutxo.RewardSnapshotKey(
			epoch, badgerutxo.RewardSnapshotHeader, rewardSnapshotMark,
		)
		if err := put(k, encodeRewardSnapshot(snap)); err != nil {
			return 0, err
		}
		err = tombstone(
			ledger.RewardInputSlot(snap.CapturedSlot, snap.BoundarySlot), k,
		)
		if err != nil {
			return 0, err
		}
	}
	reason, slot, err := rewardSeedFailure(ldb, epoch, snap, txn)
	if err != nil {
		return 0, err
	}
	if reason != "" {
		k := badgerutxo.RewardSnapshotKey(
			epoch, badgerutxo.RewardSnapshotFailure, rewardSnapshotMark,
		)
		if err := put(k, encodeRewardFailure(slot, reason)); err != nil {
			return 0, err
		}
		if err := tombstone(slot, k); err != nil {
			return 0, err
		}
	}
	pools, err := ldb.GetRewardPoolInputs(epoch, txn)
	if err != nil {
		return 0, err
	}
	poolSlot := uint64(math.MaxUint64)
	for _, p := range pools {
		k, err := badgerutxo.RewardPoolInputKey(epoch, p.PoolKeyHash)
		if err != nil {
			return 0, err
		}
		if err := put(k, encodeRewardPool(p)); err != nil {
			return 0, err
		}
		poolSlot = min(
			poolSlot, ledger.RewardInputSlot(p.CapturedSlot, p.BoundarySlot),
		)
	}
	if len(pools) > 0 {
		err := tombstone(poolSlot, badgerutxo.RewardPoolInputEpochPrefix(epoch))
		if err != nil {
			return 0, err
		}
	}
	if !withStakes {
		return 0, nil
	}
	stakes, err := ldb.GetRewardStakeInputs(epoch, txn)
	if err != nil {
		return 0, err
	}
	stakeSlot := uint64(math.MaxUint64)
	for _, st := range stakes {
		k, err := badgerutxo.RewardStakeInputKey(
			epoch, st.PoolKeyHash, st.CredentialTag, st.StakingKey,
		)
		if err != nil {
			return 0, err
		}
		if err := put(k, encodeRewardStake(st)); err != nil {
			return 0, err
		}
		stakeSlot = min(
			stakeSlot, ledger.RewardInputSlot(st.CapturedSlot, st.BoundarySlot),
		)
	}
	if len(stakes) > 0 {
		err := tombstone(
			stakeSlot, badgerutxo.RewardStakeInputEpochPrefix(epoch),
		)
		if err != nil {
			return 0, err
		}
	}
	return len(stakes), nil
}

// RewardInputsLoaded reports whether the set holds the reward snapshot inputs
// of every epoch reward calculation can read.
func (s *Set) RewardInputsLoaded() (bool, error) {
	return s.store.RewardInputsLoaded()
}

func (s *Set) rewardRow(k []byte, asOf *uint64) ([]byte, bool, error) {
	if asOf == nil {
		return s.store.Row(k)
	}
	return s.store.RowAt(k, *asOf)
}

func (s *Set) rewardRows(
	prefix []byte,
	asOf *uint64,
	fn func(k, v []byte) error,
) error {
	return s.rewardRowsBetween(prefix, nil, nil, asOf, fn)
}

// rewardRowsBetween is rewardRows for the keys from up to, not including, to.
func (s *Set) rewardRowsBetween(
	prefix, from, to []byte,
	asOf *uint64,
	fn func(k, v []byte) error,
) error {
	if asOf == nil {
		return s.store.RowsBetween(prefix, from, to, fn)
	}
	return s.store.RowsAtBetween(prefix, from, to, *asOf, fn)
}

// RewardPots returns the epoch's ADA pots, nil when the set holds none.
func (s *Set) RewardPots(
	epoch uint64,
	asOf *uint64,
) (*models.RewardAdaPots, error) {
	k := badgerutxo.RewardPotsKey(epoch)
	v, ok, err := s.rewardRow(k, asOf)
	if err != nil || !ok {
		return nil, err
	}
	return decodeRewardPots(k, v)
}

// RewardSnapshotRow returns the snapshot header, nil when the set holds none.
func (s *Set) RewardSnapshotRow(
	epoch uint64, typ string, asOf *uint64,
) (*models.RewardSnapshot, error) {
	k := badgerutxo.RewardSnapshotKey(
		epoch,
		badgerutxo.RewardSnapshotHeader,
		typ,
	)
	v, ok, err := s.rewardRow(k, asOf)
	if err != nil || !ok {
		return nil, err
	}
	return decodeRewardSnapshot(k, v)
}

// RewardFailureRow returns the imported basis failure and the slot it was
// captured at; the reason is empty when the set holds none.
func (s *Set) RewardFailureRow(
	epoch uint64, typ string, asOf *uint64,
) (string, uint64, error) {
	k := badgerutxo.RewardSnapshotKey(
		epoch,
		badgerutxo.RewardSnapshotFailure,
		typ,
	)
	v, ok, err := s.rewardRow(k, asOf)
	if err != nil || !ok {
		return "", 0, err
	}
	return decodeRewardFailure(v)
}

// RewardPoolRows returns the epoch's pool inputs ordered by pool.
func (s *Set) RewardPoolRows(
	epoch uint64, asOf *uint64,
) ([]*models.RewardPoolInput, error) {
	var out []*models.RewardPoolInput
	err := s.rewardRows(
		badgerutxo.RewardPoolInputEpochPrefix(epoch), asOf,
		func(k, v []byte) error {
			p, err := decodeRewardPool(k, v)
			if err != nil {
				return err
			}
			out = append(out, p)
			return nil
		},
	)
	return out, err
}

// RewardStakeRows returns the epoch's stake inputs ordered by pool,
// credential tag and credential, those of pools lo through hi when both are
// set.
func (s *Set) RewardStakeRows(
	epoch uint64, lo, hi []byte, asOf *uint64,
) ([]*models.RewardStakeInput, error) {
	var out []*models.RewardStakeInput
	prefix := badgerutxo.RewardStakeInputEpochPrefix(epoch)
	// Keys lead with the pool, so the pool range is a key range: a mainnet
	// epoch holds over a million stake rows, which a range of pools must not
	// read.
	var from, to []byte
	if lo != nil {
		from = append(bytes.Clone(prefix), lo...)
	}
	if hi != nil {
		to = badgerutxo.KeyAfter(append(bytes.Clone(prefix), hi...))
	}
	err := s.rewardRowsBetween(
		prefix, from, to, asOf,
		func(k, v []byte) error {
			st, err := decodeRewardStake(k, v)
			if err != nil {
				return err
			}
			if (lo != nil && bytes.Compare(st.PoolKeyHash, lo) < 0) ||
				(hi != nil && bytes.Compare(st.PoolKeyHash, hi) > 0) {
				return nil
			}
			out = append(out, st)
			return nil
		},
	)
	return out, err
}

// RewardStakeSlotBounds returns the slots the epoch's first and last stake
// rows, in key order, were captured at, and false when the epoch holds none.
// It reads those two rows only.
func (s *Set) RewardStakeSlotBounds(
	epoch uint64,
) (first, last uint64, found bool, err error) {
	fk, fv, lk, lv, found, err := s.store.EdgeRows(
		badgerutxo.RewardStakeInputEpochPrefix(epoch),
	)
	if err != nil || !found {
		return 0, 0, false, err
	}
	f, err := decodeRewardStake(fk, fv)
	if err != nil {
		return 0, 0, false, err
	}
	l, err := decodeRewardStake(lk, lv)
	if err != nil {
		return 0, 0, false, err
	}
	return f.CapturedSlot, l.CapturedSlot, true, nil
}

// RewardInputEpochs lists the epochs the set holds a snapshot, pool or stake
// row of, and the epochs it holds pots of, oldest first.
func (s *Set) RewardInputEpochs() (snapshots, pots []uint64, err error) {
	seen := map[uint64]struct{}{}
	for _, tag := range []byte{
		badgerutxo.RewardStakeInputTag, badgerutxo.RewardPoolInputTag,
		badgerutxo.RewardSnapshotTag,
	} {
		err = s.store.EachDistinctEpoch(tag, func(e uint64) error {
			seen[e] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	for e := range seen {
		snapshots = append(snapshots, e)
	}
	slices.Sort(snapshots)
	err = s.store.Rows(
		[]byte{badgerutxo.RewardPotsTag},
		func(k, _ []byte) error {
			e, err := badgerutxo.DecodeRewardPotsKey(k)
			pots = append(pots, e)
			return err
		},
	)
	return snapshots, pots, err
}

// StaleRewardSnapshotEpochs lists, oldest first, the epochs whose mark
// snapshot header carries a calculation version other than version.
func (s *Set) StaleRewardSnapshotEpochs(version uint) ([]uint64, error) {
	snapshots, _, err := s.RewardInputEpochs()
	if err != nil {
		return nil, err
	}
	var stale []uint64
	for _, epoch := range snapshots {
		snap, err := s.RewardSnapshotRow(epoch, rewardSnapshotMark, nil)
		if err != nil {
			return nil, err
		}
		if snap != nil && snap.CalculationVersion != version {
			stale = append(stale, epoch)
		}
	}
	return stale, nil
}

var _ ledger.RewardInputsReader = (*Set)(nil)
