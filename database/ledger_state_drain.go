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

package database

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrCannotDrain is returned when the metadata store cannot delete rows in
// chunks.
var ErrCannotDrain = errors.New(
	"metadata store cannot drain ledger-state rows",
)

// drainedMask caches the persisted latch for FamilyDrained, so the write and
// rollback paths do not read the gate table per call.
type drainedMask struct {
	v atomic.Uint64
}

// DrainedFamilies returns the latch: the families whose drain finished. A
// family whose drain began but did not finish is not in it; use
// EffectiveDrainedFamilies for the families whose rows can no longer be
// trusted. It refreshes the mask FamilyDrained reports, which includes
// families with a drain marker.
func (d *Database) DrainedFamilies(
	ctx context.Context,
) (nodesettings.FamilyMask, error) {
	latch, begun, err := d.readDrainState(ctx)
	if err != nil {
		return 0, err
	}
	d.publishDrained(latch | begun)
	return latch, nil
}

// EffectiveDrainedFamilies returns the latch plus every family with a drain
// marker. A marker is written before the first delete and cleared after the
// latch bit, so a family with one has lost, or may have lost, SQL rows and
// every SQL read or write of it is wrong. Only the drain loop, which resumes
// the deletes, reads the bare latch.
func (d *Database) EffectiveDrainedFamilies(
	ctx context.Context,
) (nodesettings.FamilyMask, error) {
	latch, begun, err := d.readDrainState(ctx)
	if err != nil {
		return 0, err
	}
	d.publishDrained(latch | begun)
	return latch | begun, nil
}

func (d *Database) readDrainState(
	ctx context.Context,
) (latch, begun nodesettings.FamilyMask, err error) {
	gates, err := d.Metadata().GetNodeSettingsGates(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("read ledger-state latch: %w", err)
	}
	latch, err = nodesettings.DecodeFamilyMask(
		gates[nodesettings.LedgerStateDrainedGateName],
	)
	if err != nil {
		return 0, 0, err
	}
	keys, err := d.ListSyncStateKeysByPrefix(statedrain.MarkerPrefix, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("read drain markers: %w", err)
	}
	begun, err = statedrain.BegunFamilies(keys)
	if err != nil {
		return 0, 0, err
	}
	return latch, begun, nil
}

// liveStakeDrainSetter is the metadata store hook that stops the maintenance
// of reward_live_stake.
type liveStakeDrainSetter interface {
	SetRewardLiveStakeDrained(bool)
}

// publishDrained caches mask and tells the metadata store which of its tables
// it must no longer maintain.
func (d *Database) publishDrained(mask nodesettings.FamilyMask) {
	d.drained.v.Store(uint64(mask))
	if aware, ok := d.Metadata().(metadata.DrainedFamilyAware); ok {
		aware.SetDrainedFamilies(mask)
	}
	if setter, ok := d.Metadata().(liveStakeDrainSetter); ok {
		setter.SetRewardLiveStakeDrained(
			mask.Has(nodesettings.FamilyLiveStake),
		)
	}
}

// EachRewardLiveStake calls fn with every reward_live_stake row. Only the
// drain's comparison reads the table this way, before the family is drained.
func (d *Database) EachRewardLiveStake(
	txn *Txn,
	fn func(*models.RewardLiveStake) error,
) error {
	each, ok := d.Metadata().(interface {
		EachRewardLiveStake(types.Txn, func(*models.RewardLiveStake) error) error
	})
	if !ok {
		return errors.New("the metadata store cannot list live stake rows")
	}
	if txn == nil {
		return each.EachRewardLiveStake(nil, fn)
	}
	return each.EachRewardLiveStake(txn.Metadata(), fn)
}

// FamilyDrained reports whether the SQL rows of family are deleted or being
// deleted, from the latch and drain markers as DrainedFamilies and
// RecordDrainedFamily last saw them. No SQL path
// may read or write a drained family's tables.
func (d *Database) FamilyDrained(family nodesettings.Family) bool {
	return nodesettings.FamilyMask(d.drained.v.Load()).Has(family)
}

// DrainedMask returns the mask FamilyDrained reports, without reading the metadata store.
func (d *Database) DrainedMask() nodesettings.FamilyMask {
	return nodesettings.FamilyMask(d.drained.v.Load())
}

// RecordDrainedFamily adds family to the drained set. The latch only widens:
// it re-reads the stored mask, so a stale caller cannot clear another
// family's bit. It must be called only after the family's rows are deleted.
func (d *Database) RecordDrainedFamily(
	ctx context.Context,
	family nodesettings.Family,
) error {
	current, err := d.DrainedFamilies(ctx)
	if err != nil {
		return err
	}
	next, err := nodesettings.WidenFamilyMask(current, current.With(family))
	if err != nil {
		return err
	}
	if next == current {
		return nil
	}
	epoch, slot := d.currentEpochSlot(ctx)
	if err := d.Metadata().SetNodeSettingsGates(
		ctx,
		nodesettings.Values{
			nodesettings.LedgerStateDrainedGateName: nodesettings.
				EncodeFamilyMask(next),
		},
		epoch,
		slot,
	); err != nil {
		return fmt.Errorf("record drained family %s: %w", family, err)
	}
	d.publishDrained(next | d.DrainedMask())
	return nil
}

// DeleteLedgerStateRows deletes up to limit rows of table in ascending order
// of keyColumn and returns how many it deleted; fewer than limit means the
// table is empty.
func (d *Database) DeleteLedgerStateRows(
	ctx context.Context,
	table string,
	keyColumn string,
	limit int,
) (int64, error) {
	drainer, ok := d.Metadata().(metadata.LedgerStateRowDrainer)
	if !ok {
		return 0, ErrCannotDrain
	}
	return drainer.DeleteRowsChunk(ctx, table, keyColumn, limit)
}

// NonceSource answers the block nonce reads a rollback needs once the nonces
// family is drained and the metadata store holds none.
type NonceSource interface {
	// BlockNonceAt returns the nonce of the block at point, nil when none.
	BlockNonceAt(point ocommon.Point) ([]byte, error)
	// HasCheckpointAtOrBefore reports whether a valid checkpoint nonce exists
	// at or before slot.
	HasCheckpointAtOrBefore(slot uint64) (bool, error)
}

// ErrNoNonceSource is returned by a rollback of a drained nonces family when
// no NonceSource was installed.
var ErrNoNonceSource = errors.New(
	"block nonces are drained from the metadata store and no source is installed",
)

// SetNonceSource installs the source a rollback reads nonces from once the
// nonces family is drained.
func (d *Database) SetNonceSource(src NonceSource) {
	d.nonceSource.Store(&src)
}

func (d *Database) nonceSourceOrErr() (NonceSource, error) {
	p := d.nonceSource.Load()
	if p == nil || *p == nil {
		return nil, ErrNoNonceSource
	}
	return *p, nil
}

func (d *Database) truncateTargetNonce(
	point ocommon.Point,
	txn *Txn,
) ([]byte, error) {
	if !d.FamilyDrained(nodesettings.FamilyNonces) {
		return d.GetBlockNonce(point, txn)
	}
	src, err := d.nonceSourceOrErr()
	if err != nil {
		return nil, err
	}
	return src.BlockNonceAt(point)
}

func (d *Database) truncateHasNonceCheckpoint(
	slot uint64,
	txn *Txn,
) (bool, error) {
	if !d.FamilyDrained(nodesettings.FamilyNonces) {
		return d.hasBlockNonceCheckpointAtOrBeforeSlot(slot, txn)
	}
	src, err := d.nonceSourceOrErr()
	if err != nil {
		return false, err
	}
	return src.HasCheckpointAtOrBefore(slot)
}

// deleteBlockNoncesAfterPointUnlessDrained is the rollback's nonce delete; a
// drained family has no rows, and the set drops its own nonces in the same
// rollback.
func (d *Database) deleteBlockNoncesAfterPointUnlessDrained(
	point ocommon.Point,
	txn *Txn,
) error {
	if d.FamilyDrained(nodesettings.FamilyNonces) {
		return nil
	}
	return d.DeleteBlockNoncesAfterPoint(point, txn)
}

// ErrLedgerStateDrained reports an operation that would rewrite the SQL
// ledger state of a database that has begun or finished draining a family.
var ErrLedgerStateDrained = errors.New(
	"this database has begun or finished draining ledger-state families, " +
		"so its SQL ledger state cannot be rewritten by an import that has " +
		"no live UTxO set to load them into",
)

// RefuseLedgerStateRewrite returns an error wrapping ErrLedgerStateDrained
// when any latch bit or drain marker is present. The API backfill
// rewrites the SQL ledger state: a drained family's rows would
// reappear beside a set that no longer matches them, and a marker cleared
// mid-drain would let the gate run again over partly deleted rows.
func (d *Database) RefuseLedgerStateRewrite(ctx context.Context) error {
	mask, err := d.DrainedFamilies(ctx)
	if err != nil {
		return err
	}
	if mask != 0 {
		return fmt.Errorf(
			"%w (drained families: %v)", ErrLedgerStateDrained, mask.Families(),
		)
	}
	markers, err := d.ListSyncStateKeysByPrefix(statedrain.MarkerPrefix, nil)
	if err != nil {
		return err
	}
	if len(markers) > 0 {
		return fmt.Errorf(
			"%w (drain begun: %v)", ErrLedgerStateDrained, markers,
		)
	}
	return nil
}
