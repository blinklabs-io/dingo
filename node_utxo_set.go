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

package dingo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// openUtxoSet opens the live UTxO set, which holds the UTxO set and the ledger
// state. An in-memory node uses a temporary directory. It stays open across a live
// restore or truncate, which reconcile it again.
func (n *Node) openUtxoSet() error {
	if n.utxoSet != nil {
		return nil
	}
	cfg := n.utxoSetConfig()
	cfg.Registerer = n.config.promRegistry
	set, tempDir, err := nodestorage.OpenUtxoSet(cfg)
	if err != nil {
		return err
	}
	n.utxoSetTempDir = tempDir
	n.utxoSet = set
	n.anchoredUtxoSet.Store(set)
	n.installLedgerReimport(set)
	n.db.RegisterDiskSizeSource("utxoset_checkpoint", set.CheckpointBytes)
	set.SetCheckpointBarrier(func(ctx context.Context) (func(), error) {
		return n.db.PauseCommitsContext(ctx)
	})
	return nil
}

func (n *Node) utxoSetConfig() nodestorage.UtxoSetConfig {
	return nodestorage.UtxoSetConfig{
		DataDir:           n.config.dataDir,
		Logger:            n.config.logger,
		CardanoNodeConfig: n.config.cardanoNodeConfig,
		CheckpointKeep:    n.config.ledgerUtxoCheckpointKeep,
	}
}

// migrateReleasedLedger moves a v0.94.2 database's ledger state out of its
// metadata store before the ledger's database opens over the resolved stores.
func (n *Node) migrateReleasedLedger(
	ctx context.Context,
	stores database.Stores,
	deps internalplugins.StorageDependencies,
) error {
	m := n.releasedMigration()
	m.Blob = stores.Blob
	m.Immutable = stores.Immutable
	m.OpenMetadata = internalplugins.ReleasedMetadataOpener(
		n.storageSelections(), deps,
	)
	_, err := m.Run(ctx)
	return err
}

func (n *Node) releasedMigration() nodestorage.ReleasedMigration {
	return nodestorage.ReleasedMigration{
		Config:                     n.databaseConfig(),
		UtxoSet:                    n.utxoSetConfig(),
		Set:                        n.utxoSet,
		Logger:                     n.config.logger,
		RewindBlocks:               n.shelleySecurityParam(),
		DelegatorInactivityEnabled: n.config.delegatorInactivityEnabled,
		DelegatorInactivity:        n.config.delegatorInactivity,
	}
}

// reconcileUtxoSet makes the live UTxO set equal to the ledger's live set,
// rolling the ledger back to the set's tip when the set trails it. It runs
// after database recovery and
// before the ledger starts applying blocks.
func (n *Node) reconcileUtxoSet(ctx context.Context) error {
	r := nodestorage.Reconcile{
		DB:                n.db,
		Set:               n.utxoSet,
		Logger:            n.config.logger,
		CardanoNodeConfig: n.config.cardanoNodeConfig,
		FinishImport:      n.finishPendingImport,
		Resume:            n.ledgerState.ResumeUtxoSetFrom,
		Reconciled: func(ctx context.Context) error {
			n.utxoSetReconciled = true
			return n.checkStakeSnapshotProvenance()
		},
	}
	return r.Run(ctx)
}

// utxoSetBackuper returns the live UTxO set for a snapshot, nil when the node
// keeps none.
func (n *Node) utxoSetBackuper() lifecycle.UtxoSetBackuper {
	if n.utxoSet == nil {
		return nil
	}
	return n.utxoSet
}

// defaultDurableCloseProgressInterval is how often a store close that is
// still running is logged; it matches plugin.Host's durable-close interval.
const defaultDurableCloseProgressInterval = 10 * time.Second

// closeUtxoSet closes the live UTxO set. The ledger must have stopped
// applying blocks. The set is a Badger store whose close syncs every table
// file, so, like a plugin.Descriptor.DurableClose store, the close and the
// checkpoint join before it run to completion with no deadline, logging
// progress while they run.
func (n *Node) closeUtxoSet() error {
	n.utxoSetReconciled = false
	if n.utxoSet == nil {
		return nil
	}
	set := n.utxoSet
	err := n.closeDurable("live UTxO set", func() error {
		n.stopUtxoSetCheckpoint()
		return set.Close()
	})
	n.utxoSet = nil
	if n.utxoSetTempDir != "" {
		if removeErr := os.RemoveAll(n.utxoSetTempDir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove temporary storage directory: %w", removeErr))
		}
		n.utxoSetTempDir = ""
	}
	return err
}

// closeDurable runs closeFn to completion on the calling goroutine and logs
// its start, its progress every durableCloseProgress, and its result.
func (n *Node) closeDurable(resource string, closeFn func() error) error {
	interval := n.durableCloseProgress
	if interval <= 0 {
		interval = defaultDurableCloseProgressInterval
	}
	logger := n.config.logger.With("resource", resource)
	start := time.Now()
	logger.Info("closing durable store")
	finished := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-finished:
				return
			case <-ticker.C:
				logger.Warn(
					"durable store close still in progress",
					"elapsed", time.Since(start).Round(time.Millisecond),
				)
			}
		}
	}()
	err := closeFn()
	close(finished)
	<-stopped
	elapsed := time.Since(start).Round(time.Millisecond)
	if err != nil {
		logger.Error(
			"durable store close failed",
			"elapsed",
			elapsed,
			"error",
			err,
		)
		return err
	}
	logger.Info("durable store closed", "elapsed", elapsed)
	return nil
}

const utxoSetAnchorInterval = nodestorage.AnchorInterval

// startUtxoSetAnchorUpdater keeps the live UTxO set's anchor at the ledger's
// immutable point until the node's context ends. ImmutablePoint takes the
// ledger's lock and the chain's, which the ledger's commit path already holds
// while it applies the set, so the set never asks for it: this goroutine does,
// and only hands the set the result. It reads the published ledger on each
// pass, and the open set, so it follows a live rebuild of either.
func (n *Node) startUtxoSetAnchorUpdater() {
	if n.utxoSet == nil {
		return
	}
	ctx := n.ctx
	update := func() {
		ls := n.wallClockLedger.Load()
		set := n.anchoredUtxoSet.Load()
		if ls == nil || set == nil {
			return
		}
		nodestorage.UpdateAnchor(ctx, ls, set)
	}
	checkpoint := n.utxoSetCheckpointer()
	go func() {
		update()
		ticker := time.NewTicker(utxoSetAnchorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				update()
				checkpoint(ctx)
			}
		}
	}()
}

// utxoSetCheckpointNearTipSlots is how far behind the wall clock the ledger may
// be, in slots, for a checkpoint to be written. A node replaying history would
// otherwise copy the whole set every epoch for the length of the sync, and a
// checkpoint far below the tip is the one recovery can least use.
const utxoSetCheckpointNearTipSlots = 43200

// checkpointPolicy decides when a checkpoint of the live UTxO set is written:
// only while the ledger is near the wall-clock tip, at once when none exists,
// then every `every` epochs counted from the last one written.
type checkpointPolicy struct {
	every   uint64
	last    uint64
	started bool
}

// due reports whether a checkpoint is to be written at epoch, given whether one
// is on disk and how many slots the ledger is behind the wall clock. A policy
// that is not due changes nothing, so a node still catching up counts no epoch.
func (p *checkpointPolicy) due(
	epoch uint64,
	haveCheckpoint bool,
	slotsBehind uint64,
) bool {
	if slotsBehind > utxoSetCheckpointNearTipSlots {
		return false
	}
	if !p.started {
		p.started = true
		p.last = epoch
		// A checkpoint from an earlier run counts from now.
		return !haveCheckpoint
	}
	if epoch < p.last+p.every {
		return false
	}
	p.last = epoch
	return true
}

// utxoSetCheckpointer returns the function that writes a checkpoint of the live
// UTxO set when its policy says so. The copy runs on its own goroutine, one at
// a time, so the anchor updater that calls this is never held for the length of
// a copy. A failure is logged and retried at the next due epoch: the
// checkpoint only widens what recovery can use.
func (n *Node) utxoSetCheckpointer() func(context.Context) {
	every := n.config.ledgerUtxoCheckpointEpochs
	if every <= 0 {
		return func(context.Context) {}
	}
	policy := checkpointPolicy{
		every: uint64(every),
	} // #nosec G115 -- validated non-negative
	var writing atomic.Bool
	return func(ctx context.Context) {
		ls := n.wallClockLedger.Load()
		set := n.anchoredUtxoSet.Load()
		if ls == nil || set == nil || writing.Load() {
			return
		}
		cps, err := set.Checkpoints()
		if err != nil {
			cps = nil
		}
		if !policy.due(ls.CurrentEpoch(), len(cps) > 0, ls.SlotsBehindHead()) {
			return
		}
		if !writing.CompareAndSwap(false, true) {
			return
		}
		if !n.launchUtxoSetCheckpoint(
			ctx,
			set,
			func() { writing.Store(false) },
		) {
			writing.Store(false)
		}
	}
}

// launchUtxoSetCheckpoint writes a checkpoint of set on a goroutine the node
// owns: stopUtxoSetCheckpoint cancels it and waits for it, so the set is never
// closed under a copy. It reports false, starting nothing, once the set is no
// longer the one the anchor updater feeds. done runs when the goroutine ends.
func (n *Node) launchUtxoSetCheckpoint(
	ctx context.Context,
	set *utxoset.Set,
	done func(),
) bool {
	n.utxoCkptMu.Lock()
	defer n.utxoCkptMu.Unlock()
	if n.anchoredUtxoSet.Load() != set {
		return false
	}
	ctx, cancel := context.WithCancel(ctx)
	n.utxoCkptCancel = cancel
	n.utxoCkptWG.Go(func() {
		defer cancel()
		defer done()
		if _, err := set.WriteCheckpoint(ctx); err != nil {
			n.config.logger.Warn(
				"could not write a live UTxO set checkpoint",
				"component", "dingo", "error", err,
			)
		}
	})
	return true
}

// stopUtxoSetCheckpoint cancels a running checkpoint copy and waits for it to
// end. The anchored set is withdrawn first, so none starts afterwards.
func (n *Node) stopUtxoSetCheckpoint() {
	n.anchoredUtxoSet.Store(nil)
	n.utxoCkptMu.Lock()
	if n.utxoCkptCancel != nil {
		n.utxoCkptCancel()
	}
	n.utxoCkptMu.Unlock()
	n.utxoCkptWG.Wait()
}

// finishPendingImport finishes a ledger-state re-import the node began and
// did not finish. Any other import a stop left in the set is refused: only
// running the Mithril command that began it again completes it.
func (n *Node) finishPendingImport(ctx context.Context) error {
	if resumed, err := n.resumeLedgerReimport(ctx); resumed || err != nil {
		return err
	}
	if n.utxoSet == nil {
		return nil
	}
	if _, pending, err := n.utxoSet.ImportPending(); err != nil {
		return fmt.Errorf("read the live UTxO set's import mark: %w", err)
	} else if pending {
		return badgerutxo.ErrImportPending
	}
	return nil
}

// resumeLedgerReimport finishes a ledger-state re-import the node began and
// did not finish, from where it stopped. The run's marks stay set until it has
// swapped the imported set in and recorded the Mithril anchor, so they, not the
// import's own marker, say that something is left: a set that holds the
// imported state, which the swap mark names, needs only the anchor and the
// marks cleared, and anything earlier runs the import again from a full pass
// over the snapshot. A set at the ledger's tip without the swap mark is not
// taken for the imported one.
func (n *Node) resumeLedgerReimport(ctx context.Context) (bool, error) {
	if n.utxoSet == nil || !n.utxoSet.CanReimport() ||
		n.finishLedgerReimport == nil {
		return false, nil
	}
	status, err := n.db.GetChainState(database.SyncStatusKey, nil)
	if err != nil {
		return false, fmt.Errorf("read sync status: %w", err)
	}
	reimport, err := n.db.GetChainState(database.LedgerReimportSyncKey, nil)
	if err != nil {
		return false, fmt.Errorf("read re-import mark: %w", err)
	}
	if status == "" || reimport == "" {
		return false, nil
	}
	if err := n.utxoSet.FinishRestore(ctx); err != nil {
		return true, err
	}
	tip, err := n.db.GetTip(nil)
	if err != nil {
		return true, fmt.Errorf("read ledger tip: %w", err)
	}
	_, pending, err := n.utxoSet.ImportPending()
	if err != nil {
		return true, fmt.Errorf("read the live UTxO set's import mark: %w", err)
	}
	setTip, ok, err := n.utxoSet.Tip()
	if err != nil {
		return true, fmt.Errorf("read live UTxO set tip: %w", err)
	}
	swapped, err := n.db.GetChainState(
		database.LedgerReimportSwappedSyncKey,
		nil,
	)
	if err != nil {
		return true, fmt.Errorf("read re-import swap mark: %w", err)
	}
	if !pending && ok && setTip.Slot == tip.Point.Slot &&
		bytes.Equal(setTip.Hash, tip.Point.Hash) &&
		swapped == database.LedgerReimportPointMark(tip.Point) {
		n.config.logger.Info(
			"finishing a ledger-state re-import whose set swap completed",
			"component", "dingo", "slot", tip.Point.Slot,
		)
		return true, n.finishLedgerReimport(ctx, tip.Point)
	}
	n.config.logger.Info(
		"resuming a ledger-state re-import the node began",
		"component", "dingo",
	)
	_, err = n.utxoSet.Reimport(ctx, tip.Point)
	return true, err
}

// installLedgerReimport gives the set the Mithril re-import it falls back to
// when no checkpoint brings it to the ledger.
func (n *Node) installLedgerReimport(set *utxoset.Set) {
	factory := n.config.mithrilLedgerReimport
	if factory == nil {
		return
	}
	storageMode := n.config.storageMode
	if storageMode == "" {
		storageMode = StorageModeCore
	}
	re, err := factory(MithrilCatchUpParams{
		Mithril:           n.config.cfg.Mithril,
		StorageMode:       storageMode,
		CardanoNodeConfig: n.config.cardanoNodeConfig,
		Network:           n.config.network,
		NetworkMagic:      n.config.networkMagic,
		DataDir:           n.config.dataDir,
		Logger:            n.config.logger,
		PromRegistry:      n.retainedComponentPromRegistry(),
	})
	if err != nil {
		n.config.logger.Warn(
			"no Mithril source for recovering the live UTxO set",
			"component", "dingo", "error", err,
		)
		return
	}
	if re == nil {
		return
	}
	n.finishLedgerReimport = func(ctx context.Context, point ocommon.Point) error {
		return re.FinishLedgerReimport(ctx, n.db, point)
	}
	set.SetScratchReimporter(func(
		ctx context.Context,
		localTip ocommon.Point,
		target utxoset.ReimportTarget,
	) (ocommon.Point, error) {
		ls := n.ledgerState
		if ls == nil {
			return ocommon.Point{}, errors.New("the ledger is not available")
		}
		return re.ReimportLedgerState(ctx, LedgerReimportParams{
			DB:       n.db,
			UtxoSet:  target.Set,
			Commit:   target.Commit,
			LocalTip: localTip,
			OnChain:  ls.ChainContainsPoint,
			EpochOf: func(slot uint64) (uint64, error) {
				e, err := ls.SlotToEpoch(slot)
				return e.EpochId, err
			},
		})
	})
}
