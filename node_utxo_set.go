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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
)

// openUtxoSet opens the live UTxO set when it is enabled. It needs an on-disk
// database: an in-memory node logs that and runs without it. It stays open
// across a live restore or truncate, which reconcile it again.
func (n *Node) openUtxoSet() error {
	if !n.config.ledgerUtxoBadger || n.utxoSet != nil {
		return nil
	}
	if n.config.dataDir == "" {
		n.config.logger.Warn(
			"live UTxO set needs an on-disk database; not opening it",
		)
		return nil
	}
	// The Shelley window, 3k/f, is longer than Byron's 2k, so it covers
	// every era's rollbacks.
	history, err := ledger.ShelleyStabilityWindow(
		n.config.cardanoNodeConfig.ShelleyGenesis(),
	)
	if err != nil {
		n.config.logger.Warn(
			"no stability window for the live UTxO set; keeping its whole rollback journal",
			"error",
			err,
		)
		history = 0
	}
	set, err := utxoset.Open(utxoset.Config{
		Dir:          utxoset.DirFor(n.config.dataDir),
		Logger:       n.config.logger,
		Registerer:   n.config.promRegistry,
		HistorySlots: history,
		Only:         n.config.ledgerUtxoBadgerOnly,
		Authoritative: n.config.ledgerUtxoReads ==
			ledger.UtxoSetReadsSet,
	})
	if err != nil {
		return fmt.Errorf("open live UTxO set: %w", err)
	}
	n.utxoSet = set
	n.anchoredUtxoSet.Store(set)
	return nil
}

// reconcileUtxoSet makes the live UTxO set equal to the ledger's live set,
// rolling the ledger back to the set's tip when the set trails it. It runs
// after database recovery and before the ledger starts applying blocks.
func (n *Node) reconcileUtxoSet(ctx context.Context) error {
	if err := n.checkLedgerStateSetOnly(ctx); err != nil {
		return err
	}
	if err := n.checkUtxoSetOnly(); err != nil {
		return err
	}
	if err := n.publishDrainedFamilies(ctx); err != nil {
		return err
	}
	if n.utxoSet != nil {
		if err := n.utxoSet.Reconcile(
			ctx,
			dbadapter.New(n.db),
			n.ledgerState.ResumeUtxoSetFrom,
		); err != nil {
			return fmt.Errorf("reconcile live UTxO set: %w", err)
		}
	}
	n.utxoSetReconciled = true
	if n.config.ledgerUtxoBadgerOnly {
		if err := n.backfillRewardLiveStake(ctx); err != nil {
			return fmt.Errorf("failed to backfill reward live stake: %w", err)
		}
	}
	return n.drainLedgerState(ctx)
}

// utxoSetBackuper returns the live UTxO set for a snapshot, nil when the node
// keeps none.
func (n *Node) utxoSetBackuper() lifecycle.UtxoSetBackuper {
	if n.utxoSet == nil {
		return nil
	}
	return n.utxoSet
}

// closeUtxoSet closes the live UTxO set. The ledger must have stopped
// applying blocks.
func (n *Node) closeUtxoSet() error {
	n.utxoSetReconciled = false
	if n.utxoSet == nil {
		return nil
	}
	n.anchoredUtxoSet.Store(nil)
	err := n.utxoSet.Close()
	n.utxoSet = nil
	return err
}

// checkUtxoSetOnly keeps a database in the mode it was synced in. A database
// synced with the live UTxO set as the only copy has no UTxO set in its
// metadata store to fall back to, so it refuses to start without
// ledgerUtxoBadgerOnly; and a database with a metadata UTxO set can be
// switched to ledgerUtxoBadgerOnly only while it is empty, before any block.
func (n *Node) checkUtxoSetOnly() error {
	marked, err := n.db.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	if err != nil {
		return fmt.Errorf("read UTxO set mode: %w", err)
	}
	only := n.config.ledgerUtxoBadgerOnly
	switch {
	case !only && marked != "":
		return errors.New(
			"this database keeps its UTxO set only in the live UTxO set; start with ledgerUtxoBadgerOnly",
		)
	case only && marked == "":
		tip, err := n.db.GetTip(nil)
		if err != nil {
			return fmt.Errorf("read tip: %w", err)
		}
		if tip.Point.Slot != 0 || len(tip.Point.Hash) != 0 {
			return errors.New(
				"ledgerUtxoBadgerOnly needs a database synced that way from genesis; this one has a metadata UTxO set",
			)
		}
		if err := n.db.SetSyncState(database.UtxoSetOnlySyncKey, "1", nil); err != nil {
			return fmt.Errorf("record UTxO set mode: %w", err)
		}
	}
	return nil
}

// checkLedgerStateSetOnly keeps a database in the ledger-state mode it was
// drained in. A database that has drained, or begun draining, any family has no SQL rows for it,
// so it refuses to start without ledgerStateSetOnly; and the switch is
// accepted only for a database that holds no UTxO rows to begin with: one
// marked set-only, or still at origin.
func (n *Node) checkLedgerStateSetOnly(ctx context.Context) error {
	drained, err := n.db.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	if !n.config.ledgerStateSetOnly {
		if drained != 0 {
			return fmt.Errorf(
				"this database no longer holds the SQL rows of ledger-state "+
					"families %v; start with ledgerStateSetOnly",
				drained.Families(),
			)
		}
		return nil
	}
	marked, err := n.db.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	if err != nil {
		return fmt.Errorf("read UTxO set mode: %w", err)
	}
	if marked != "" {
		return nil
	}
	tip, err := n.db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read tip: %w", err)
	}
	if tip.Point.Slot != 0 || len(tip.Point.Hash) != 0 {
		return errors.New(
			"ledgerStateSetOnly needs a database synced with " +
				"ledgerUtxoBadgerOnly from genesis or a Mithril import; " +
				"this one keeps UTxO rows in the metadata store, so " +
				"resync it from genesis or a Mithril snapshot",
		)
	}
	return nil
}

// utxoSetAnchorInterval is how often the live UTxO set's anchor is refreshed.
// The anchor is k blocks deep, so a few seconds of staleness cannot matter.
const utxoSetAnchorInterval = 5 * time.Second

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
		point, found, err := ls.ImmutablePoint(ctx)
		if err != nil || !found {
			return
		}
		set.UpdateAnchor(point)
	}
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
			}
		}
	}()
}
