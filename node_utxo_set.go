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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
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
	})
	if err != nil {
		return fmt.Errorf("open live UTxO set: %w", err)
	}
	n.utxoSet = set
	return nil
}

// reconcileUtxoSet makes the live UTxO set equal to the ledger's live set,
// rolling the ledger back to the set's tip when the set trails it. It runs
// after database recovery and before the ledger starts applying blocks.
func (n *Node) reconcileUtxoSet(ctx context.Context) error {
	if err := n.checkUtxoSetOnly(); err != nil {
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
	return nil
}

// closeUtxoSet closes the live UTxO set. The ledger must have stopped
// applying blocks.
func (n *Node) closeUtxoSet() error {
	n.utxoSetReconciled = false
	if n.utxoSet == nil {
		return nil
	}
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
