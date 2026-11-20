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

	"github.com/blinklabs-io/dingo/internal/nodestorage"
)

// The steps of nodestorage.Reconcile, run one at a time against a node's
// database and set.

type (
	drainStore      = nodestorage.DrainStore
	publishingStore = nodestorage.PublishingStore
)

func (n *Node) migrateUtxoRows(ctx context.Context) error {
	return nodestorage.MigrateUtxoRows(
		ctx, n.db, n.utxoSet, n.config.dataDir, n.config.logger,
	)
}

func (n *Node) checkUtxoSetOnly() error {
	return nodestorage.CheckUtxoSetOnly(n.db)
}

func (n *Node) publishDrainedFamilies(ctx context.Context) error {
	mask, err := n.db.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	n.setDrainedFamilies(mask)
	return nil
}

func (n *Node) drainLedgerState(ctx context.Context) error {
	return nodestorage.DrainLedgerState(
		ctx, n.db, n.utxoSet, n.config.logger, nil, n.setDrainedFamilies,
	)
}
