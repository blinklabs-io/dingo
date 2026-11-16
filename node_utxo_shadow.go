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
	"fmt"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoshadow"
)

// startUtxoShadow starts the shadow UTxO set when it is enabled. It needs an
// on-disk database: an in-memory node logs that and runs without it.
func (n *Node) startUtxoShadow() error {
	if !n.config.ledgerUtxoShadow || n.utxoShadow != nil {
		return nil
	}
	if n.config.dataDir == "" {
		n.config.logger.Warn(
			"shadow UTxO set needs an on-disk database; not starting it",
		)
		return nil
	}
	shadow, err := utxoshadow.Start(utxoshadow.Config{
		Dir:        filepath.Join(n.config.dataDir, "ledger", "utxo-shadow"),
		LedgerDB:   dbadapter.New(n.db),
		EventBus:   n.eventBus,
		Logger:     n.config.logger,
		Registerer: n.config.promRegistry,
	})
	if err != nil {
		return fmt.Errorf("start shadow UTxO set: %w", err)
	}
	n.utxoShadow = shadow
	return nil
}

// stopUtxoShadow stops the shadow UTxO set if it is running.
func (n *Node) stopUtxoShadow() error {
	if n.utxoShadow == nil {
		return nil
	}
	err := n.utxoShadow.Stop()
	n.utxoShadow = nil
	return err
}
