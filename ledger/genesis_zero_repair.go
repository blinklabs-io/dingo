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

package ledger

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ZeroValueByronOutputs names the zero-coin Byron genesis outputs of a
// network's genesis configuration.
type ZeroValueByronOutputs struct {
	// Refs are the outputs.
	Refs []models.UtxoId
	// TxHashes are the genesis transactions that produced no other genesis
	// output.
	TxHashes [][]byte
}

// ByronGenesisZeroRepairSyncKey is the sync_state key recording that
// RepairZeroValueByronGenesis finished on a metadata store.
const ByronGenesisZeroRepairSyncKey = "ledger.byron_genesis_zero_repair"

// ZeroValueByronGenesisOutputs derives the zero-coin Byron genesis outputs
// from the genesis configuration, the same source buildGenesisUtxoSet reads.
// A configuration without Byron genesis, and a network with no zero-coin
// Byron output, yield none.
//
// The reference Byron to Shelley UTxO translation drops exactly these
// outputs; a zero-coin Shelley initialFunds output is kept, so only the
// Byron genesis decides membership. A Byron output whose reference a retained
// genesis output also has is left out of Refs, since removing by reference
// would remove the retained output.
func ZeroValueByronGenesisOutputs(
	nodeCfg *cardano.CardanoNodeConfig,
) (ZeroValueByronOutputs, error) {
	var zero ZeroValueByronOutputs
	if nodeCfg == nil || nodeCfg.ByronGenesis() == nil {
		return zero, nil
	}
	byronUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	if err != nil {
		return zero, fmt.Errorf("generate Byron genesis UTxOs: %w", err)
	}
	dropped, retained := splitZeroValueByron(byronUtxos)
	if len(dropped) == 0 {
		return zero, nil
	}
	if shelleyGenesis := nodeCfg.ShelleyGenesis(); shelleyGenesis != nil {
		shelleyUtxos, err := shelleyGenesis.GenesisUtxos()
		if err != nil {
			return zero, fmt.Errorf("generate Shelley genesis UTxOs: %w", err)
		}
		retained = append(retained, shelleyUtxos...)
	}
	// A genesis transaction's id is the hash of its address and its one
	// output is index 0, so a retained output at the same address has the
	// same reference and shares the transaction. The reference then names the
	// retained output, so it is never one to remove.
	type outputRef struct {
		id  [32]byte
		idx uint32
	}
	kept := make(map[outputRef]struct{}, len(retained))
	shared := make(map[[32]byte]struct{}, len(retained))
	for _, u := range retained {
		kept[outputRef{u.Id.Id(), u.Id.Index()}] = struct{}{}
		shared[u.Id.Id()] = struct{}{}
	}
	seen := make(map[[32]byte]struct{}, len(dropped))
	for _, u := range dropped {
		id := u.Id.Id()
		if _, ok := kept[outputRef{id, u.Id.Index()}]; !ok {
			zero.Refs = append(zero.Refs, models.UtxoId{
				Hash: id.Bytes(),
				Idx:  u.Id.Index(),
			})
		}
		if _, ok := shared[id]; ok {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		zero.TxHashes = append(zero.TxHashes, id.Bytes())
	}
	slices.SortFunc(zero.Refs, func(a, b models.UtxoId) int {
		if c := bytes.Compare(a.Hash, b.Hash); c != 0 {
			return c
		}
		return cmp.Compare(a.Idx, b.Idx)
	})
	slices.SortFunc(zero.TxHashes, bytes.Compare)
	return zero, nil
}

// splitZeroValueByron separates the zero-coin outputs of utxos from the rest.
func splitZeroValueByron(
	utxos []lcommon.Utxo,
) (zero, retained []lcommon.Utxo) {
	for _, u := range utxos {
		if isZeroCoin(u) {
			zero = append(zero, u)
		} else {
			retained = append(retained, u)
		}
	}
	return zero, retained
}

func isZeroCoin(u lcommon.Utxo) bool {
	amount := u.Output.Amount()
	return amount != nil && amount.Sign() == 0
}

// RepairZeroValueByronGenesis removes the outputs of zero from a metadata
// store that holds them: a store seeded before the genesis seed dropped
// them. It removes exactly those outputs, only while unspent, with their
// dependent rows and the genesis transactions nothing else refers to, and
// records ByronGenesisZeroRepairSyncKey in the same transaction, so an
// interrupted repair leaves the store as it was and the next start repeats
// it. A store with the key set, and a network with no such output, are not
// touched.
//
// removedTransactions, when non-nil, is called inside that transaction with
// how many applied_transaction rows the repair removed, for a store that
// keeps a count of them elsewhere.
func RepairZeroValueByronGenesis(
	ctx context.Context,
	db *database.Database,
	zero ZeroValueByronOutputs,
	logger *slog.Logger,
	removedTransactions func(*database.Txn, uint64) error,
) error {
	if len(zero.Refs) == 0 {
		return nil
	}
	done, err := db.GetSyncState(ByronGenesisZeroRepairSyncKey, nil)
	if err != nil {
		return fmt.Errorf(
			"read zero-value Byron genesis repair marker: %w",
			err,
		)
	}
	if done != "" {
		return nil
	}
	txn := db.Transaction(ctx, true)
	defer txn.Rollback() //nolint:errcheck
	removed, applied, err := db.RemoveGenesisUtxos(
		ctx, zero.Refs, zero.TxHashes, txn,
	)
	if err != nil {
		return fmt.Errorf("remove zero-value Byron genesis outputs: %w", err)
	}
	if removedTransactions != nil {
		if err := removedTransactions(txn, applied); err != nil {
			return err
		}
	}
	if err := db.SetSyncState(ByronGenesisZeroRepairSyncKey, "1", txn); err != nil {
		return fmt.Errorf("record zero-value Byron genesis repair: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit zero-value Byron genesis repair: %w", err)
	}
	if logger != nil && len(removed) > 0 {
		logger.Info(
			"removed zero-value Byron genesis outputs",
			"component", "ledger",
			"outputs", len(removed),
		)
	}
	return nil
}
