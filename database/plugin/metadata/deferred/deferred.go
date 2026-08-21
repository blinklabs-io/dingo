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

// Package deferred holds the bulk-load deferred-index manifest.
//
// The parent metadata package owns the DeferredIndexManager
// interface that callers type-assert against. This package owns the
// data: what to drop, what to rebuild, and the sync_state key that
// records crash-recovery state.
package deferred

// Index is one entry in the deferred-index manifest. Each entry
// names an index that is safe to drop while the database is in
// bulk-load mode (Mithril sync ledger-state import, immutable blob
// load, API-mode historical metadata backfill) and rebuild before
// the database is marked ready.
//
// The manifest deliberately excludes:
//
//   - Primary keys (autoincrement IDs).
//   - Unique indexes that back ON CONFLICT clauses used during
//     import (e.g. utxo.tx_id_output_idx, transaction.hash,
//     asset.idx_asset_unique, datum.hash, script.hash,
//     certs.uniq_tx_cert).
//   - Indexes on resume-checkpoint tables
//     (import_checkpoint.import_key, backfill_checkpoint.phase).
//   - The utxo (tx_id, output_idx) lookup index, required to
//     resolve transaction inputs during backfill UTxO spending.
//   - Indexes the import path's own idempotency deletes filter on
//     (key_witness, witness_scripts, redeemer, plutus_data by
//     transaction_id; address_transaction by transaction_id).
//   - Cross-row uniqueness constraints used by ledger-state import
//     (pool_stake_snapshot, reward_snapshot, reward_pool_input,
//     network_state, account.staking_key, drep.credential, etc.).
//
// Adding a new index to the versioned metadata schema requires deciding its
// bulk-load behavior at the same time:
//
//  1. Does any import path (ledger-state import, immutable blob
//     load, backfill block replay) rely on the index for an ON
//     CONFLICT target, FK enforcement, constraint lookup, or the
//     WHERE clause of a per-row idempotency delete or aggregate
//     refresh? If yes, leave it out of the manifest. A predicate an
//     import path runs once per transaction cannot afford a scan of
//     a table the same import path is growing.
//  2. Does the index only serve API/query/rollback paths that do
//     not run during Mithril sync? If yes, add it here.
//  3. Composite indexes share state with their constituent columns. If a field
//     has both a deferrable single-column query index and a protected composite
//     unique index, give the single-column index an explicit name and list that
//     name here instead of the field.
//
// See deferred_test.go for manifest invariants.
type Index struct {
	// Name is the explicit index name in the versioned schema.
	Name string
	// Table is the SQL table name.
	Table string
	// Columns is the ordered SQL column list.
	Columns []string
	// Notes documents why this index is safe to defer. Surfaces
	// in the manifest test failure message when the
	// classification is questioned.
	Notes string
	// Critical marks indexes that must be present before the API
	// can serve traffic. Critical indexes are rebuilt first so
	// that the node can accept queries while the remaining lazy
	// indexes finish in the background.
	//
	// Criteria for Critical=true:
	//   - Any WHERE predicate on the index column used by a live
	//     API query path (blockfrost, utxorpc, ledger queries).
	//   - Any WHERE predicate used by the rollback path
	//     (DeleteXAfterSlot), since rollbacks can occur as soon
	//     as live sync resumes.
	//
	// Everything else is lazy: FK reverse-lookups,
	// witness/redeemer secondary indexes, and any column that is
	// only SELECTed or SET but never filtered.
	Critical bool
}

// CriticalManifest returns the subset of Manifest entries that are
// marked Critical=true. These are the indexes that must be present
// before the API can serve traffic.
func CriticalManifest() []Index {
	var out []Index
	for _, idx := range Manifest {
		if idx.Critical {
			out = append(out, idx)
		}
	}
	return out
}

// SyncStateKey is the sync_state row that marks an in-flight (or
// interrupted) deferred-index drop/rebuild cycle. The value is the
// string "true" while the rebuild is outstanding and is removed
// once every manifest entry is present.
const SyncStateKey = "metadata_indexes_pending"

// SyncStateValue is the literal sync_state value written while a
// drop/rebuild cycle is outstanding.
const SyncStateValue = "true"

// Manifest is the canonical list of metadata-store indexes that are
// dropped before bulk load and rebuilt before the database is
// marked ready.
//
// The list is intentionally conservative: it targets the heaviest
// write paths (utxo, transaction, asset, datum, witness,
// certs/redeemer secondary indexes) where API backfill spends the
// bulk of its time.
//
// Order matters at rebuild time only as a logging convenience;
// SQLite builds each index in a single statement and does not
// benefit from re-ordering.
var Manifest = []Index{
	{
		Name: "idx_utxo_payment_key", Table: "utxo",
		Columns: []string{"payment_key"},
		Notes:   "API address lookup", Critical: true,
	},
	{
		Name: "idx_utxo_staking_key", Table: "utxo",
		Columns: []string{"staking_key"},
		Notes:   "API stake lookup", Critical: true,
	},
	{
		Name: "idx_utxo_spent_at_tx_id", Table: "utxo",
		Columns: []string{"spent_at_tx_id"},
		Notes:   "Consumer transaction lookup and rollback repair", Critical: true,
	},
	{
		Name: "idx_utxo_referenced_by_tx_id", Table: "utxo",
		Columns: []string{"referenced_by_tx_id"},
		Notes:   "Reference-input lookup and rollback repair", Critical: true,
	},
	{
		Name: "idx_utxo_collateral_by_tx_id", Table: "utxo",
		Columns: []string{"collateral_by_tx_id"},
		Notes:   "Collateral lookup and rollback repair", Critical: true,
	},
	{
		Name: "idx_utxo_added_slot", Table: "utxo",
		Columns: []string{"added_slot"},
		Notes:   "Rollback range scan", Critical: true,
	},
	{
		Name: "idx_utxo_transaction_id", Table: "utxo",
		Columns: []string{"transaction_id"},
		Notes:   "Foreign-key reverse lookup",
	},
	{
		Name: "idx_utxo_deleted_staking_amount", Table: "utxo",
		Columns: []string{
			"deleted_slot",
			"credential_tag",
			"staking_key",
			"amount",
		},
		Notes: "Primary UTxO RPC search path", Critical: true,
	},
	// idx_utxo_staking_deleted_amount is deliberately NOT deferred.
	// FlushBatch -> refreshRewardLiveStakeAggregates runs a per-credential
	// live UTxO SUM (WHERE credential_tag = ? AND staking_key = ? AND
	// deleted_slot = 0) on every API-mode backfill batch. With this index
	// dropped each of those SUMs is a full scan of the growing utxo table,
	// making the backfill quadratic: measured on preview, throughput
	// collapsed from ~1700 to ~4 blocks/sec by 3% progress. The one-shot
	// RebuildRewardLiveStake at the end of ledger-state import tolerates a
	// missing index (single full scan), but the per-batch incremental
	// refresh cannot.
	{
		Name: "idx_utxo_deleted_payment_script", Table: "utxo",
		Columns: []string{"deleted_slot", "payment_script", "amount"},
		Notes:   "Script-locked supply", Critical: true,
	},
	{
		Name: "idx_transaction_block_hash", Table: "transaction",
		Columns: []string{"block_hash"},
		Notes:   "Block transaction grouping", Critical: true,
	},
	{
		Name: "idx_transaction_slot", Table: "transaction",
		Columns: []string{"slot"},
		Notes:   "Rollback and transaction history ordering", Critical: true,
	},
	{
		Name:    "idx_asset_name_hex",
		Table:   "asset",
		Columns: []string{"name_hex"},
		Notes:   "Asset name lookup",
	},
	{
		Name: "idx_asset_policy_id", Table: "asset",
		Columns: []string{"policy_id"},
		Notes:   "Policy lookup", Critical: true,
	},
	{
		Name:    "idx_asset_fingerprint",
		Table:   "asset",
		Columns: []string{"fingerprint"},
		Notes:   "Fingerprint lookup",
	},
	{
		Name:    "idx_asset_amount",
		Table:   "asset",
		Columns: []string{"amount"},
		Notes:   "Amount range scan",
	},
	{
		Name:    "idx_datum_added_slot",
		Table:   "datum",
		Columns: []string{"added_slot"},
		Notes:   "Datum rollback scan",
	},
	{
		Name:    "idx_certs_block_hash",
		Table:   "certs",
		Columns: []string{"block_hash"},
		Notes:   "Block certificate lookup",
	},
	{
		Name:    "idx_certs_certificate_id",
		Table:   "certs",
		Columns: []string{"certificate_id"},
		Notes:   "Certificate reverse lookup",
	},
	{
		Name: "idx_certs_slot", Table: "certs", Columns: []string{"slot"},
		Notes: "Certificate rollback scan", Critical: true,
	},
	{
		Name:    "idx_certs_cert_type",
		Table:   "certs",
		Columns: []string{"cert_type"},
		Notes:   "Certificate type filter",
	},
	// idx_redeemer_transaction_id, idx_key_witness_transaction_id, and
	// idx_witness_scripts_transaction_id are deliberately NOT deferred.
	// storeTransactionWitnesses (database/plugin/metadata/sqlstore) rewrites
	// the witness tables on every API-mode SetTransaction, and clears the
	// previous attempt's rows first with an unconditional
	// DELETE ... WHERE transaction_id = ? per table. With those indexes
	// dropped each delete is a full scan of a table that gains a row per
	// transaction written, so per-transaction cost grows with the rows
	// already present and historical backfill turns quadratic: measured on
	// preview, Mithril backfill fell from 3311 to 9 blocks/sec by 2%
	// progress while its own ETA climbed from 30m to 177h (issue #3253).
	// plutus_data, the fourth table the same loop clears, was never
	// deferred; these three now match it.
	{
		Name:    "idx_redeemer_index",
		Table:   "redeemer",
		Columns: []string{"index"},
		Notes:   "Redeemer index lookup",
	},
	{
		Name:    "idx_redeemer_tag",
		Table:   "redeemer",
		Columns: []string{"tag"},
		Notes:   "Redeemer tag filter",
	},
	{
		Name:    "idx_key_witness_type",
		Table:   "key_witness",
		Columns: []string{"type"},
		Notes:   "Witness type filter",
	},
	{
		Name:    "idx_witness_scripts_script_hash",
		Table:   "witness_scripts",
		Columns: []string{"script_hash"},
		Notes:   "Script hash lookup",
	},
	{
		Name:    "idx_witness_scripts_type",
		Table:   "witness_scripts",
		Columns: []string{"type"},
		Notes:   "Script type filter",
	},
}
