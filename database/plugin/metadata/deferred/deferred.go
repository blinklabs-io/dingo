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

// Package deferred holds the bulk-load deferred-index manifest in a
// stand-alone package so each metadata plugin (sqlite, mysql,
// postgres) can import it without pulling in the parent metadata
// package, which side-effect-imports every plugin and would create
// an import cycle.
//
// The parent metadata package owns the DeferredIndexManager
// interface that callers type-assert against. This package owns the
// data: what to drop, what to rebuild, and the sync_state key that
// records crash-recovery state.
package deferred

import (
	"github.com/blinklabs-io/dingo/database/models"
)

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
//   - Cross-row uniqueness constraints used by ledger-state import
//     (pool_stake_snapshot, reward_snapshot, reward_pool_input,
//     network_state, account.staking_key, drep.credential, etc.).
//
// Adding a new index to a metadata GORM model? Decide on bulk-load
// behavior at the same time:
//
//  1. Does any import path (ledger-state import, immutable blob
//     load, backfill block replay) rely on the index for an ON
//     CONFLICT target, FK enforcement, or constraint lookup? If
//     yes, leave it out of the manifest.
//  2. Does the index only serve API/query/rollback paths that do
//     not run during Mithril sync? If yes, add it here.
//  3. Composite indexes share state with their constituent columns. If a field
//     has both a deferrable single-column query index and a protected composite
//     unique index, give the single-column index an explicit name and list that
//     name here instead of the field.
//
// See deferred_test.go for the regression test that walks every
// model field and asserts the classification.
type Index struct {
	// Model is the GORM model the index is attached to. Used by
	// GORM's migrator to resolve the index back to the underlying
	// SQL statement.
	Model any
	// Field is the Go struct field name on Model. When the index
	// is a single-column index defined inline (`gorm:"index"`),
	// GORM resolves Field to an auto-generated index name during
	// DropIndex/CreateIndex.
	Field string
	// Name is the literal index name. Used for composite/named
	// indexes (e.g. idx_utxo_deleted_staking_amount) where the
	// struct-tag name takes precedence over field resolution.
	// Leave empty for single-column auto-named indexes.
	Name string
	// Table is the SQL table name. Carried separately because
	// some backends can drop an index without going through GORM.
	Table string
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

// ResolvedName returns the GORM-visible name for the index — the
// literal Name when set, otherwise the Field which GORM resolves to
// the auto-generated name through the struct's schema.
func (i Index) ResolvedName() string {
	if i.Name != "" {
		return i.Name
	}
	return i.Field
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
	// utxo: PaymentKey/StakingKey are used by API address
	// lookups; SpentAtTxId / ReferencedByTxId / CollateralByTxId
	// support rollback and consumer-tx queries; AddedSlot
	// supports range scans. None of these is consulted by the
	// spend predicate, which uses tx_id_output_idx exclusively.
	{
		Model: &models.Utxo{}, Field: "PaymentKey", Table: "utxo",
		Notes:    "API address lookup; not touched by UTxO insert or spend",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Field: "StakingKey", Table: "utxo",
		Notes:    "API stake lookup; not touched by UTxO insert or spend",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Field: "SpentAtTxId", Table: "utxo",
		Notes:    "Consumer-tx query and rollback repair (DeleteTransactionsAfterSlot); not used by spend predicate",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Field: "ReferencedByTxId", Table: "utxo",
		Notes:    "Reference-input query and rollback repair (DeleteTransactionsAfterSlot); not used by insert path",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Field: "CollateralByTxId", Table: "utxo",
		Notes:    "Collateral query and rollback repair (DeleteTransactionsAfterSlot); not used by insert path",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Field: "AddedSlot", Table: "utxo",
		Notes:    "Rollback range scan (DeleteUtxosAfterSlot); not used by insert or spend",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Field: "TransactionID", Table: "utxo",
		Notes: "FK reverse-lookup; cascades go the other direction during sync",
	},
	{
		Model: &models.Utxo{}, Name: "idx_utxo_deleted_staking_amount", Table: "utxo",
		Notes:    "Composite SearchUtxos index; primary utxorpc query path",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Name: "idx_utxo_staking_deleted_amount", Table: "utxo",
		Notes: "DRep voting-power live UTxO SUM; live query path. RebuildRewardLiveStake " +
			"runs during ledger-state import and hints this index, but the sqlite backend " +
			"applies the INDEXED BY hint only when the index exists, so it stays deferrable.",
		Critical: true,
	},
	{
		Model: &models.Utxo{}, Name: "idx_utxo_deleted_payment_script", Table: "utxo",
		Notes:    "Script-locked supply SUM (blockfrost /network); live query path",
		Critical: true,
	},

	// transaction: BlockHash/Slot are query indexes; the
	// uniqueIndex on Hash carries the ON CONFLICT target and
	// stays.
	{
		Model: &models.Transaction{}, Field: "BlockHash", Table: "transaction",
		Notes:    "Block-tx grouping query (blockfrost /blocks/{id}/txs); not used by tx upsert",
		Critical: true,
	},
	{
		Model: &models.Transaction{}, Field: "Slot", Table: "transaction",
		Notes:    "Rollback range scan (DeleteTransactionsAfterSlot) and tx history ordering; not used by tx upsert",
		Critical: true,
	},

	// asset: idx_asset_unique (Name + PolicyId + UtxoID) is the
	// ON CONFLICT target during import and stays. Secondary
	// per-column indexes are deferable.
	{
		Model: &models.Asset{}, Field: "NameHex", Table: "asset",
		Notes: "Hex name lookup; query-only; no current WHERE name_hex=? path in API",
	},
	{
		Model: &models.Asset{}, Name: "idx_asset_policy_id", Table: "asset",
		Notes:    "Policy-id lookup (GetAssetsByPolicy); query-only; idx_asset_unique still covers import",
		Critical: true,
	},
	{
		Model: &models.Asset{}, Field: "Fingerprint", Table: "asset",
		Notes: "Fingerprint lookup; query-only; returned as response field, not a WHERE predicate",
	},
	{
		Model: &models.Asset{}, Field: "Amount", Table: "asset",
		Notes: "Amount range scan; query-only",
	},

	// datum: AddedSlot is rollback / query only. The unique
	// constraint on Hash stays for the dedup ON CONFLICT path.
	{
		Model: &models.Datum{}, Field: "AddedSlot", Table: "datum",
		Notes: "Rollback range scan; not used by datum upsert",
	},

	// certs: Slot/BlockHash/CertificateID/CertType drive query
	// and rollback paths. The uniq_tx_cert (TransactionID +
	// CertIndex) uniqueIndex remains for cert ordering.
	{
		Model: &models.Certificate{}, Field: "BlockHash", Table: "certs",
		Notes: "Block-cert query; not used by cert insert",
	},
	{
		Model: &models.Certificate{}, Field: "CertificateID", Table: "certs",
		Notes: "Polymorphic FK reverse-lookup; not enforced by DB",
	},
	{
		Model: &models.Certificate{}, Field: "Slot", Table: "certs",
		Notes:    "Rollback range scan (DeleteCertificatesAfterSlot); not used by cert insert",
		Critical: true,
	},
	{
		Model: &models.Certificate{}, Field: "CertType", Table: "certs",
		Notes: "Filter index; query-only",
	},

	// redeemer: secondary indexes for witness queries. Redeemers
	// are inserted via Transaction's foreignKey cascade and do
	// not rely on these indexes.
	{
		Model: &models.Redeemer{}, Field: "TransactionID", Table: "redeemer",
		Notes: "Witness query; FK enforced via parent Transaction",
	},
	{
		Model: &models.Redeemer{}, Field: "Index", Table: "redeemer",
		Notes: "Redeemer index lookup; query-only",
	},
	{
		Model: &models.Redeemer{}, Field: "Tag", Table: "redeemer",
		Notes: "Redeemer tag filter; query-only",
	},

	// witness: KeyWitness/WitnessScripts indexes back API
	// witness queries. Inserts cascade from the parent
	// Transaction.
	{
		Model: &models.KeyWitness{}, Field: "TransactionID", Table: "key_witness",
		Notes: "Witness query; FK cascade from Transaction handles inserts",
	},
	{
		Model: &models.KeyWitness{}, Field: "Type", Table: "key_witness",
		Notes: "Witness-type filter; query-only",
	},
	{
		Model: &models.WitnessScripts{}, Field: "ScriptHash", Table: "witness_scripts",
		Notes: "Script-hash lookup; query-only",
	},
	{
		Model: &models.WitnessScripts{}, Field: "TransactionID", Table: "witness_scripts",
		Notes: "Witness query; FK cascade from Transaction handles inserts",
	},
	{
		Model: &models.WitnessScripts{}, Field: "Type", Table: "witness_scripts",
		Notes: "Witness-type filter; query-only",
	},
}
