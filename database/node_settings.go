// Copyright 2025 Blink Labs Software
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
	"maps"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
)

// ErrNodeSettingsMissing reports a ledger database whose blob store has an
// identity but whose node settings are recorded nowhere.
var ErrNodeSettingsMissing = errors.New("node settings are missing")

// NodeSettingsError is returned when the configured node settings differ
// from those persisted in the database. Changing immutable settings after
// initial sync would leave the database in an inconsistent state.
type NodeSettingsError struct {
	Mismatches []string
}

// Error's message does not append a generic remedy: each entry in
// e.Mismatches is a rendered Mismatch (see nodesettings.Mismatch.String())
// that already carries its own gate-specific reason -- appending a blanket
// "requires re-syncing from scratch" on top would both duplicate that text
// for the common case and misdirect for a gate whose Remedy says otherwise
// (e.g. blob_store_id, whose fix is pointing at the right blob store, not a
// resync).
func (e NodeSettingsError) Error() string {
	return "node settings mismatch: " + strings.Join(e.Mismatches, "; ")
}

// phase1GateValues returns the gates a bare database open can supply.
// Genesis hashes and the ledger feature gates are absent here and are
// supplied later by EnforceNodeSettings, once the node has parsed its
// cardano config. Blob-store identity is required: an inability to read or
// durably create it must fail closed rather than omit the pairing gate.
func (d *Database) phase1GateValues() (nodesettings.Values, error) {
	values := nodesettings.Values{
		"storage_mode": d.config.StorageMode,
		"network":      d.config.Network,
	}
	// No bool-derived gate belongs here. A bool cannot distinguish "the
	// operator turned it off" from "this caller never set it", and phase 1
	// has partial callers: mithril/sync.go:1200 and
	// database/lifecycle/restore.go:609 construct a Config with only
	// DataDir, Logger, StorageMode and Network. history_expiry_active and
	// the two validation taints therefore all live in phase 2
	// (EnforceNodeSettings), which only full node startup reaches.
	// A zero magic means "not supplied on this path" rather than a real
	// magic of zero, so it is left absent for the fill-once rule.
	if d.config.NetworkMagic != 0 {
		values["network_magic"] = strconv.FormatUint(
			uint64(d.config.NetworkMagic), 10,
		)
	}
	if d.config.IndexerLayout != "" {
		values["indexer_layout"] = d.config.IndexerLayout
	}
	if d.config.BlobPlugin != "" {
		values["blob_plugin"] = d.config.BlobPlugin
	}
	if d.config.MetadataPlugin != "" {
		values["metadata_plugin"] = d.config.MetadataPlugin
	}
	// start_era has the same "don't know" ambiguity as NetworkMagic in
	// reverse: its zero value means both "explicitly no start era" (the
	// ordinary case for a full node startup) and "this caller's Config
	// never populated the field" (the same partial callers named above).
	// MetadataPlugin is only ever set by a full caller, so its presence is
	// what tells the
	// two apart: only then is an empty StartEra a genuine, confirmable "no
	// start era", worth persisting as nodesettings.NoStartEra so a later
	// --start-era dijkstra against this database has something to compare
	// against instead of silently filling in for free. A partial caller
	// leaves the key absent entirely, exactly like NetworkMagic/BlobPlugin/
	// MetadataPlugin above, so Evaluate treats it as unknown rather than
	// comparing.
	switch {
	case d.config.StartEra != "":
		values["start_era"] = d.config.StartEra
	case d.config.MetadataPlugin != "":
		values["start_era"] = nodesettings.NoStartEra
	}
	// blob_store_id is read (and minted, on first use) from the blob store
	// itself rather than from config. A genuine read, write, commit, or sync
	// failure must fail closed: omitting this value would let startup proceed
	// without validating the metadata/blob pairing.
	if id, err := d.blobStoreID(); err != nil {
		return nil, fmt.Errorf("get blob store id: %w", err)
	} else if id != "" {
		values["blob_store_id"] = id
	}
	return values, nil
}

// persistedGateValues returns the node settings persisted for this
// database. The ledger keeps them in the data directory's dbinfo file
// (ledgerGateValues); an IndexerDB keeps them in its own metadata store,
// which is shared across processes for the server providers.
func (d *Database) persistedGateValues(
	ctx context.Context,
) (nodesettings.Values, error) {
	if d.isIndexer() {
		return d.metadataGateValues(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return d.ledgerGateValues()
}

// metadataGateValues merges the node_settings row with node_settings_gate.
// node_settings_gate is copied on top: it is authoritative for every gate,
// including storage_mode and network, while the node_settings row keeps the
// values of its first insert forever.
func (d *Database) metadataGateValues(
	ctx context.Context,
) (nodesettings.Values, error) {
	row, err := d.Metadata().GetNodeSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get node settings from metadata: %w", err,
		)
	}
	gates, err := d.Metadata().GetNodeSettingsGates(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get node settings gates from metadata: %w", err,
		)
	}
	values := make(nodesettings.Values, len(gates)+2)
	if row != nil {
		if row.StorageMode != "" {
			values["storage_mode"] = row.StorageMode
		}
		if row.Network != "" {
			values["network"] = row.Network
		}
	}
	maps.Copy(values, gates)
	return values, nil
}

// ledgerGateValues reads the ledger's node settings from the dbinfo file. A
// ledger without a data directory keeps them in memory; see
// settleLedgerNodeSettings.
func (d *Database) ledgerGateValues() (nodesettings.Values, error) {
	d.gatesMu.Lock()
	defer d.gatesMu.Unlock()
	return d.ledgerGateValuesLocked()
}

func (d *Database) ledgerGateValuesLocked() (nodesettings.Values, error) {
	if d.config.DataDir == "" {
		values := maps.Clone(d.memGates)
		if values == nil {
			values = nodesettings.Values{}
		}
		return values, nil
	}
	info, err := dbinfo.Read(d.config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("read node settings: %w", err)
	}
	if !info.HoldsGates() {
		return nodesettings.Values{}, nil
	}
	return info.Gates, nil
}

// writeLedgerGates merges writes into the ledger's node settings and
// replaces the dbinfo file with the result. The read-modify-write needs no
// lock across processes: only the process holding the data directory opens
// the ledger, and Badger's directory lock enforces that for the local blob
// store.
func (d *Database) writeLedgerGates(writes nodesettings.Values) error {
	d.gatesMu.Lock()
	defer d.gatesMu.Unlock()
	values, err := d.ledgerGateValuesLocked()
	if err != nil {
		return err
	}
	maps.Copy(values, writes)
	if d.config.DataDir == "" {
		d.memGates = values
		return nil
	}
	if err := dbinfo.WriteGates(d.config.DataDir, values); err != nil {
		return fmt.Errorf("write node settings: %w", err)
	}
	return nil
}

// writeGateRows persists values as they are, with no evaluation and no
// node_settings row mirror.
func (d *Database) writeGateRows(
	ctx context.Context,
	values nodesettings.Values,
) error {
	if len(values) == 0 {
		return nil
	}
	if !d.isIndexer() {
		return d.writeLedgerGates(values)
	}
	epoch, slot := d.currentEpochSlot(ctx)
	if err := d.Metadata().SetNodeSettingsGates(ctx, values, epoch, slot); err != nil {
		return fmt.Errorf("failed to persist node settings gates: %w", err)
	}
	return nil
}

// NodeSettingsGates returns the persisted node settings of this database.
func (d *Database) NodeSettingsGates(
	ctx context.Context,
) (nodesettings.Values, error) {
	return d.persistedGateValues(ctx)
}

// SetNodeSettingsGates records values without evaluating them against what
// is persisted. It is for latches a migration owns, such as indexer_layout;
// configured values go through CheckNodeSettings and EnforceNodeSettings.
func (d *Database) SetNodeSettingsGates(
	ctx context.Context,
	values nodesettings.Values,
) error {
	return d.writeGateRows(ctx, values)
}

// writeGateValues persists the gates an evaluation decided to write. For an
// IndexerDB it also mirrors storage_mode and network into the node_settings
// row, which older tooling reads, in two cases: the first write ever (no
// row yet) and the first write of network (row.Network empty). The backfill
// is keyed on the row's own storage_mode, which never changes after its
// first insert; neither case can change that column, so the mirror cannot
// contradict node_settings_gate.
func (d *Database) writeGateValues(
	ctx context.Context,
	writes nodesettings.Values,
) error {
	if len(writes) == 0 {
		return nil
	}
	if !d.isIndexer() {
		return d.writeLedgerGates(writes)
	}
	row, err := d.Metadata().GetNodeSettings(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to get node settings from metadata: %w", err,
		)
	}
	switch {
	case row == nil:
		if err := d.Metadata().SetNodeSettings(ctx, &types.NodeSettings{
			StorageMode: writes["storage_mode"],
			Network:     writes["network"],
		}); err != nil {
			return fmt.Errorf("failed to persist node settings: %w", err)
		}
	case row.Network == "" && writes["network"] != "":
		if err := d.Metadata().SetNodeSettings(ctx, &types.NodeSettings{
			StorageMode: row.StorageMode,
			Network:     writes["network"],
		}); err != nil {
			return fmt.Errorf("failed to persist node settings: %w", err)
		}
	}
	return d.writeGateRows(ctx, writes)
}

// settleLedgerNodeSettings gives the ledger's node settings their home in
// the dbinfo file before anything reads them.
//
// A data directory with no file and a blob store with no identity is fresh:
// nothing can have recorded settings for it, so the metadata store is not
// asked. Its protocol parameters are written in the current unit, which the
// marker records.
//
// Otherwise the data directory is one v0.94.2 left, whose settings are in
// the metadata store; they are copied, once, into the file. v0.94.2 mints
// the blob store's identity before it writes any gate, so an identity
// without settings in the file means v0.94.2 settings in the metadata store.
// The copy is refused unless those settings name this blob store, the same
// pairing the blob_store_id gate enforces: a data directory whose dbinfo
// file was lost has no settings to pair, and a refill from the current
// configuration would bless whatever network or latches it names.
func (d *Database) settleLedgerNodeSettings(ctx context.Context) error {
	if d.isIndexer() {
		return nil
	}
	fresh := nodesettings.Values{
		nodesettings.AlonzoPParamsUnitGateName: nodesettings.AlonzoPParamsUnitWordV1,
	}
	if d.config.DataDir == "" {
		// The live UTxO set of a ledger without a data directory lives in a
		// temporary one for the process's lifetime, so its settings do too.
		d.gatesMu.Lock()
		defer d.gatesMu.Unlock()
		if d.memGates == nil {
			d.memGates = fresh
		}
		return nil
	}
	info, err := dbinfo.Read(d.config.DataDir)
	if err != nil {
		return fmt.Errorf("read node settings: %w", err)
	}
	if info.HoldsGates() {
		return nil
	}
	held, err := d.recordedBlobStoreID()
	if err != nil {
		return fmt.Errorf("get blob store id: %w", err)
	}
	if held == "" && info.MetadataPlugin == "" {
		return d.writeLedgerGates(fresh)
	}
	return d.copyReleasedNodeSettings(ctx, info, held)
}

// copyReleasedNodeSettings copies a v0.94.2 database's node settings from
// its metadata store into the dbinfo file. Other rows of the same table,
// such as the per-family state mask, stay where they are read. The rows are
// left behind as well: the ledger never reads them again, and adopting an
// API store as the IndexerDB copies them with the rest of that store.
func (d *Database) copyReleasedNodeSettings(
	ctx context.Context,
	info dbinfo.Info,
	held string,
) error {
	// The ledger's database opens no metadata store, so it holds no settings
	// to copy: a lost dbinfo file is refused below rather than dereferencing
	// the missing store.
	stored := nodesettings.Values{}
	if d.metadata != nil {
		var err error
		if stored, err = d.metadataGateValues(ctx); err != nil {
			return err
		}
	}
	values := nodesettings.Persisted(stored)
	// v0.94.2 writes the file before the gate, so a stop between the two
	// leaves the plugin recorded only here.
	if _, ok := values["metadata_plugin"]; !ok && info.MetadataPlugin != "" {
		values["metadata_plugin"] = info.MetadataPlugin
	}
	recorded := values["blob_store_id"]
	if recorded == "" && held != "" {
		return fmt.Errorf(
			"%w: the blob store has an identity but no node settings are "+
				"recorded for it: %s is missing from %q and the metadata "+
				"store holds none to move there; restore the file from a "+
				"copy of this data directory, or re-sync from scratch",
			ErrNodeSettingsMissing,
			dbinfo.FileName,
			d.config.DataDir,
		)
	}
	result := nodesettings.Evaluate(
		values,
		nodesettings.Values{"blob_store_id": held},
		map[string]bool{"blob_store_id": true},
	)
	if len(result.Mismatches) > 0 {
		return NodeSettingsError{Mismatches: mismatchStrings(result.Mismatches)}
	}
	if err := d.writeLedgerGates(values); err != nil {
		return err
	}
	d.logger.Info(
		"moved node settings out of the metadata store",
		"file", dbinfo.FileName,
		"gates", len(values),
	)
	return nil
}

// currentEpochSlot returns the tip's epoch and slot, for stamping gate
// writes with when they were recorded. It returns zeros when there is no
// tip yet, which is the normal state for a database open that precedes the
// first block being processed.
func (d *Database) currentEpochSlot(
	ctx context.Context,
) (epoch uint64, slot uint64) {
	txn := d.MetadataTxn(ctx, false)
	defer txn.Release()
	tip, err := d.GetTip(txn)
	if err != nil || tip.Point.Slot == 0 {
		return 0, 0
	}
	slot = tip.Point.Slot
	if d.metadata == nil {
		return 0, slot
	}
	ep, err := d.GetEpochBySlot(slot, txn)
	if err != nil || ep == nil {
		return 0, slot
	}
	return ep.EpochId, slot
}

// currentEpochSlotNonEmpty reports whether the database has a chain tip.
func (d *Database) currentEpochSlotNonEmpty(ctx context.Context) bool {
	txn := d.MetadataTxn(ctx, false)
	defer txn.Release()
	tip, err := d.GetTip(txn)
	return err == nil && tip.Point.Slot != 0
}

// mismatchStrings renders each Mismatch via its own String(), for the two
// NodeSettingsError sites in evaluateAndPersistGates below (the ordinary
// path and the re-evaluation after losing a first-write race).
func mismatchStrings(mismatches []nodesettings.Mismatch) []string {
	out := make([]string, 0, len(mismatches))
	for _, mismatch := range mismatches {
		out = append(out, mismatch.String())
	}
	return out
}

// evaluateAndPersistGates is the shared body of CheckNodeSettings (phase 1)
// and EnforceNodeSettings (phase 2): read what is already persisted,
// evaluate configured against it with every key in configured treated as
// explicit, fail loudly on a mismatch, and persist (then verify) whatever
// Evaluate says should be written. The two phases differ only in what
// configured contains and where it comes from -- phase 1 supplies
// d.phase1GateValues() from a bare database open, phase 2 supplies node.go's
// fully-resolved gate values -- so both callers reduce to a single call to
// this with their own configured map.
func (d *Database) evaluateAndPersistGates(ctx context.Context,
	configured nodesettings.Values,
) error {
	persisted, err := d.persistedGateValues(ctx)
	if err != nil {
		return err
	}
	// A database that predates the indexer_layout gate and already holds
	// blocks has its API rows in the consensus store. Filling the gate with
	// "split" would point the readers at an IndexerDB that does not exist.
	if configured["indexer_layout"] == "split" {
		if _, has := persisted["indexer_layout"]; !has &&
			d.currentEpochSlotNonEmpty(ctx) {
			return NodeSettingsError{Mismatches: []string{
				"indexer layout: configured \"split\" but this database " +
					"already holds chain state without an IndexerDB, and " +
					"startup migration must complete before opening the consensus store",
			}}
		}
	}
	explicit := make(map[string]bool, len(configured))
	for name := range configured {
		explicit[name] = true
	}
	result := nodesettings.Evaluate(persisted, configured, explicit)
	if len(result.Mismatches) > 0 {
		return NodeSettingsError{Mismatches: mismatchStrings(result.Mismatches)}
	}
	if len(result.Writes) == 0 {
		return nil
	}
	// A gate written here for the first time ever can race a concurrent
	// opener doing the same first write, which an unconditional upsert would
	// let silently overwrite. Only an IndexerDB on a server provider is
	// shared across processes; the ledger's settings live in its own data
	// directory. Reserve the complete first-fill set in one transaction, so
	// a losing opener re-evaluates against the winner's values.
	firstFill := make(nodesettings.Values, len(result.Writes))
	if d.isIndexer() {
		for name, value := range result.Writes {
			if _, has := persisted[name]; !has {
				firstFill[name] = value
			}
		}
	}
	if len(firstFill) > 0 {
		epoch, slot := d.currentEpochSlot(ctx)
		inserted, err := d.Metadata().InsertNodeSettingsGatesIfAbsent(ctx,
			firstFill, epoch, slot,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to reserve node settings gates: %w",
				err,
			)
		}
		if !inserted {
			persisted, err = d.persistedGateValues(ctx)
			if err != nil {
				return err
			}
			result = nodesettings.Evaluate(persisted, configured, explicit)
			if len(result.Mismatches) > 0 {
				return NodeSettingsError{
					Mismatches: mismatchStrings(result.Mismatches),
				}
			}
			if len(result.Writes) == 0 {
				return nil
			}
		}
	}
	if err := d.writeGateValues(ctx, result.Writes); err != nil {
		return err
	}
	// A write that silently dropped a gate would leave a database that
	// never enforces it; read every one back.
	persistedAfter, err := d.persistedGateValues(ctx)
	if err != nil {
		return err
	}
	for name, want := range result.Writes {
		if got := persistedAfter[name]; got != want {
			return fmt.Errorf(
				"node settings gate %q did not persist: wrote %q, read back %q",
				name,
				want,
				got,
			)
		}
	}
	d.logger.Info(
		"node settings gates recorded",
		"gates", len(result.Writes),
	)
	return nil
}

// CheckNodeSettings validates the gates a bare database open can know and
// persists them on first start. Every value it supplies is treated as
// explicit: any override against a built-in default has already happened in
// the configuration layer, so this is a strict re-validation.
func (d *Database) CheckNodeSettings(ctx context.Context) error {
	// The unit marker is settled before any first-fill gate is written, so
	// an unclassified database can never be blessed by the current
	// configuration. Reconciliation is idempotent: a stop after a rollback
	// removed the last Alonzo row but before the marker was rewritten is
	// settled on the next open.
	if err := d.ReconcileAlonzoPParamsUnitAfterRecovery(ctx); err != nil {
		return err
	}
	if err := d.checkAlonzoPParamsUnit(ctx); err != nil {
		return err
	}
	configured, err := d.phase1GateValues()
	if err != nil {
		return err
	}
	if err := d.evaluateAndPersistGates(ctx, configured); err != nil {
		return err
	}
	// The gates above pair this metadata store with its blob store; only a
	// paired store's rows are copied into it.
	if err := d.moveChainStateToBlockStore(ctx); err != nil {
		return fmt.Errorf("move chain state to the block store: %w", err)
	}
	return nil
}
