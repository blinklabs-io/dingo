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
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
)

// CommitTimestampError contains the timestamps of the metadata and blob stores
type CommitTimestampError struct {
	MetadataTimestamp int64
	BlobTimestamp     int64
}

// Error returns the stringified error
func (e CommitTimestampError) Error() string {
	return fmt.Sprintf(
		"commit timestamp mismatch: %d (metadata) != %d (blob)",
		e.MetadataTimestamp,
		e.BlobTimestamp,
	)
}

func (b *Database) checkCommitTimestamp() error {
	// Get value from metadata
	metadataTimestamp, metadataErr := b.Metadata().GetCommitTimestamp()
	if metadataErr != nil {
		return fmt.Errorf(
			"failed to get metadata timestamp from plugin: %w",
			metadataErr,
		)
	}
	// Get value from blob
	blobTimestamp, blobErr := b.Blob().GetCommitTimestamp()
	if blobErr != nil {
		return fmt.Errorf(
			"failed to get blob timestamp from plugin: %w",
			blobErr,
		)
	}
	// Compare values
	if blobTimestamp != metadataTimestamp {
		return CommitTimestampError{
			MetadataTimestamp: metadataTimestamp,
			BlobTimestamp:     blobTimestamp,
		}
	}
	return nil
}

func (b *Database) updateCommitTimestamp(txn *Txn, timestamp int64) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	// Update metadata
	metaTxn := txn.Metadata()
	if err := b.Metadata().SetCommitTimestamp(timestamp, metaTxn); err != nil {
		return err
	}
	// Update blob
	blobTxn := txn.Blob()
	if err := b.Blob().SetCommitTimestamp(timestamp, blobTxn); err != nil {
		return err
	}
	return nil
}

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
	// MetadataPlugin is only ever set by a full caller -- the same signal
	// writeDBInfoSidecar uses below -- so its presence is what tells the
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

// persistedGateValues merges the legacy node_settings row with the
// node_settings_gate table. Order is load-bearing: the legacy row is
// loaded first and node_settings_gate is copied on top of it (last write
// wins), because node_settings_gate is authoritative for every gate
// including storage_mode and network. SetNodeSettings's row is immutable
// after its first insert -- InsertNodeSettings is ON CONFLICT DO NOTHING
// (a deliberate MySQL-equivalent no-op UPDATE), and the only other query
// touching that table, BackfillNodeSettingsNetwork, only ever fills the
// network column once and never touches storage_mode -- so once a gate has
// moved past whatever it was on first insert, only node_settings_gate
// reflects the current value; a database created before
// node_settings_gate existed simply has no rows there yet, and the legacy
// row is all this function has for storage_mode/network until the first
// write after this change records them into the gate table too.
// Reversing this order (copying the legacy row on top of the gate table)
// would let that stale legacy column shadow a correctly-latched gate
// value forever, which is exactly the bug this ordering fixes.
func (d *Database) persistedGateValues() (nodesettings.Values, error) {
	legacy, err := d.Metadata().GetNodeSettings()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get node settings from metadata: %w", err,
		)
	}
	gates, err := d.Metadata().GetNodeSettingsGates()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get node settings gates from metadata: %w", err,
		)
	}
	values := make(nodesettings.Values, len(gates)+2)
	if legacy != nil {
		if legacy.StorageMode != "" {
			values["storage_mode"] = legacy.StorageMode
		}
		if legacy.Network != "" {
			values["network"] = legacy.Network
		}
	}
	maps.Copy(values, gates)
	return values, nil
}

// writeGateValues persists every gate in writes to node_settings_gate,
// which is authoritative for all of them including storage_mode and
// network -- see persistedGateValues's doc comment for why -- and is
// written to unconditionally below, regardless of what happens to the
// legacy mirror. It also opportunistically mirrors into the legacy
// node_settings row, purely so older tooling that still reads
// node_settings directly keeps seeing a sensible value, in two cases:
//
//   - The very first write this database ever makes (legacy == nil): seeds
//     the row so one exists at all.
//   - A later write that fills network for the first time
//     (legacy.Network == ""): SetNodeSettings's backfill query is `UPDATE
//     node_settings SET network = ? WHERE id = 1 AND storage_mode = ? AND
//     network = <empty string>`, so the match key it is given must be
//     whatever is physically already in the row's storage_mode column --
//     legacy.StorageMode, just read above -- not the new value
//     node_settings_gate is about to record for the storage_mode gate.
//     Using the new value here is exactly the bug an earlier version of
//     this function had: once storage_mode has latched away from whatever
//     the row was first inserted with, the row's own storage_mode column
//     never changes again (SetNodeSettings's insert is ON CONFLICT DO
//     NOTHING), so a backfill keyed on the new value would never match and
//     would silently no-op.
//
// Neither branch can ever change the row's storage_mode column -- the insert
// is a no-op once a row exists, and the only UPDATE this store has only
// ever touches network -- so this mirror can never contradict
// node_settings_gate, which remains the only thing persistedGateValues
// treats as authoritative for storage_mode.
//
// requireSidecar says that the metadata-plugin gate was not present before
// this enforcement pass. In that case the sidecar is still needed to protect
// the next pre-open provider selection, including for databases created before
// node_settings_gate existed.
func (d *Database) writeGateValues(
	writes nodesettings.Values,
	requireSidecar bool,
) error {
	if len(writes) == 0 {
		return nil
	}
	legacy, err := d.Metadata().GetNodeSettings()
	if err != nil {
		return fmt.Errorf(
			"failed to get node settings from metadata: %w", err,
		)
	}
	switch {
	case legacy == nil:
		if err := d.Metadata().SetNodeSettings(&types.NodeSettings{
			StorageMode: writes["storage_mode"],
			Network:     writes["network"],
		}); err != nil {
			return fmt.Errorf("failed to persist node settings: %w", err)
		}
	case legacy.Network == "" && writes["network"] != "":
		if err := d.Metadata().SetNodeSettings(&types.NodeSettings{
			StorageMode: legacy.StorageMode,
			Network:     writes["network"],
		}); err != nil {
			return fmt.Errorf("failed to persist node settings: %w", err)
		}
	}
	epoch, slot := d.currentEpochSlot()
	if err := d.Metadata().SetNodeSettingsGates(writes, epoch, slot); err != nil {
		return fmt.Errorf("failed to persist node settings gates: %w", err)
	}
	if requireSidecar {
		if err := d.writeDBInfoSidecarErr(); err != nil {
			return fmt.Errorf(
				"failed to establish dbinfo sidecar for unprotected database: %w",
				err,
			)
		}
	} else {
		d.writeDBInfoSidecar()
	}
	return nil
}

// writeDBInfoSidecar writes the dbinfo sidecar recording which metadata
// plugin produced this database, alongside the metadata_plugin gate write
// above: the gate row and the sidecar record the same fact, one inside the
// database and one outside it. internal/settingsresolve's pre-open check
// reads this file to identify the correct plugin to open before it has
// opened anything -- without a real writer, that check would be dead code
// on every real run. This is a best-effort convenience once the
// metadata_plugin gate is already latched in node_settings_gate. During the
// first enforcement pass that can establish that gate, writeGateValues uses
// writeDBInfoSidecarErr and fails closed if the sidecar cannot be established.
//
// Two guards apply:
//
//   - d.config.MetadataPlugin must be non-empty. Partial callers
//     (mithril/sync.go, database/lifecycle/restore.go) construct a Config
//     with only DataDir, Logger, StorageMode, and Network, so
//     MetadataPlugin is zero-value empty on those paths; writing
//     {"metadataPlugin":""} from one of them would poison the pre-open
//     check for every later, complete start against the same directory.
//   - The sidecar must be absent. A sidecar that is already present and
//     names a different plugin means the pre-open check either already
//     failed or was never reached on this path (a tool that skips
//     settingsresolve entirely); silently overwriting it here would erase
//     the exact signal it exists to carry. Absent means a first start or an
//     operator who deleted it, and both cases want it written.
func (d *Database) writeDBInfoSidecar() {
	if err := d.writeDBInfoSidecarErr(); err != nil {
		d.logger.Warn("failed to write dbinfo sidecar", "error", err)
	}
}

// writeDBInfoSidecarErr is writeDBInfoSidecar's error-returning body. See
// that function's doc comment for the two guards and the fatal/non-fatal
// split between callers.
func (d *Database) writeDBInfoSidecarErr() error {
	if d.config.MetadataPlugin == "" {
		return nil
	}
	existing, err := dbinfo.Read(d.config.DataDir)
	if err != nil {
		return fmt.Errorf("read dbinfo sidecar: %w", err)
	}
	if existing.MetadataPlugin != "" {
		// Already present: never overwrite (see doc comment above).
		return nil
	}
	if err := dbinfo.Write(d.config.DataDir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: d.config.MetadataPlugin,
	}); err != nil {
		return fmt.Errorf("write dbinfo sidecar: %w", err)
	}
	return nil
}

// currentEpochSlot returns the tip's epoch and slot, for stamping gate
// writes with when they were recorded. It returns zeros when there is no
// tip yet, which is the normal state for a database open that precedes the
// first block being processed.
func (d *Database) currentEpochSlot() (epoch uint64, slot uint64) {
	tip, err := d.GetTip(nil)
	if err != nil || tip.Point.Slot == 0 {
		return 0, 0
	}
	slot = tip.Point.Slot
	ep, err := d.GetEpochBySlot(slot, nil)
	if err != nil || ep == nil {
		return 0, slot
	}
	return ep.EpochId, slot
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
func (d *Database) evaluateAndPersistGates(
	configured nodesettings.Values,
) error {
	persisted, err := d.persistedGateValues()
	if err != nil {
		return err
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
		// No gate needs writing, but the dbinfo sidecar is a separate,
		// independently-deletable file: an operator who removes it on an
		// otherwise steady-state database must still get it back, or the
		// pre-open metadata-plugin check in internal/settingsresolve stays
		// silently disabled from here on (see writeDBInfoSidecar's doc
		// comment for the two guards that make this a no-op everywhere it
		// should be).
		d.writeDBInfoSidecar()
		return nil
	}
	requireSidecar := d.config.MetadataPlugin != ""
	if requireSidecar {
		_, hasMetadataPluginGate := persisted["metadata_plugin"]
		requireSidecar = !hasMetadataPluginGate
	}
	// A gate written here for the first time ever (absent from persisted)
	// can race against a concurrent opener doing the same first-ever
	// write: two openers both see nothing persisted, both Evaluate against
	// their own configured values with no mismatch (there is nothing yet
	// to disagree with), and an unconditional upsert would let whichever
	// one commits last silently overwrite the other's value with no
	// record a collision even happened. This is reachable in practice only
	// when the metadata plugin is shared across processes by design
	// (postgres, mysql, both dingo_extra_plugins-gated): sqlite is opened
	// per-process, and the default blob plugin, badger, takes an exclusive
	// process lock that already rules out two full opens of the same
	// database at once regardless of metadata plugin. Reserve each
	// first-ever name with a conditional insert before the plain upsert
	// below, so a losing opener discovers the winner's value instead of
	// blindly overwriting it.
	firstFill := make(nodesettings.Values, len(result.Writes))
	for name, value := range result.Writes {
		if _, has := persisted[name]; !has {
			firstFill[name] = value
		}
	}
	if len(firstFill) > 0 {
		epoch, slot := d.currentEpochSlot()
		lostRace := false
		for name, value := range firstFill {
			inserted, err := d.Metadata().InsertNodeSettingsGateIfAbsent(
				name, value, epoch, slot,
			)
			if err != nil {
				return fmt.Errorf(
					"failed to reserve node settings gate %q: %w",
					name,
					err,
				)
			}
			if !inserted {
				lostRace = true
			}
		}
		if lostRace {
			// Another opener's first write to at least one of these names
			// landed before ours. Re-evaluate configured against what is
			// now actually persisted -- exactly what a genuinely
			// sequential second start would do -- rather than trusting the
			// persisted map read at the top of this function, which is now
			// stale for at least the reserved names.
			persisted, err = d.persistedGateValues()
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
				d.writeDBInfoSidecar()
				return nil
			}
		}
	}
	if err := d.writeGateValues(result.Writes, requireSidecar); err != nil {
		return err
	}
	// Verify every write actually landed rather than trusting the store
	// call succeeded silently: node_settings' immutable-after-first-insert
	// row previously made exactly this kind of write silently no-op (see
	// writeGateValues's doc comment), so this turns any future write path
	// that can drop a gate the same way into a loud startup failure instead
	// of a database that quietly never enforces it.
	persistedAfter, err := d.persistedGateValues()
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
//
// It is normally called once, by New (via init), and callers do not invoke
// it directly on that path. It is exported so node.go can re-invoke it after
// a commit-timestamp recovery: New returns before ever calling this when
// checkCommitTimestamp fails, so a startup that takes the recovery path
// never runs phase 1 on its own -- see node.go's dbNeedsRecovery handling,
// which calls this explicitly once RecoverCommitTimestampConflict succeeds.
func (d *Database) CheckNodeSettings() error {
	configured, err := d.phase1GateValues()
	if err != nil {
		return err
	}
	return d.evaluateAndPersistGates(configured)
}
