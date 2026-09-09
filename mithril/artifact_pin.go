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

package mithril

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
)

// syncKeyPinnedArtifact records the Mithril artifact a sync run bound the
// database to, written at the first database mutation and read by the next run
// so an interrupted import resumes against the same artifact instead of
// whatever the aggregator has published since.
//
// It is part of the ephemeral sync lifecycle: ClearSyncState (run on sync
// completion) wipes it, so its presence means "a sync run is mid-flight against
// this artifact" and never survives a completed sync. That is the opposite of
// syncKeyImmutableMax, which is deliberately written after the completion clear.
//
// Why the pin has to exist at all: a fresh bootstrap imports ledger state with
// Reconcile disabled, and the metadata import is insert-if-absent
// (Store.ImportUtxos and the account/pool/DRep phases). Importing a second,
// newer snapshot's live set over a first snapshot's partially imported rows
// therefore leaves the union of the two — every UTxO created before the first
// artifact's slot and spent before the second's stays live, and every account,
// pool and DRep the second snapshot no longer carries stays active — with no
// reconcile pass and no divergence check to remove them. The ledger-state phase
// checkpoints cannot catch this either: they are keyed by "{digest}:{slot}", so
// a different artifact simply looks like a fresh import.
const syncKeyPinnedArtifact = "mithril_pinned_artifact"

// pinnedArtifact is the durable identity of the Mithril artifact an in-flight
// sync run is importing. Digest is the v2 artifact hash or the v1 snapshot
// digest; Backend distinguishes the two because the identifiers are not
// interchangeable across the aggregator's artifact endpoints.
type pinnedArtifact struct {
	Backend             string `json:"backend"`
	Network             string `json:"network"`
	Digest              string `json:"digest"`
	Epoch               uint64 `json:"epoch"`
	ImmutableFileNumber uint64 `json:"immutable_file_number"`
	CertificateHash     string `json:"certificate_hash"`
	// CertifiedTipSlot is the ImmutableDB tip the certificate covers. It is
	// zero until the bootstrap has opened the certified ImmutableDB, and is
	// filled in before the ledger-state import runs, so a resume can detect an
	// artifact that re-resolved to different content.
	CertifiedTipSlot    uint64 `json:"certified_tip_slot"`
	CertifiedTipSlotSet bool   `json:"certified_tip_slot_set"`
}

// setPinnedArtifact records pin as the artifact this sync run is importing.
func setPinnedArtifact(db *database.Database, pin pinnedArtifact) error {
	if pin.Digest == "" {
		return errors.New(
			"pinning mithril artifact: artifact has no digest",
		)
	}
	encoded, err := json.Marshal(pin)
	if err != nil {
		return fmt.Errorf("encoding mithril artifact pin: %w", err)
	}
	if err := db.SetSyncState(
		syncKeyPinnedArtifact, string(encoded), nil,
	); err != nil {
		return fmt.Errorf("pinning mithril artifact: %w", err)
	}
	return nil
}

// getPinnedArtifact returns the artifact an interrupted sync run pinned. ok is
// false when no run is mid-flight against a pinned artifact — a completed sync
// (ClearSyncState wiped it) or a database whose in-progress marker predates
// artifact pinning.
func getPinnedArtifact(
	db *database.Database,
) (pin pinnedArtifact, ok bool, err error) {
	val, err := db.GetSyncState(syncKeyPinnedArtifact, nil)
	if err != nil {
		return pinnedArtifact{}, false, fmt.Errorf(
			"reading mithril artifact pin: %w", err,
		)
	}
	if val == "" {
		return pinnedArtifact{}, false, nil
	}
	if err := json.Unmarshal([]byte(val), &pin); err != nil {
		return pinnedArtifact{}, false, fmt.Errorf(
			"parsing mithril artifact pin %q: %w", val, err,
		)
	}
	if pin.Digest == "" {
		return pinnedArtifact{}, false, fmt.Errorf(
			"mithril artifact pin %q has no digest", val,
		)
	}
	return pin, true, nil
}

// clearPinnedArtifact removes the pin. Used on the Sync paths that return
// without importing anything, so a pin never outlives the run that wrote it on
// a database that stays complete.
func clearPinnedArtifact(db *database.Database) error {
	if err := db.DeleteSyncState(syncKeyPinnedArtifact, nil); err != nil {
		return fmt.Errorf("clearing mithril artifact pin: %w", err)
	}
	return nil
}

// recordPinnedCertifiedTip stores the certified ImmutableDB tip slot on the
// existing pin. It is a no-op when no pin is present, which is the v1/no-pin
// path rather than an error.
func recordPinnedCertifiedTip(db *database.Database, slot uint64) error {
	pin, ok, err := getPinnedArtifact(db)
	if err != nil || !ok {
		return err
	}
	if pin.CertifiedTipSlotSet && pin.CertifiedTipSlot == slot {
		return nil
	}
	pin.CertifiedTipSlot = slot
	pin.CertifiedTipSlotSet = true
	return setPinnedArtifact(db, pin)
}

// validateForRun rejects a pin that cannot describe this run's target: a pin
// written by the other artifact backend, or for another network. Resuming
// across either would re-import a different snapshot's live set over the
// partially imported one.
func (p pinnedArtifact) validateForRun(backend, network string) error {
	if p.Backend != "" && p.Backend != normalizeBackend(backend) {
		return fmt.Errorf(
			"interrupted Mithril sync pinned artifact %s on the %s backend, "+
				"but this run selects the %s backend; re-run with the "+
				"original backend, or remove the database directory and "+
				"bootstrap again",
			p.Digest, p.Backend, normalizeBackend(backend),
		)
	}
	if p.Network != "" && network != "" && p.Network != network {
		return fmt.Errorf(
			"interrupted Mithril sync pinned artifact %s on network %s, "+
				"but this run selects network %s; remove the database "+
				"directory and bootstrap again",
			p.Digest, p.Network, network,
		)
	}
	return nil
}

// verifyResolvedArtifact checks the artifact a resumed run re-resolved from the
// aggregator against the pin. A digest that no longer names the same beacon (or
// a different certificate) means the aggregator republished under the pinned
// identity, which the resume cannot reconcile with the partially imported rows.
func (p pinnedArtifact) verifyResolved(snapshot *SnapshotListItem) error {
	if snapshot == nil {
		return fmt.Errorf(
			"resuming pinned Mithril artifact %s: aggregator returned no "+
				"artifact",
			p.Digest,
		)
	}
	if snapshot.Digest != p.Digest {
		return fmt.Errorf(
			"resuming pinned Mithril artifact %s: aggregator returned "+
				"artifact %s instead",
			p.Digest, snapshot.Digest,
		)
	}
	if snapshot.Beacon.Epoch != p.Epoch ||
		snapshot.Beacon.ImmutableFileNumber != p.ImmutableFileNumber {
		return fmt.Errorf(
			"resuming pinned Mithril artifact %s: beacon moved from "+
				"epoch %d immutable file %d to epoch %d immutable file %d; "+
				"remove the database directory and bootstrap again",
			p.Digest,
			p.Epoch, p.ImmutableFileNumber,
			snapshot.Beacon.Epoch, snapshot.Beacon.ImmutableFileNumber,
		)
	}
	if p.CertificateHash != "" && snapshot.CertificateHash != p.CertificateHash {
		return fmt.Errorf(
			"resuming pinned Mithril artifact %s: certificate changed",
			p.Digest,
		)
	}
	return nil
}

// verifyCertifiedTip checks the certified ImmutableDB tip a resumed run
// produced against the tip recorded when the interrupted run reached the same
// point. A pin with no recorded tip (the interrupted run stopped before the
// certified ImmutableDB was opened) accepts any tip and is filled in by the
// caller.
func (p pinnedArtifact) verifyCertifiedTip(slot uint64) error {
	if !p.CertifiedTipSlotSet || p.CertifiedTipSlot == slot {
		return nil
	}
	return fmt.Errorf(
		"resuming pinned Mithril artifact %s: certified ImmutableDB tip "+
			"slot %d does not match the %d recorded before the interruption; "+
			"the extracted snapshot cache no longer matches the pinned "+
			"artifact — remove the database directory and bootstrap again",
		p.Digest, slot, p.CertifiedTipSlot,
	)
}

// errNoArtifactPin is returned when an interrupted sync run left no artifact
// pin. It is not recoverable in-process: the artifact the interrupted run was
// importing cannot be identified, so no artifact can be safely imported over
// its partial rows.
var errNoArtifactPin = errors.New(
	"interrupted Mithril sync did not record which artifact it was " +
		"importing (in-progress marker written by a build without artifact " +
		"pinning); resuming would import a newer snapshot's ledger state " +
		"over the partially imported one and leave both snapshots' live " +
		"UTxOs, accounts and pools in the database. Remove the database " +
		"directory and run 'dingo mithril sync' again",
)
