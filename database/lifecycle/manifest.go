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

// Package lifecycle implements database snapshot, restore, and truncate
// operations shared by the offline CLI and (later) a live-node code path.
// It is a pure library: it operates on an already-constructed
// *database.Database (or, for restore, plugin instances that have not yet
// been started against their real data directory) and knows nothing about
// node composition, CLI flags, or gRPC.
package lifecycle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

// ManifestFormatVersion is the current on-disk schema version of Manifest.
// Bump it when making a breaking change to the JSON shape so old restore
// tooling can reject a manifest it doesn't understand instead of silently
// misreading it. Adding the Gates field did not bump this -- see Gates'
// doc comment for why that is acceptable today.
const ManifestFormatVersion = 1

// ManifestFileName is the name of the manifest file inside a snapshot
// directory.
const ManifestFileName = "manifest.json"

// MaxManifestBytes bounds manifest inputs before JSON decoding. Manifests are
// small metadata files; accepting an arbitrarily large object here would let
// a malformed local or cloud snapshot consume memory before validation.
const MaxManifestBytes = 1 << 20

// ErrManifestCorrupted marks a manifest that was found and read but failed
// checksum validation — i.e. it (or the snapshot it belongs to) exists but
// is corrupted or was hand-edited, as distinct from a manifest that simply
// isn't there. Callers that otherwise treat a ReadManifest/ParseManifest
// error as "snapshot not found" (e.g. bark's resolveSnapshotSource probing
// whether a local copy exists) should check errors.Is against this first,
// so a corrupted snapshot is reported as corrupted rather than missing.
var ErrManifestCorrupted = errors.New(
	"manifest failed checksum validation (corrupted or hand-edited)",
)

// Trigger values recorded in Manifest.Trigger.
const (
	TriggerEpochBoundary = "epoch-boundary"
	TriggerManual        = "manual"
)

// Manifest describes a single database snapshot: what produced it, what
// point in the chain it captures, and enough about the source database's
// configuration to detect an incompatible restore before touching any
// data. It is written as JSON (not CBOR) since it is operator/tooling
// facing metadata, not chain data.
type Manifest struct {
	FormatVersion int       `json:"formatVersion"`
	CreatedAt     time.Time `json:"createdAt"`
	Trigger       string    `json:"trigger"`

	// Name/Description are operator-supplied labels, set after the fact
	// via LabelSnapshot (Snapshot itself has no caller-facing label
	// parameters) — bark's CreateSnapshot RPC is the current writer of
	// these fields. Both are empty for a snapshot that was never labeled.
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// StorageMode/Network mirror types.NodeSettings so a restore can
	// refuse an incompatible target before opening the restored store;
	// database.New's own CheckNodeSettings then re-validates them for
	// free once the restored store is opened for real.
	StorageMode string `json:"storageMode"`
	Network     string `json:"network"`

	// CommitTimestamp is the value SetCommitTimestamp wrote to both
	// stores at backup time, so database.New's checkCommitTimestamp
	// passes immediately after restore rather than reporting a
	// mismatch that was actually just "restore in progress".
	CommitTimestamp int64 `json:"commitTimestamp"`

	// Chain position captured at backup time, for CLI/gRPC display and
	// for a post-restore sanity check against the restored tip.
	TipSlot        uint64 `json:"tipSlot"`
	TipHash        []byte `json:"tipHash"`
	TipBlockNumber uint64 `json:"tipBlockNumber"`

	// BlobPlugin/MetadataPlugin identify which plugin produced the
	// backup. Restoring a badger backup into a gcs-configured store (or
	// vice versa) is meaningless, so restore refuses on a mismatch.
	BlobPlugin     string `json:"blobPlugin"`
	MetadataPlugin string `json:"metadataPlugin"`

	// Gates mirrors the source database's persisted node settings gates
	// (database/nodesettings.Gates) at backup time — network_magic,
	// start_era, the era genesis hashes, the ledger-semantics gates, and
	// so on — so CheckGateMatch can refuse restoring a snapshot whose
	// consensus-relevant configuration disagrees with what the caller
	// intends to run it with (e.g. a snapshot taken with CIP-0163
	// full-pot rewards enabled, restored onto a node configured without
	// it, would otherwise silently diverge rewards from that point
	// forward).
	//
	// This field is optional (omitempty), and its addition did not bump
	// ManifestFormatVersion, but it is not actually backward compatible:
	// checksum() covers the whole struct, so a build that predates Gates
	// ignores the unknown "gates" key on unmarshal (ParseManifest decodes
	// with a plain json.Unmarshal, not DisallowUnknownFields) and then
	// recomputes the checksum without a field it never saw, getting a
	// different digest and rejecting an otherwise-valid manifest as
	// ErrManifestCorrupted. That is acceptable only because this manifest
	// format is unreleased: no build that predates Gates is deployed
	// anywhere to hit it.
	Gates nodesettings.Values `json:"gates,omitempty"`

	// DingoVersion records the dingo build that produced the backup, so
	// a restore across incompatible dingo versions is at least detectable rather
	// than failing schema migration in a confusing way partway through.
	DingoVersion string `json:"dingoVersion"`

	// BlobBytes/MetadataBytes are informational (display, progress),
	// not required for correctness.
	BlobBytes     int64 `json:"blobBytes"`
	MetadataBytes int64 `json:"metadataBytes"`

	// Checksum is a SHA-256 hex digest of the manifest's own JSON
	// encoding with Checksum itself blanked out, guarding against a
	// corrupted or hand-edited manifest file. It is not a security
	// mechanism (no key), just a corruption/typo trap.
	Checksum string `json:"checksum"`
}

// checksum returns the manifest's checksum computed over its JSON
// encoding with the Checksum field cleared.
func (m Manifest) checksum() (string, error) {
	m.Checksum = ""
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal manifest for checksum: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// CheckPluginMatch returns an error if the manifest was produced by a
// different blob or metadata plugin than the ones given.
func (m Manifest) CheckPluginMatch(blobPlugin, metadataPlugin string) error {
	if m.BlobPlugin != blobPlugin {
		return fmt.Errorf(
			"manifest blob plugin %q does not match target %q",
			m.BlobPlugin,
			blobPlugin,
		)
	}
	if m.MetadataPlugin != metadataPlugin {
		return fmt.Errorf(
			"manifest metadata plugin %q does not match target %q",
			m.MetadataPlugin,
			metadataPlugin,
		)
	}
	return nil
}

// CheckGateMatch returns an error if any gate present in both m.Gates and
// configured is incompatible under the registry's own decision policy
// (nodesettings.Evaluate/Gate.apply) — the same policy startup enforcement
// applies via evaluateAndPersistGates — rather than raw equality. Raw
// equality was too strict for a LatchBool gate: restoring a snapshot
// recorded "off" onto a target configured "on" is exactly the one-way
// latch upgrade Evaluate permits at startup, and rejecting it here would
// make offline restore refuse a resume that a live database.New against
// the same manifest values would accept.
//
// A gate present in only one of the two maps is not an error: that is what
// lets an older snapshot (missing a gate a newer dingo would record)
// restore under that newer dingo, and a newer snapshot restore when the
// caller has no way to supply every gate it recorded (e.g. no cardano
// config loaded, so no genesis hashes). Evaluate already treats a gate
// absent from configured as skipped, and a gate absent from persisted (the
// manifest) as an ordinary first-write with no mismatch, so both
// directions of "only one side has it" fall out of Evaluate for free.
//
// blob_store_id is never compared, even when present in both maps: the
// restored blob store IS the snapshot's, so its identity always differs
// from whatever the caller had, and comparing it would fail every
// restore. metadata_plugin and blob_plugin are excluded too, since
// CheckPluginMatch already reports those — comparing them again here
// would just report the same mismatch twice. All three are filtered out of
// configured before Evaluate ever sees them, rather than skipped
// after-the-fact, so Evaluate cannot mint a Write for them either.
//
// Every gate configured supplies is marked explicit for this call, the same
// as database.New's own strict validation: CheckGateMatch is a
// compatibility check, not a resume, so OverrideEligible's "fall back to
// whatever is persisted" behavior must never mask a real mismatch here.
func (m Manifest) CheckGateMatch(configured nodesettings.Values) error {
	filtered := make(nodesettings.Values, len(configured))
	for name, want := range configured {
		switch name {
		case "blob_store_id", "metadata_plugin", "blob_plugin":
			continue
		}
		filtered[name] = want
	}
	explicit := make(map[string]bool, len(filtered))
	for name := range filtered {
		explicit[name] = true
	}
	result := nodesettings.Evaluate(m.Gates, filtered, explicit)
	if len(result.Mismatches) == 0 {
		return nil
	}
	mismatches := make([]string, 0, len(result.Mismatches))
	for _, mm := range result.Mismatches {
		mismatches = append(mismatches, fmt.Sprintf(
			"%s: manifest %q, target %q", mm.Gate, mm.Persisted, mm.Configured,
		))
	}
	// Map iteration order is random; sort so the error message is
	// deterministic across calls.
	sort.Strings(mismatches)
	return fmt.Errorf(
		"manifest gates do not match target configuration: %s",
		strings.Join(mismatches, "; "),
	)
}

// CheckCompatibility returns an error if the manifest's recorded plugins,
// storage mode, network, or gates are incompatible with the target values
// given. Offline restore call sites should call this (directly, or via
// RestoreValidated) before targetDataDir is touched in any way: unlike the
// live-node restore path, which always opens the restored copy through
// database.New using the node's own real configured plugins (so
// CheckNodeSettings catches a mismatch immediately), an offline restore
// has no such automatic check — Restore's own validateRestoredDatabase
// only opens the result using the manifest's own recorded plugins, which
// is a self-consistency check, not a check against what the caller
// actually intends to run the restored store with.
func (m Manifest) CheckCompatibility(
	blobPlugin, metadataPlugin, storageMode, network string,
	gates nodesettings.Values,
) error {
	if err := m.CheckPluginMatch(blobPlugin, metadataPlugin); err != nil {
		return err
	}
	if m.StorageMode != storageMode {
		return fmt.Errorf(
			"manifest storage mode %q does not match target %q",
			m.StorageMode,
			storageMode,
		)
	}
	if m.Network != network {
		return fmt.Errorf(
			"manifest network %q does not match target %q",
			m.Network,
			network,
		)
	}
	if err := m.CheckGateMatch(gates); err != nil {
		return err
	}
	return nil
}

// WriteManifest computes m's checksum and writes it as indented JSON to
// dir/ManifestFileName. dir must already exist.
//
// Written via a same-directory temp file plus an atomic rename, not a
// direct write to the final path: a direct write truncates any existing
// file before writing the new content, so an interruption partway
// through would leave a corrupt, partially-written manifest.json in its
// place. That matters most for LabelSnapshot, which rewrites the
// manifest of an already-complete snapshot purely to update its
// Name/Description — a truncated manifest fails ReadManifest's checksum
// validation, and catalog scanning (ListSnapshots) treats that
// identically to "this snapshot doesn't exist," silently disappearing an
// otherwise perfectly good snapshot from the catalog over what should
// have been a harmless label update. Renaming a fully-written temp file
// over the target is atomic on the same filesystem, so a reader always
// observes either the complete old manifest or the complete new one,
// never a partial one.
func WriteManifest(dir string, m Manifest) error {
	m.FormatVersion = ManifestFormatVersion
	sum, err := m.checksum()
	if err != nil {
		return err
	}
	m.Checksum = sum
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if len(data) > MaxManifestBytes {
		return fmt.Errorf(
			"manifest size %d exceeds maximum %d bytes",
			len(data), MaxManifestBytes,
		)
	}
	path := filepath.Join(dir, ManifestFileName)

	tmp, err := os.CreateTemp(dir, ManifestFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp manifest in %q: %w", dir, err)
	}
	tmpPath := tmp.Name()
	renamed := false
	// Best-effort: if anything below fails before the rename, don't leave
	// the temp file behind for a future WriteManifest/directory listing
	// to trip over. No-op once renamed, since tmpPath no longer exists.
	defer func() {
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp manifest %q: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp manifest %q: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp manifest %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp manifest to %q: %w", path, err)
	}
	renamed = true

	// A file's own fsync does not guarantee its directory entry is
	// persisted; sync dir itself so the rename above is durable too, not
	// just atomic.
	return fsyncdir.Sync(dir)
}

// ReadManifest reads and validates the manifest at dir/ManifestFileName,
// rejecting it if the checksum doesn't match its contents or if its
// format version is newer than this build understands.
func ReadManifest(dir string) (Manifest, error) {
	path := filepath.Join(dir, ManifestFileName)
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest %q: %w", path, err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxManifestBytes+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest %q: %w", path, err)
	}
	if len(data) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf(
			"read manifest %q: size exceeds maximum %d bytes",
			path, MaxManifestBytes,
		)
	}
	m, err := ParseManifest(data)
	if err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}

// ParseManifest validates and decodes a manifest already read into memory
// — the same validation ReadManifest performs against a local file, but
// against bytes fetched however the caller obtained them (e.g. a cloud
// destination's ListSnapshots fetching a single remote manifest.json
// object without downloading the whole snapshot).
func ParseManifest(data []byte) (Manifest, error) {
	if len(data) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf(
			"manifest size %d exceeds maximum %d bytes",
			len(data), MaxManifestBytes,
		)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest: %w", err)
	}
	if m.FormatVersion > ManifestFormatVersion {
		return Manifest{}, fmt.Errorf(
			"manifest has format version %d, newest supported is %d",
			m.FormatVersion,
			ManifestFormatVersion,
		)
	}
	wantSum := m.Checksum
	gotSum, err := m.checksum()
	if err != nil {
		return Manifest{}, err
	}
	if wantSum == "" || gotSum != wantSum {
		return Manifest{}, ErrManifestCorrupted
	}
	return m, nil
}

// LabelSnapshot sets name/description on the manifest at dir and rewrites
// it (recomputing its checksum), without touching the snapshot's blob or
// metadata backups. Intended for callers that only learn a human-readable
// label after Snapshot has already produced dir (e.g. bark's
// CreateSnapshot RPC receives name/description in the same request but
// Snapshot itself has no such parameters).
func LabelSnapshot(dir string, name string, description string) error {
	m, err := ReadManifest(dir)
	if err != nil {
		return err
	}
	m.Name = name
	m.Description = description
	return WriteManifest(dir, m)
}
