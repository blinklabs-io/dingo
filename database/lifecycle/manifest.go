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
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
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
//
// Version 2 added the signed snapshot ID. A version 1 manifest carries no ID,
// so a keyed verification rejects it; see ErrManifestLegacyFormat.
const ManifestFormatVersion = 2

// ManifestFileName is the name of the manifest file inside a snapshot
// directory.
const ManifestFileName = "manifest.json"

// MaxManifestBytes is the default bound before JSON decoding. Manifests are
// small metadata files; accepting an arbitrarily large object here would let
// a malformed local or cloud snapshot consume memory before validation.
const MaxManifestBytes = 1 << 20

// ErrManifestTooLarge marks a manifest that exceeds its encoded byte budget.
// Callers can distinguish this resource rejection from missing or corrupt data.
var ErrManifestTooLarge = errors.New("manifest size limit exceeded")

// ManifestOption configures snapshot and manifest operations. Each option is
// consumed only by the operations that use it.
type ManifestOption func(*manifestConfig)

type manifestConfig struct {
	maxBytes int64
	key      []byte
	// id is the snapshot ID a manifest must carry when verified under a key.
	id string
	// maxPause is consumed only by Snapshot and SnapshotToCloud.
	maxPause time.Duration
	// pauseNow, pauseContext, and pauseDeadline are per-call test seams
	// consumed only by Snapshot. Production callers cannot construct options
	// that set them.
	pauseNow      func() time.Time
	pauseContext  func(context.Context) context.Context
	pauseDeadline func(context.Context, time.Duration) (context.Context, context.CancelFunc)
	// catalogReconcile restores provider-backed rows after a damaged local
	// catalog has been rebuilt.
	catalogReconcile func(context.Context, string) error
}

func withSnapshotCatalogReconcile(
	reconcile func(context.Context, string) error,
) ManifestOption {
	return func(cfg *manifestConfig) { cfg.catalogReconcile = reconcile }
}

func snapshotCatalogReconcile(
	opts []ManifestOption,
) func(context.Context, string) error {
	cfg := manifestConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg.catalogReconcile
}

// WithManifestKey sets the operator trust root: a shared secret that
// WriteManifest uses to authenticate a manifest, and that ParseManifest,
// ReadManifest and Manifest.Authenticate require a manifest to verify against.
// A manifest carries the digests of its payload files, so authenticating it
// authenticates them. An empty key is the same as passing no option.
func WithManifestKey(key []byte) ManifestOption {
	return func(cfg *manifestConfig) { cfg.key = key }
}

// WithManifestID sets the snapshot ID (local directory name or cloud object
// prefix segment) that a keyed verification requires the manifest to carry.
// ReadManifest defaults it to the base name of the directory it reads, so
// this is needed only where the manifest is read from a location whose name
// is not the snapshot ID, such as a temporary download directory or a cached
// catalog row. An empty id is the same as passing no option.
func WithManifestID(id string) ManifestOption {
	return func(cfg *manifestConfig) {
		if id != "" {
			cfg.id = id
		}
	}
}

func manifestID(opts []ManifestOption) string {
	cfg := manifestConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg.id
}

// snapshotIDForDir returns the snapshot ID a snapshot directory is addressed
// by: its base name, without resolving symlinks, so a link to another
// snapshot is addressed by the link's name.
func snapshotIDForDir(dir string) string {
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	return filepath.Base(dir)
}

// cloudSnapshotIDFromURI returns the snapshot ID a cloud snapshot URI
// addresses: the last segment of the canonical path ParseCloudDestination
// hands the destination factory, which JoinCloudURI makes the snapshot ID.
// Deriving it from the raw path instead would let "prefix/newer/." read the
// objects under prefix/newer while naming no ID. It returns "" when the URI
// is invalid or names no path segment.
func cloudSnapshotIDFromURI(uri string) string {
	u, err := parseCloudDestinationURL(uri)
	if err != nil {
		return ""
	}
	id := path.Base(u.Path)
	if id == "." || id == "/" {
		return ""
	}
	return id
}

// withCloudManifestID returns opts plus the snapshot ID uri addresses as the
// ID a keyed verification requires. Under a key it refuses a URI naming no
// snapshot ID: snapshots are always written one segment below a destination,
// and accepting any signed manifest at the bucket root would let a writer to
// the bucket substitute one there.
func withCloudManifestID(
	uri string,
	opts []ManifestOption,
) ([]ManifestOption, error) {
	id := cloudSnapshotIDFromURI(uri)
	if id == "" && len(manifestKey(opts)) > 0 {
		return nil, fmt.Errorf(
			"%w: %w: cloud URI %q names no snapshot ID",
			ErrManifestUnauthenticated, ErrManifestIDMismatch,
			CloudDestinationDisplay(uri),
		)
	}
	return append(
		append([]ManifestOption(nil), opts...),
		WithManifestID(id),
	), nil
}

func manifestKey(opts []ManifestOption) []byte {
	cfg := manifestConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg.key
}

func requireManifestKey(opts []ManifestOption) error {
	if len(manifestKey(opts)) == 0 {
		return fmt.Errorf(
			"%w: cloud snapshots require a manifest trust key",
			ErrManifestUnauthenticated,
		)
	}
	return nil
}

// WithManifestMaxBytes sets the maximum encoded manifest size. Zero uses
// MaxManifestBytes (1 MiB); negative values are rejected before I/O.
func WithManifestMaxBytes(maxBytes int64) ManifestOption {
	return func(cfg *manifestConfig) { cfg.maxBytes = maxBytes }
}

func manifestByteLimit(opts []ManifestOption) (int64, error) {
	cfg := manifestConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.maxBytes < 0 {
		return 0, errors.New("manifest maximum must be >= 0")
	}
	if cfg.maxBytes == 0 {
		return MaxManifestBytes, nil
	}
	return cfg.maxBytes, nil
}

// readManifestData applies the byte limit before allocating or decoding a
// complete input. Probe separately so MaxInt64 does not overflow at limit+1.
func readManifestData(r io.Reader, opts []ManifestOption) ([]byte, error) {
	limit, err := manifestByteLimit(opts)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(r, limit))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) == limit {
		var probe [1]byte
		n, probeErr := io.ReadFull(r, probe[:])
		if n > 0 {
			return nil, fmt.Errorf("manifest size exceeds maximum %d bytes: %w", limit, ErrManifestTooLarge)
		}
		if !errors.Is(probeErr, io.EOF) {
			return nil, probeErr
		}
	}
	return data, nil
}

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

// ErrManifestUnauthenticated marks a manifest whose authentication tag is
// missing or does not verify under the configured trust key. Unlike
// ErrManifestCorrupted, which an unkeyed checksum can only trap by accident,
// this means the manifest was not produced by a holder of the key.
var ErrManifestUnauthenticated = errors.New(
	"manifest is not authenticated by the configured trust key",
)

// ErrManifestLegacyFormat marks a manifest written before snapshot IDs were
// signed (format version 1). It carries no ID the trust key authenticates, so
// it cannot be bound to the directory or object prefix it was found under;
// the snapshot must be re-created. It is returned only by keyed verification,
// and always joined with ErrManifestUnauthenticated.
var ErrManifestLegacyFormat = errors.New(
	"snapshot predates signed snapshot IDs and must be re-created",
)

// ErrManifestIDMismatch marks an authenticated manifest whose signed snapshot
// ID differs from the directory name or object prefix it was found under,
// which is how a complete older snapshot copied under a newer ID presents.
// It is always joined with ErrManifestUnauthenticated.
var ErrManifestIDMismatch = errors.New(
	"manifest snapshot ID does not match the requested snapshot ID",
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
	FormatVersion int `json:"formatVersion"`

	// ID is the snapshot ID, the base name of the snapshot directory or cloud
	// prefix the snapshot was written to. It is covered by Authentication, so
	// a keyed verification can bind the manifest to the ID it is requested by.
	// Empty in a format version 1 manifest.
	ID string `json:"id,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
	Trigger   string    `json:"trigger"`

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

	// BlobSHA256/MetadataSHA256 are the hex SHA-256 digests of the blob and
	// metadata backup files (BlobBackupFileName, MetadataBackupFileName). With
	// BlobBytes/MetadataBytes they declare the only objects a restore will
	// fetch and the sizes it will accept. Empty in a manifest written before
	// they existed.
	BlobSHA256     string `json:"blobSha256,omitempty"`
	MetadataSHA256 string `json:"metadataSha256,omitempty"`

	// Authentication is the hex HMAC-SHA256, under the operator trust key
	// (WithManifestKey), of the manifest's JSON encoding with Checksum and
	// Authentication blanked. Empty when no key was configured at snapshot time.
	// Like Gates, its addition did not bump ManifestFormatVersion: a build
	// that predates these fields recomputes Checksum without them and rejects
	// such a manifest as corrupted.
	Authentication string `json:"authentication,omitempty"`

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

// authentication returns the HMAC-SHA256 of m's JSON encoding, with Checksum
// and Authentication cleared, under key.
func (m Manifest) authentication(key []byte) (string, error) {
	m.Checksum = ""
	m.Authentication = ""
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal manifest for authentication: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// Authenticate verifies m against the trust key in opts. It returns nil when
// no key is configured, since there is then nothing to verify against, and
// ErrManifestUnauthenticated when m's tag is absent or does not match.
// Under a key it also rejects a format version 1 manifest
// (ErrManifestLegacyFormat) and, when WithManifestID names the requested
// snapshot, a manifest whose signed ID differs (ErrManifestIDMismatch).
func (m Manifest) Authenticate(opts ...ManifestOption) error {
	key := manifestKey(opts)
	if len(key) == 0 {
		return nil
	}
	if m.FormatVersion < 2 {
		return fmt.Errorf(
			"%w: %w (format version %d)",
			ErrManifestUnauthenticated, ErrManifestLegacyFormat,
			m.FormatVersion,
		)
	}
	want, err := m.authentication(key)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(want), []byte(m.Authentication)) {
		return ErrManifestUnauthenticated
	}
	if id := manifestID(opts); id != "" && m.ID != id {
		return fmt.Errorf(
			"%w: %w: manifest signed for %q, requested %q",
			ErrManifestUnauthenticated, ErrManifestIDMismatch, m.ID, id,
		)
	}
	return nil
}

// authenticateEntry verifies a catalog or listing entry against the trust key,
// requiring the manifest to carry the entry's own ID.
func authenticateEntry(entry SnapshotEntry, opts []ManifestOption) error {
	return entry.Manifest.Authenticate(
		append(
			append([]ManifestOption(nil), opts...),
			WithManifestID(entry.ID),
		)...,
	)
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
func WriteManifest(dir string, m Manifest, opts ...ManifestOption) error {
	return writeManifest(context.Background(), dir, m, opts...)
}

func writeManifest(
	ctx context.Context,
	dir string,
	m Manifest,
	opts ...ManifestOption,
) error {
	m, data, err := prepareManifest(dir, m, opts...)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, ManifestFileName)

	tmp, err := os.CreateTemp(dir, ManifestFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp manifest in %q: %w", dir, err)
	}
	tmpPath := tmp.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := writeAndSyncManifest(tmp, data, tmpPath); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp manifest to %q: %w", path, err)
	}
	renamed = true

	if err := fsyncdir.Sync(dir); err != nil {
		return err
	}
	return publishManifestToCatalog(ctx, dir, m, opts...)
}

func prepareManifest(
	dir string,
	m Manifest,
	opts ...ManifestOption,
) (Manifest, []byte, error) {
	limit, err := manifestByteLimit(opts)
	if err != nil {
		return Manifest{}, nil, err
	}
	m.FormatVersion = ManifestFormatVersion
	m.ID = snapshotIDForDir(dir)
	m.Authentication = ""
	if key := manifestKey(opts); len(key) > 0 {
		tag, err := m.authentication(key)
		if err != nil {
			return Manifest{}, nil, err
		}
		m.Authentication = tag
	}
	sum, err := m.checksum()
	if err != nil {
		return Manifest{}, nil, err
	}
	m.Checksum = sum
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("marshal manifest: %w", err)
	}
	if int64(len(data)) > limit {
		return Manifest{}, nil, fmt.Errorf(
			"manifest size %d exceeds maximum %d bytes: %w",
			len(data), limit, ErrManifestTooLarge,
		)
	}
	return m, data, nil
}

func writeManifestAtRoot(
	ctx context.Context,
	root *os.Root,
	dir string,
	m Manifest,
	opts ...ManifestOption,
) (Manifest, error) {
	m, data, err := prepareManifest(dir, m, opts...)
	if err != nil {
		return Manifest{}, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return Manifest{}, fmt.Errorf("choose temp manifest name: %w", err)
	}
	tmpName := ManifestFileName + ".tmp-" + hex.EncodeToString(random[:])
	tmp, err := root.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Manifest{}, fmt.Errorf("create temp manifest: %w", err)
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = root.Remove(tmpName)
		}
	}()
	if err := writeAndSyncManifest(tmp, data, tmpName); err != nil {
		return Manifest{}, err
	}
	if err := root.Rename(tmpName, ManifestFileName); err != nil {
		return Manifest{}, fmt.Errorf("publish manifest: %w", err)
	}
	renamed = true
	rootDir, err := root.Open(".")
	if err != nil {
		return Manifest{}, fmt.Errorf("open snapshot directory for sync: %w", err)
	}
	syncErr := rootDir.Sync()
	closeErr := rootDir.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return Manifest{}, fmt.Errorf("sync snapshot directory: %w", err)
	}
	if err := snapshotRootMatchesPath(root, dir); err != nil {
		return Manifest{}, err
	}
	if err := publishManifestToCatalog(ctx, dir, m, opts...); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func writeAndSyncManifest(file *os.File, data []byte, name string) error {
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write temp manifest %q: %w", name, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync temp manifest %q: %w", name, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp manifest %q: %w", name, err)
	}
	return nil
}

func publishManifestToCatalog(
	ctx context.Context,
	dir string,
	m Manifest,
	opts ...ManifestOption,
) error {
	if err := updateSnapshotCatalogIfPresent(
		ctx,
		filepath.Dir(dir),
		SnapshotEntry{ID: filepath.Base(dir), Manifest: m},
		opts...,
	); err != nil {
		return fmt.Errorf("%w: %w", ErrSnapshotCatalogUpdate, err)
	}
	return nil
}

// ReadManifest reads and validates the manifest at dir/ManifestFileName,
// rejecting it if the checksum doesn't match its contents or if its
// format version is newer than this build understands.
func ReadManifest(dir string, opts ...ManifestOption) (Manifest, error) {
	if _, err := manifestByteLimit(opts); err != nil {
		return Manifest{}, err
	}
	path := filepath.Join(dir, ManifestFileName)
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest %q: %w", path, err)
	}
	defer file.Close()
	data, err := readManifestData(file, opts)
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest %q: %w", path, err)
	}
	// Explicit options come last so a caller-supplied ID wins the default.
	idOpts := append(
		[]ManifestOption{WithManifestID(snapshotIDForDir(dir))},
		opts...,
	)
	m, err := ParseManifest(data, idOpts...)
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
func ParseManifest(data []byte, opts ...ManifestOption) (Manifest, error) {
	limit, err := manifestByteLimit(opts)
	if err != nil {
		return Manifest{}, err
	}
	if int64(len(data)) > limit {
		return Manifest{}, fmt.Errorf(
			"manifest size %d exceeds maximum %d bytes: %w",
			len(data), limit, ErrManifestTooLarge,
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
	if err := m.Authenticate(opts...); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// LabelSnapshot sets name/description on the manifest at dir and rewrites
// it (recomputing its checksum), without touching the snapshot's blob or
// metadata backups. Intended for callers that only learn a human-readable
// label after Snapshot has already produced dir (e.g. bark's
// CreateSnapshot RPC receives name/description in the same request but
// Snapshot itself has no such parameters).
func LabelSnapshot(dir string, name string, description string, opts ...ManifestOption) error {
	return labelSnapshot(
		context.Background(), dir, name, description, opts...,
	)
}

func labelSnapshot(
	ctx context.Context,
	dir string,
	name string,
	description string,
	opts ...ManifestOption,
) error {
	m, err := ReadManifest(dir, opts...)
	if err != nil {
		return err
	}
	m.Name = name
	m.Description = description
	return writeManifest(ctx, dir, m, opts...)
}
