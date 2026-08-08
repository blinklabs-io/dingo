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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrUnsafeSnapshotDigest reports an aggregator-supplied digest that cannot be
// used to name a directory inside the download directory.
var ErrUnsafeSnapshotDigest = errors.New("unsafe snapshot digest")

// validateSnapshotDigest refuses a digest that would not stay one element deep
// inside the download directory.
//
// The digest is the aggregator's string and v1 has nothing to check it
// against — no computed hash, the way v2 recomputes an artifact's — yet it
// names two directories, `immutable-<digest>` and `ancillary-<digest>`, that
// get extracted into and later removed with os.RemoveAll. Joining it raw does
// not keep it inside: a leading separator makes the `immutable-` prefix its own
// path element, and a following `..` then pops it, so `/../..` names the
// download directory's grandparent.
//
// Refused rather than reduced. A digest is an identifier, and one that is not a
// single path element is not a digest — reducing it would silently give two
// different snapshots the same cache key, which is how a stale extraction gets
// reused for the wrong artifact.
func validateSnapshotDigest(digest string) error {
	if digest == "" {
		return fmt.Errorf("%w: snapshot has no digest", ErrUnsafeSnapshotDigest)
	}
	if digest == "." || digest == ".." ||
		strings.ContainsRune(digest, '/') ||
		strings.ContainsRune(digest, '\\') ||
		strings.ContainsRune(digest, filepath.Separator) {
		return fmt.Errorf(
			"%w: %q is not a single path element",
			ErrUnsafeSnapshotDigest, digest,
		)
	}
	return nil
}

// Mithril artifact backends.
const (
	// BackendV1 restores from the legacy full-database snapshot
	// archives (CardanoImmutableFilesFull, /artifact/snapshots).
	// Upstream Mithril is phasing this artifact type out.
	BackendV1 = "v1"
	// BackendV2 restores from incremental Cardano database artifacts
	// (CardanoDatabase, /artifact/cardano-database): per-immutable
	// archives verified against the certified merkle root.
	BackendV2 = "v2"
)

// AcceptedBackends returns the recognized Mithril artifact backends.
// It is the single source for backend-name validation: cmd/dingo's
// resolveMithrilBackend derives from it, and internal/config's
// validation whitelist (which cannot import this package) verifies
// parity against it.
func AcceptedBackends() []string {
	return []string{BackendV1, BackendV2}
}

// BootstrapConfig holds configuration for the Mithril bootstrap
// process.
type BootstrapConfig struct {
	// Network is the Cardano network name (e.g., "mainnet",
	// "preprod", "preview").
	Network string
	// Backend selects the Mithril artifact backend: BackendV1
	// downloads the legacy full-database tarball, BackendV2 restores
	// per-immutable archives verified against the certified merkle
	// root. Empty selects BackendV2.
	Backend string
	// AggregatorURL overrides the default aggregator URL for the
	// network. If empty, the default URL for the network is used.
	AggregatorURL string
	// DownloadDir is the directory where the snapshot archive will
	// be downloaded. If empty, a temporary directory is created.
	DownloadDir string
	// CleanupAfterLoad controls whether temporary files are removed
	// after loading completes.
	CleanupAfterLoad bool
	// VerifyCertificateChain enables certificate chain verification
	// against the aggregator. When true, the bootstrap process
	// walks the certificate chain from the snapshot back to the
	// genesis certificate to verify the chain is unbroken.
	VerifyCertificateChain bool
	// GenesisVerificationKey is the Mithril genesis verification key loaded
	// from Cardano network config. It is validated for parseability now and
	// will be used by full STM verification.
	GenesisVerificationKey string
	// AncillaryVerificationKey is the Mithril ancillary verification key loaded
	// from Cardano network config. It is validated for parseability now and
	// will be used when ancillary artifacts are verified cryptographically.
	AncillaryVerificationKey string
	// Logger is used for structured logging.
	Logger *slog.Logger
	// OnProgress is called during download with progress updates.
	OnProgress ProgressFunc
	// DownloadIdleTimeout is the maximum time to wait for download
	// response headers or body bytes before retrying. Zero uses the
	// downloader default; negative disables idle detection.
	DownloadIdleTimeout time.Duration
	// DownloadMaxIdleRetries is the number of consecutive idle retries
	// allowed without additional bytes. Zero uses the downloader default.
	DownloadMaxIdleRetries int
	// DownloadMaxTransientRetries is the maximum number of retry attempts
	// for transient network errors (TLS handshake failures, connection
	// resets, HTTP 429, HTTP 5xx) per download. Zero uses the downloader
	// default. Negative disables transient retries.
	DownloadMaxTransientRetries int
	// httpClient, when set, is the shared keep-alive client the v2
	// immutable-download pool reuses across all archive fetches so
	// connections are pooled instead of re-handshaked per file. It is set
	// internally by downloadImmutables on a by-value copy of the config;
	// callers do not populate it.
	httpClient *http.Client
	// StartImmutable is the lowest immutable file number to download and
	// extract. Files below it are assumed already present (a Mithril v2
	// catch-up sets this to the immutable-import marker so it only fetches the
	// archives missing from the existing blob store). Zero downloads the full
	// 0..N range, the normal bootstrap behaviour.
	StartImmutable uint64
	// OnChunkContiguous, when set, enables download<->processing
	// pipelining: chunks are fetched in parallel (out of order) but this
	// callback is invoked for each immutable file number in strict
	// contiguous order as soon as that prefix is fully downloaded. The order
	// runs from StartImmutable upwards, not from zero — a catch-up leaves
	// everything below the marker to the blob store this run is adding to, so
	// those archives are neither downloaded nor extracted here and the files
	// need not exist. It lets the caller copy blocks into the blob store while
	// later chunks are still downloading. When nil, downloads run to
	// completion before any processing (legacy behaviour).
	// The callback runs on a single consumer goroutine and serializes
	// processing, so it needs no internal locking.
	OnChunkContiguous func(chunk ContiguousChunk) error
}

// ContiguousChunk describes the contiguous immutable prefix a pipelined
// bootstrap has finished downloading, verifying and extracting.
type ContiguousChunk struct {
	// Dir is the directory the chunks are extracted into. It is the name the
	// directory was vetted under, for messages; read through Root.
	Dir string
	// Root is the open handle extraction writes through. Read through it
	// rather than through Dir: the name can be repointed while the download
	// runs and the handle cannot.
	Root *os.Root
	// Digests is the certified SHA-256 of every file the download covers,
	// keyed by the name beneath Dir. The handle settles which directory is
	// read and these settle which bytes, which is the half a handle cannot
	// carry — see BootstrapResult.ImmutableDigests.
	Digests map[string]string
	// Start is the lowest immutable file number this run covers, from
	// BootstrapConfig.StartImmutable. Anything below it was left to the blob
	// store the run is adding to and is not in Dir.
	Start uint64
	// Num is the highest immutable file number whose trio is complete. Every
	// number in [Start, Num] has been downloaded, verified and extracted;
	// below Start, nothing has, which is why the range is given rather than
	// implied. Chunks are addressed by number — a number is not a position in
	// Dir's listing unless Start is zero.
	Num uint64
}

// VerificationMode selects the level of Mithril certificate verification.
type VerificationMode uint8

const (
	// VerificationModeStructural verifies certificate chain linkage and leaf
	// binding to the requested snapshot digest.
	VerificationModeStructural VerificationMode = iota + 1
	// VerificationModeSTM verifies the structural certificate chain and the
	// aggregate multi-signature of each non-genesis certificate.
	VerificationModeSTM
)

// CertificateChainVerificationResult captures the parsed certificate chain and
// derived leaf/root metadata that higher verification modes can build on.
type CertificateChainVerificationResult struct {
	Certificates       []*Certificate
	LeafCertificate    *Certificate
	GenesisCertificate *Certificate
	SignedEntityKind   string
	SnapshotDigest     string
}

func normalizeBackend(backend string) string {
	if backend == "" {
		return BackendV2
	}
	return backend
}

// BootstrapResult contains the result of a bootstrap operation.
type BootstrapResult struct {
	// Snapshot is the snapshot that was downloaded and extracted.
	Snapshot *SnapshotListItem
	// ImmutableDir is the path to the extracted ImmutableDB
	// directory. It is the name the directory was vetted under; use
	// ImmutableRoot to read it.
	ImmutableDir string
	// ImmutableRoot is an open handle on ImmutableDir, held from the moment
	// the directory was vetted until Cleanup closes it.
	//
	// The load path opens the ImmutableDB through this handle rather than
	// through ImmutableDir. The directory sits in a download area, so between
	// vetting it and reading it a concurrent writer could put a different tree
	// at that name; resolving the name at load time would then read the
	// replacement, with the vetting having been about something else. The
	// handle refers to the directory that was vetted, and keeps referring to it
	// however the name is repointed.
	//
	// Both bootstrap paths set it. A result without it did not come from a
	// vetted lookup, and loading refuses rather than falling back to the
	// pathname — a silent fallback would reinstate exactly the open this
	// replaces.
	ImmutableRoot *os.Root
	// ImmutableDigests is the certified SHA-256 of every file in ImmutableDir,
	// keyed by the name beneath it ("00000.chunk"). Set by the v2 backend,
	// which downloads each immutable trio against a digest list covered by the
	// certificate's merkle root; nil for v1, which certifies one archive
	// rather than the files inside it and so has nothing to re-check against.
	//
	// It is carried beside ImmutableRoot because the two answer different
	// questions and both have to be answered. The handle says the load reads
	// the directory that was vetted. It says nothing about the files in it: a
	// writer who shares the download directory can rename a file of their own
	// over a verified one without ever leaving the directory the handle refers
	// to. The digests are what let the load refuse those bytes, checked from
	// the descriptor the read goes through rather than from a name reopened
	// after the check.
	ImmutableDigests map[string]string
	// ExtractDir is the root directory where the archive was
	// extracted. Contains db/immutable/, db/ledger/, etc.
	ExtractDir string
	// AncillaryDir is the root directory where the ancillary
	// archive was extracted. Contains ledger/<slot>/{meta,state,
	// tables/tvar}. Empty if no ancillary data was downloaded.
	AncillaryDir string
	// AncillaryRoot is an open handle on AncillaryDir, on the same terms as
	// ImmutableRoot: the ledger-state import discovers and reads through it, so
	// the tree the signed manifest was checked against is the tree that gets
	// loaded. Nil when no ancillary data was obtained.
	AncillaryRoot *os.Root
	// AncillaryVerified reports that the ancillary tree's contents were checked
	// against the signed ancillary manifest (a verified v2 bootstrap).
	//
	// The ledger-state import will not look past a verified tree that yields no
	// state. Falling through would move the import from a tree covered by a
	// signature to one that is not, and an attacker who can empty the first can
	// then choose the second. Where nothing was verified there is no such
	// downgrade, and the fallback stays available — v1 keeps its ledger state
	// in the main archive, so looking there is how that layout works at all.
	AncillaryVerified bool
	// AncillaryDigests is the signed ancillary manifest's digest map: every
	// file the ancillary key vouched for, keyed by its slash-separated path
	// under AncillaryDir. Set only when AncillaryVerified is true.
	//
	// The manifest check hashes each file and closes it; the import opens the
	// state and table it selects afterwards. Between those two the tree is the
	// same tree, but a file in it need not be the same file — so the map
	// travels with the handle and the selected files are checked again from
	// the descriptors the import reads through, before anything is parsed.
	AncillaryDigests map[string]string
	// ExtractRoot is an open handle on ExtractDir.
	//
	// The ledger-state import falls back to the main extraction directory when
	// the ancillary archive carried no ledger state (v1 snapshots keep it in
	// db/ledger). ExtractDir is derived inside the download directory like
	// everything else here, so that fallback reads through a handle too rather
	// than resolving a name nothing vetted.
	ExtractRoot *os.Root
	// AncillaryArchivePath is the path to the downloaded ancillary
	// archive file. Empty if no ancillary data was downloaded.
	AncillaryArchivePath string
	// ArchivePath is the path to the downloaded archive file.
	ArchivePath string
	// TempDir is the auto-created temporary directory that holds
	// all downloaded and extracted files. Set only when
	// BootstrapConfig.DownloadDir was empty. Cleanup() removes it
	// after removing its children.
	TempDir string
}

// Bootstrap orchestrates the full Mithril bootstrap flow:
//  1. Fetch the latest snapshot from the aggregator
//  2. Download the snapshot archive
//  3. Extract the archive to obtain the ImmutableDB files
//  4. Return the path for loading with existing immutable DB logic
//
// The caller is responsible for invoking the immutable DB load using
// the returned ImmutableDir path. If CleanupAfterLoad is true, the
// caller should call Cleanup() on the result after loading.
func Bootstrap(
	ctx context.Context,
	cfg BootstrapConfig,
) (*BootstrapResult, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cfg.Backend = normalizeBackend(cfg.Backend)
	if cfg.VerifyCertificateChain {
		if cfg.GenesisVerificationKey != "" {
			if _, err := ParseVerificationKey(cfg.GenesisVerificationKey); err != nil {
				return nil, fmt.Errorf(
					"parsing Mithril genesis verification key: %w",
					err,
				)
			}
		}
		if cfg.AncillaryVerificationKey != "" {
			if _, err := ParseVerificationKey(cfg.AncillaryVerificationKey); err != nil {
				return nil, fmt.Errorf(
					"parsing Mithril ancillary verification key: %w",
					err,
				)
			}
		}
	}

	// Resolve aggregator URL
	aggregatorURL := cfg.AggregatorURL
	if aggregatorURL == "" {
		var err error
		aggregatorURL, err = AggregatorURLForNetwork(cfg.Network)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving aggregator URL: %w",
				err,
			)
		}
	}

	cfg.Logger.Info(
		"starting Mithril bootstrap",
		"component", "mithril",
		"network", cfg.Network,
		"aggregator", aggregatorURL,
		"backend", cfg.Backend,
	)

	// Dispatch to the selected artifact backend. The remainder of
	// this function implements the legacy v1 (full snapshot
	// archive) flow.
	switch cfg.Backend {
	case BackendV1:
		// Continue with the legacy v1 flow below
	case BackendV2:
		return bootstrapV2(ctx, cfg, aggregatorURL)
	default:
		return nil, fmt.Errorf(
			"unsupported Mithril backend %q (expected %q or %q)",
			cfg.Backend,
			BackendV1,
			BackendV2,
		)
	}

	// Step 1: Fetch latest snapshot
	client := NewClient(aggregatorURL)
	snapshot, err := client.GetLatestSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"fetching latest snapshot: %w",
			err,
		)
	}
	if cfg.Network != "" && snapshot.Network != "" &&
		cfg.Network != snapshot.Network {
		return nil, fmt.Errorf(
			"mithril snapshot network mismatch: requested=%s snapshot=%s",
			cfg.Network,
			snapshot.Network,
		)
	}
	// Before anything is derived from it. The digest names the extraction and
	// ancillary directories, so a refusal that came after the first join would
	// already have said where they are. v2 gets this from recomputing the
	// artifact hash, which constrains it to hex; v1 has no such check.
	if err := validateSnapshotDigest(snapshot.Digest); err != nil {
		return nil, err
	}

	cfg.Logger.Info(
		"found latest snapshot",
		"component", "mithril",
		"digest", snapshot.Digest,
		"epoch", snapshot.Beacon.Epoch,
		"immutable_file_number", snapshot.Beacon.ImmutableFileNumber,
		"size", snapshot.Size,
	)

	if len(snapshot.Locations) == 0 {
		return nil, fmt.Errorf(
			"snapshot %s has no download locations",
			snapshot.Digest,
		)
	}

	// Step 1b: Verify certificate chain (optional)
	if cfg.VerifyCertificateChain {
		if snapshot.CertificateHash == "" {
			return nil, fmt.Errorf(
				"certificate chain verification requested "+
					"but snapshot %s has no certificate hash",
				snapshot.Digest,
			)
		}
		cfg.Logger.Info(
			"verifying certificate chain",
			"component", "mithril",
			"certificate_hash", snapshot.CertificateHash,
		)
		verificationMode := VerificationModeStructural
		if cfg.GenesisVerificationKey != "" {
			verificationMode = VerificationModeSTM
		}
		verificationResult, err := VerifyCertificateChainWithMode(
			ctx,
			client,
			snapshot.CertificateHash,
			snapshot.Digest,
			verificationMode,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"certificate chain verification failed: %w",
				err,
			)
		}
		if cfg.GenesisVerificationKey != "" {
			if verificationResult == nil ||
				verificationResult.GenesisCertificate == nil {
				return nil, errors.New(
					"genesis verification key provided but no genesis certificate found in chain",
				)
			}
			if err := VerifyGenesisCertificateSignature(
				verificationResult.GenesisCertificate,
				cfg.GenesisVerificationKey,
			); err != nil {
				return nil, fmt.Errorf(
					"genesis certificate verification failed: %w",
					err,
				)
			}
		}
		verificationMaterial, err := BuildVerificationMaterial(
			ctx,
			client,
			verificationResult,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"building verification material failed: %w",
				err,
			)
		}
		if err := ValidateVerificationMaterial(verificationMaterial); err != nil {
			return nil, fmt.Errorf(
				"verification material validation failed: %w",
				err,
			)
		}
		if verificationResult.SignedEntityKind !=
			signedEntityTypeCardanoImmutableFilesFull {
			return nil, fmt.Errorf(
				"unexpected signed entity kind for snapshot bootstrap: %s",
				verificationResult.SignedEntityKind,
			)
		}
		if snapshot.Network != "" &&
			(verificationResult.LeafCertificate.Metadata.Network == "" ||
				verificationResult.LeafCertificate.Metadata.Network != snapshot.Network) {
			return nil, fmt.Errorf(
				"certificate network mismatch: certificate=%s snapshot=%s",
				verificationResult.LeafCertificate.Metadata.Network,
				snapshot.Network,
			)
		}
		if beacon := verificationResult.LeafCertificate.SignedEntityType.
			CardanoImmutableFilesFull(); beacon != nil {
			if beacon.Epoch != snapshot.Beacon.Epoch ||
				beacon.ImmutableFileNumber != snapshot.Beacon.ImmutableFileNumber {
				return nil, fmt.Errorf(
					"signed entity beacon mismatch: certificate=(epoch=%d, immutable=%d) snapshot=(epoch=%d, immutable=%d)",
					beacon.Epoch,
					beacon.ImmutableFileNumber,
					snapshot.Beacon.Epoch,
					snapshot.Beacon.ImmutableFileNumber,
				)
			}
		}
		cfg.Logger.Info(
			"certificate chain verified",
			"component", "mithril",
		)
		// The legacy v1 artifact has no signed ancillary manifest. It can
		// authenticate its immutable tarball, but not the ledger state that
		// would be imported with it, so it cannot satisfy the verified fast
		// bootstrap trust boundary.
		return nil, errors.New(
			"verified Mithril v1 bootstrap is unsupported because it has no signed ancillary state; use the v2 backend",
		)
	}

	// Step 2: Set up download directory
	downloadDir := cfg.DownloadDir
	createdTempDir := false
	if downloadDir == "" {
		var err error
		downloadDir, err = os.MkdirTemp("", "dingo-mithril-*")
		if err != nil {
			return nil, fmt.Errorf(
				"creating temp directory: %w",
				err,
			)
		}
		createdTempDir = true
	}
	// Clean up temp dir on error to avoid leaking disk space
	success := false
	defer func() {
		if !success && createdTempDir {
			os.RemoveAll(downloadDir)
		}
	}()

	// Step 3: Download snapshot archive (skip if already complete)
	archiveFilename := fmt.Sprintf(
		"%s-%s.tar.zst",
		snapshot.Network,
		truncateDigest(snapshot.Digest),
	)
	archivePath := filepath.Join(downloadDir, archiveFilename)
	snapshotCacheKey := snapshot.Digest

	if isFileComplete(archivePath, snapshot.Size) {
		cfg.Logger.Info(
			"snapshot archive already downloaded, skipping",
			"component", "mithril",
			"path", archivePath,
		)
	} else {
		var dlErr error
		for i, loc := range snapshot.Locations {
			archivePath, dlErr = DownloadSnapshot(
				ctx, DownloadConfig{
					URL:                 loc,
					DestDir:             downloadDir,
					Filename:            archiveFilename,
					ExpectedSize:        snapshot.Size,
					Logger:              cfg.Logger,
					OnProgress:          cfg.OnProgress,
					IdleTimeout:         cfg.DownloadIdleTimeout,
					MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
					MaxTransientRetries: cfg.DownloadMaxTransientRetries,
				},
			)
			if dlErr == nil {
				break
			}
			cfg.Logger.Warn(
				"download location failed, trying next",
				"component", "mithril",
				"location", i+1,
				"total", len(snapshot.Locations),
				"error", dlErr,
			)
		}
		if dlErr != nil {
			return nil, fmt.Errorf(
				"downloading snapshot (all %d locations failed): %w",
				len(snapshot.Locations),
				dlErr,
			)
		}
	}

	// Steps 4+5: Extract main archive and download ancillary in
	// parallel. These write to separate directories (immutable/
	// vs ancillary/) so they are independent.
	extractDir := filepath.Join(
		downloadDir,
		"immutable-"+snapshotCacheKey,
	)
	var ancillaryTree *vettedDir
	var ancillaryArchivePath string
	// Closed only when the result that would own it is never returned; on
	// success it is carried to the ledger-state import and released by Cleanup.
	defer func() {
		if !success {
			ancillaryTree.Close()
		}
	}()

	// Launch ancillary download concurrently (non-fatal if it fails).
	// Always wait for the goroutine before returning, even on error,
	// to prevent goroutine leaks and races with temp dir cleanup.
	// Use a derived context so the goroutine is promptly cancelled
	// if the main extraction fails. Defers execute in LIFO order:
	// ancCancel (registered last) runs first, signalling the
	// goroutine to stop, then ancWg.Wait blocks until it exits.
	ancCtx, ancCancel := context.WithCancel(ctx)
	var ancWg sync.WaitGroup
	defer ancWg.Wait()
	defer ancCancel()
	if len(snapshot.AncillaryLocations) > 0 {
		ancWg.Go(func() {
			candidateDir := filepath.Join(
				downloadDir,
				"ancillary-"+snapshotCacheKey,
			)
			if cached := ledgerDir(candidateDir); cached != nil {
				cfg.Logger.Info(
					"ancillary data already "+
						"extracted, skipping",
					"component", "mithril",
					"path", cached.Path(),
				)
				ancillaryTree = cached
				// Only set archive path if the file still
				// exists (it may have been cleaned up after
				// a prior successful extraction).
				candidateArchive := filepath.Join(
					downloadDir,
					fmt.Sprintf(
						"%s-%s-ancillary.tar.zst",
						snapshot.Network,
						truncateDigest(
							snapshot.Digest,
						),
					),
				)
				if _, err := os.Stat(candidateArchive); err == nil {
					ancillaryArchivePath = candidateArchive
				}
				return
			}
			tree, archPath, ancErr := downloadAncillary(
				ancCtx, cfg, snapshot, downloadDir,
			)
			// Recorded whether or not the tree turned out usable, so a
			// downloaded archive still gets cleaned up.
			if archPath != "" {
				ancillaryArchivePath = archPath
			}
			if ancErr != nil {
				cfg.Logger.Warn(
					"failed to download ancillary "+
						"data, continuing without "+
						"ledger state",
					"component", "mithril",
					"error", ancErr,
				)
				return
			}
			// The handle the tree was vetted through, carried across
			// rather than reopened from its name here.
			ancillaryTree = tree
		})
	}

	// Step 4: Extract main archive (skip if already extracted)
	//
	// One handle on the extraction directory, and both trees taken from it: the
	// immutable DB below it, and — when the ancillary archive carries no ledger
	// state — the ledger state below it too. Vetting the directory twice would
	// be two resolutions of one name, so the ImmutableDB that was accepted and
	// the ledger state that gets imported could come from different trees.
	//
	// Both handles are held from here until Cleanup, and closed on every error
	// path below, since the result that would own them is never returned.
	var extractTree *os.Root
	var immutableTree *vettedDir
	defer func() {
		if !success {
			immutableTree.Close()
			if extractTree != nil {
				_ = extractTree.Close()
			}
		}
	}()

	// Absent on a first run, which is not an error — it means no cached tree.
	if extractTree, err = openVerifiedDir(extractDir); err == nil {
		immutableTree = findImmutableDirIn(extractTree, extractDir)
	}
	if immutableTree != nil {
		cfg.Logger.Info(
			"snapshot already extracted, skipping",
			"component", "mithril",
			"immutable_dir", immutableTree.Path(),
		)
	} else {
		// Reopened after extracting, not before: publication renames a staging
		// directory onto extractDir, so a handle taken beforehand would refer
		// to the directory that rename replaced.
		if extractTree != nil {
			_ = extractTree.Close()
			extractTree = nil
		}
		// Replace: a previous run may have left a partial extraction
		// here, which the lookup above did not accept.
		_, err = ExtractArchive(
			ctx, archivePath, extractDir, cfg.Logger,
			WithReplaceDestination(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"extracting snapshot archive: %w",
				err,
			)
		}

		if extractTree, err = openVerifiedDir(extractDir); err != nil {
			return nil, fmt.Errorf(
				"verifying extraction directory %s: %w", extractDir, err,
			)
		}
		immutableTree = findImmutableDirIn(extractTree, extractDir)
		if immutableTree == nil {
			return nil, fmt.Errorf(
				"immutable DB directory not found in "+
					"extracted archive at %s",
				extractDir,
			)
		}
	}

	// Wait for ancillary download to finish (also deferred above
	// for the error-return path; calling Wait twice is safe).
	ancWg.Wait()

	cfg.Logger.Info(
		"Mithril bootstrap ready for loading",
		"component", "mithril",
		"immutable_dir", immutableTree.Path(),
		"ancillary_dir", ancillaryTree.Path(),
	)

	success = true
	result := &BootstrapResult{
		Snapshot:             snapshot,
		ImmutableDir:         immutableTree.Path(),
		ImmutableRoot:        immutableTree.Root(),
		ExtractDir:           extractDir,
		ExtractRoot:          extractTree,
		AncillaryDir:         ancillaryTree.Path(),
		AncillaryRoot:        ancillaryTree.Root(),
		ArchivePath:          archivePath,
		AncillaryArchivePath: ancillaryArchivePath,
	}
	if createdTempDir {
		result.TempDir = downloadDir
	}
	return result, nil
}

// downloadAncillary downloads and extracts the ancillary archive
// which contains the ledger state in UTxO-HD format. It returns the extracted
// tree as the handle it was vetted through.
//
// v1 has no ancillary manifest, so nothing here is signature-bound the way the
// v2 path is. The handle is still what is returned rather than the directory's
// name: a function that vets a tree, drops the handle and hands back a name
// invites the caller to reopen it and believe the check applies to what they
// reopened — which is exactly how the v2 path went wrong once. The caller
// closes the result.
func downloadAncillary(
	ctx context.Context,
	cfg BootstrapConfig,
	snapshot *SnapshotListItem,
	downloadDir string,
) (tree *vettedDir, archivePath string, err error) {
	if len(snapshot.AncillaryLocations) == 0 {
		return nil, "", errors.New(
			"snapshot has no ancillary locations",
		)
	}

	cfg.Logger.Info(
		"downloading ancillary data (ledger state)",
		"component", "mithril",
		"size", snapshot.AncillarySize,
	)

	ancillaryFilename := fmt.Sprintf(
		"%s-%s-ancillary.tar.zst",
		snapshot.Network,
		truncateDigest(snapshot.Digest),
	)

	// Where the download lands whether or not it completes: DownloadSnapshot
	// resumes, so a failed attempt deliberately leaves a partial file here.
	// Reporting the path with every error below is what lets Cleanup remove it
	// — otherwise a failed ancillary download leaves a file behind in an
	// operator-supplied download directory, which no temp-dir removal sweeps.
	//
	// Asked of the downloader rather than assembled here. The filename carries
	// the network name, which comes from the aggregator, and the downloader
	// reduces it to its last element before writing. Joining it raw would name
	// a different file for a network like "../../etc" — one outside the
	// download directory, which Cleanup would then remove.
	ancillaryDest := downloadDestinationPath(DownloadConfig{
		DestDir:  downloadDir,
		Filename: ancillaryFilename,
	})

	var ancillaryPath string
	for i, loc := range snapshot.AncillaryLocations {
		ancillaryPath, err = DownloadSnapshot(
			ctx, DownloadConfig{
				URL:                 loc,
				DestDir:             downloadDir,
				Filename:            ancillaryFilename,
				ExpectedSize:        snapshot.AncillarySize,
				Logger:              cfg.Logger,
				OnProgress:          cfg.OnProgress,
				IdleTimeout:         cfg.DownloadIdleTimeout,
				MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
				MaxTransientRetries: cfg.DownloadMaxTransientRetries,
			},
		)
		if err == nil {
			break
		}
		cfg.Logger.Warn(
			"ancillary download location failed, "+
				"trying next",
			"component", "mithril",
			"location", i+1,
			"total", len(snapshot.AncillaryLocations),
			"error", err,
		)
	}
	if err != nil {
		return nil, ancillaryDest, fmt.Errorf(
			"downloading ancillary archive "+
				"(all %d locations failed): %w",
			len(snapshot.AncillaryLocations),
			err,
		)
	}

	ancillaryDir := filepath.Join(
		downloadDir,
		"ancillary-"+snapshot.Digest,
	)
	// Replace: the ancillary directory is keyed by digest, so a stale
	// copy from an interrupted run may already be present.
	if _, extractErr := ExtractArchive(
		ctx, ancillaryPath, ancillaryDir, cfg.Logger,
		WithReplaceDestination(),
	); extractErr != nil {
		return nil, ancillaryPath, fmt.Errorf(
			"extracting ancillary archive: %w",
			extractErr,
		)
	}

	extracted := ledgerDir(ancillaryDir)
	if extracted == nil {
		// Removed here or not at all: Cleanup takes AncillaryDir from the
		// returned handle, and this path has no handle to return. The archive
		// path goes back even so, since removing the extraction is not
		// removing the archive it came from.
		os.RemoveAll(ancillaryDir)
		return nil, ancillaryPath, fmt.Errorf(
			"extracted ancillary data at %s holds no ledger state",
			ancillaryDir,
		)
	}

	cfg.Logger.Info(
		"ancillary data extracted",
		"component", "mithril",
		"path", extracted.Path(),
	)

	return extracted, ancillaryPath, nil
}

// Cleanup removes the temporary files created during bootstrap.
// It removes the archive, extract directory, and ancillary
// directory individually rather than the entire parent directory,
// to avoid deleting user-specified download directories.
func (r *BootstrapResult) Cleanup(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	// Released before the directories go, so Windows — which refuses to remove
	// a directory with an open handle beneath it — can remove the tree.
	r.CloseHandles()
	paths := []string{
		r.ArchivePath,
		r.ExtractDir,
		r.AncillaryArchivePath,
		r.AncillaryDir,
	}
	for _, p := range paths {
		if p == "" {
			continue
		}
		if err := os.RemoveAll(p); err != nil {
			logger.Warn(
				"failed to clean up Mithril temp file",
				"component", "mithril",
				"path", p,
				"error", err,
			)
		} else {
			logger.Info(
				"cleaned up Mithril temp file",
				"component", "mithril",
				"path", p,
			)
		}
	}
	// Remove auto-created temp directory and any remaining contents
	// (e.g. a partially-extracted ancillary archive).
	if r.TempDir != "" {
		if err := os.RemoveAll(r.TempDir); err != nil {
			logger.Warn(
				"failed to remove Mithril temp directory",
				"component", "mithril",
				"path", r.TempDir,
				"error", err,
			)
		} else {
			logger.Info(
				"removed Mithril temp directory",
				"component", "mithril",
				"path", r.TempDir,
			)
		}
	}
}

// CloseHandles releases the directory handles the result carries. It is
// idempotent, and callers that do not clean up (CleanupAfterLoad off, which
// keeps the extracted tree for a later run) still owe this call once loading is
// done — the handles are descriptors held for the lifetime of the result.
//
// Not safe to call while another goroutine reads the handle fields: it clears
// them. Call it once the work that reads them has finished — in Sync that is
// after the import errgroup is joined.
func (r *BootstrapResult) CloseHandles() {
	if r == nil {
		return
	}
	for _, root := range []**os.Root{
		&r.ImmutableRoot,
		&r.AncillaryRoot,
		&r.ExtractRoot,
	} {
		if *root != nil {
			_ = (*root).Close()
			*root = nil
		}
	}
}

// findImmutableDirIn is findImmutableDir with the extraction directory already
// open, so that every read — the layout enumeration as much as the per-
// candidate checks — resolves through the one handle that was vetted.
//
// root stays the caller's. When the extraction directory is itself the answer,
// the returned vettedDir gets its own handle on it, derived from this one
// through the open descriptor rather than by resolving the name a second time.
// That is what lets a caller hold one vetted extraction handle and give both
// the immutable lookup and the ledger-state fallback a share of it.
//
// Taking the handle as a parameter is also what lets a test place a directory
// swap between the open and the reads, which is the window the enumeration and
// the returned path were exposed to.
func findImmutableDirIn(root *os.Root, extractDir string) *vettedDir {
	if hasChunkFilesIn(root, ".") {
		self, err := root.OpenRoot(".")
		if err != nil {
			return nil
		}
		return vetted(self, extractDir, ".")
	}
	chunkDir := func(rel string) *vettedDir {
		if err := assertNoSymlinkComponents(root, rel); err != nil {
			return nil
		}
		// The candidate is opened once, then read and handed on through that
		// one handle. Resolving rel again for each step would be a fresh
		// chance for it to be something other than what the previous step saw,
		// and the answer would then be about whichever tree held the name last.
		candidate, err := openVerifiedRoot(root, rel)
		if err != nil {
			return nil
		}
		if !hasChunkFilesIn(candidate, ".") {
			_ = candidate.Close()
			return nil
		}
		return vetted(candidate, extractDir, rel)
	}

	// Check common subdirectory layouts
	candidates := []string{
		"immutable",
		filepath.Join("db", "immutable"),
	}

	// Check for a single top-level directory. ReadDir reports a symlink as
	// its own type rather than the directory it points at, so one is never
	// collected here.
	//
	// Read through the handle rather than by pathname: enumerating the pathname
	// again would list the entries of a directory swapped in behind it, while
	// the names taken from there are checked against the tree that was opened.
	// A candidate passing that pair of reads is a path into the replacement.
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return nil
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	if len(dirs) == 1 {
		candidates = append(candidates,
			dirs[0],
			filepath.Join(dirs[0], "immutable"),
			filepath.Join(dirs[0], "db", "immutable"),
		)
	}

	for _, c := range candidates {
		if dir := chunkDir(c); dir != nil {
			return dir
		}
	}

	return nil
}

// VerifyCertificateChain walks the Mithril certificate chain from
// the given hash back to the genesis certificate. This verifies
// the chain is unbroken and, if snapshotDigest is non-empty,
// that the leaf certificate's protocol message binds to it. It
// does not verify STM cryptographic signatures (Phase 2).
func VerifyCertificateChain(
	ctx context.Context,
	client *Client,
	certificateHash string,
	snapshotDigest string,
) error {
	_, err := VerifyCertificateChainWithMode(
		ctx,
		client,
		certificateHash,
		snapshotDigest,
		VerificationModeStructural,
	)
	return err
}

// VerifyCertificateChainWithMode verifies the Mithril certificate chain using
// the requested verification mode.
func VerifyCertificateChainWithMode(
	ctx context.Context,
	client *Client,
	certificateHash string,
	snapshotDigest string,
	mode VerificationMode,
) (*CertificateChainVerificationResult, error) {
	if mode == 0 {
		mode = VerificationModeStructural
	}
	if mode != VerificationModeStructural && mode != VerificationModeSTM {
		return nil, fmt.Errorf("unsupported verification mode: %d", mode)
	}
	if client == nil {
		return nil, errors.New("mithril client is nil")
	}
	if certificateHash == "" {
		return nil, errors.New("certificate hash is empty")
	}

	// Certificate chains on long-lived networks can exceed hundreds
	// of links; keep a high bound to prevent runaway loops while
	// allowing normal operation.
	const maxDepth = 10000

	currentHash := certificateHash
	seen := make(map[string]bool)
	isLeaf := true
	var childCert *Certificate
	result := &CertificateChainVerificationResult{
		SnapshotDigest: snapshotDigest,
	}

	for range maxDepth {
		if seen[currentHash] {
			return nil, fmt.Errorf(
				"certificate chain cycle detected at %s",
				currentHash,
			)
		}
		seen[currentHash] = true

		cert, err := client.GetCertificate(ctx, currentHash)
		if err != nil {
			return nil, fmt.Errorf(
				"fetching certificate %s: %w",
				currentHash,
				err,
			)
		}
		if cert.Hash != currentHash {
			return nil, fmt.Errorf(
				"certificate hash mismatch: requested %s, got %s",
				currentHash,
				cert.Hash,
			)
		}
		if cert.ProtocolMessage.ComputeHash() != cert.SignedMessage {
			return nil, fmt.Errorf(
				"certificate %s signed_message mismatch",
				currentHash,
			)
		}
		expectedHash, err := cert.ComputeHash()
		if err != nil {
			return nil, fmt.Errorf(
				"computing certificate hash for %s: %w",
				currentHash,
				err,
			)
		}
		if expectedHash != cert.Hash {
			return nil, fmt.Errorf(
				"certificate %s content hash mismatch",
				currentHash,
			)
		}
		currentEpoch, ok := cert.ProtocolMessage.MessageParts["current_epoch"]
		expectedEpoch := strconv.FormatUint(cert.Epoch, 10)
		if !ok || currentEpoch != expectedEpoch {
			return nil, fmt.Errorf(
				"certificate %s current_epoch mismatch: got %q, expected %s",
				currentHash,
				currentEpoch,
				expectedEpoch,
			)
		}
		if mode == VerificationModeSTM {
			if err := verifySTMCertificate(cert); err != nil {
				return nil, fmt.Errorf(
					"STM verification failed for certificate %s: %w",
					currentHash,
					err,
				)
			}
		}
		if childCert != nil {
			if childCert.PreviousHash != cert.Hash {
				return nil, fmt.Errorf(
					"certificate chain previous hash mismatch: child=%s previous=%s parent=%s",
					childCert.Hash,
					childCert.PreviousHash,
					cert.Hash,
				)
			}
			if childCert.Epoch != cert.Epoch &&
				childCert.Epoch != cert.Epoch+1 {
				return nil, fmt.Errorf(
					"certificate chain missing epoch between child=%d and parent=%d",
					childCert.Epoch,
					cert.Epoch,
				)
			}
			if childCert.Epoch == cert.Epoch {
				if childCert.AggregateVerificationKey != cert.AggregateVerificationKey {
					return nil, errors.New(
						"certificate chain aggregate verification key mismatch within epoch",
					)
				}
				if childCert.Metadata.Parameters != cert.Metadata.Parameters {
					return nil, errors.New(
						"certificate chain protocol parameters mismatch within epoch",
					)
				}
			} else {
				nextAVK, ok := cert.ProtocolMessage.MessageParts["next_aggregate_verification_key"]
				if !ok || nextAVK != childCert.AggregateVerificationKey {
					return nil, errors.New(
						"certificate chain aggregate verification key mismatch across epoch",
					)
				}
				nextProtocolParameters, ok := cert.ProtocolMessage.MessageParts["next_protocol_parameters"]
				if !ok || nextProtocolParameters != childCert.Metadata.Parameters.ComputeHash() {
					return nil, errors.New(
						"certificate chain protocol parameters mismatch across epoch",
					)
				}
			}
		}
		result.Certificates = append(result.Certificates, cert)

		// Verify the leaf certificate binds to the snapshot
		if isLeaf {
			result.LeafCertificate = cert
			if !cert.IsGenesis() {
				entityKind, err := cert.SignedEntityType.Kind()
				if err != nil {
					return nil, fmt.Errorf(
						"certificate %s has invalid signed entity type: %w",
						currentHash, err,
					)
				}
				result.SignedEntityKind = entityKind
			}
			if snapshotDigest != "" {
				certDigest := cert.ProtocolMessage.
					MessageParts["snapshot_digest"]
				if certDigest == "" {
					return nil, fmt.Errorf(
						"leaf certificate %s is missing "+
							"snapshot_digest",
						currentHash,
					)
				}
				if certDigest != snapshotDigest {
					return nil, fmt.Errorf(
						"certificate snapshot_digest "+
							"mismatch: cert has %q, "+
							"expected %q",
						certDigest,
						snapshotDigest,
					)
				}
			}
		}
		isLeaf = false
		childCert = cert

		// Genesis certificate terminates the chain
		if cert.IsGenesis() || cert.IsChainingToItself() {
			result.GenesisCertificate = cert
			return result, nil
		}

		if cert.PreviousHash == "" {
			return nil, fmt.Errorf(
				"certificate %s has empty previous_hash "+
					"but is not genesis",
				currentHash,
			)
		}

		currentHash = cert.PreviousHash
	}

	return nil, fmt.Errorf(
		"certificate chain exceeded maximum depth of %d",
		maxDepth,
	)
}

// chunkDirIn returns rel beneath an already-open, already-vetted base when rel
// is a directory holding chunk files, and nil when it is not. base is the name
// baseRoot was vetted under, for the returned vettedDir's own name check.
//
// Both components end up checked rather than only the last: verifying only the
// last is enough when the one above it is the operator's, but where that one is
// itself derived content — the extraction directory holding `immutable`, say —
// a symlink one level up would carry the whole lookup. The caller supplying a
// vetted baseRoot is what covers that half.
//
// The directory is returned as the handle it was inspected through rather than
// as a name the caller reassembles. A caller that builds the name itself is
// naming whatever occupies it now, not what was verified a moment ago — and so
// is a caller handed only a name.
//
// baseRoot stays the caller's; the returned vettedDir owns the child handle.
func chunkDirIn(baseRoot *os.Root, base, rel string) *vettedDir {
	root, err := openVerifiedRoot(baseRoot, rel)
	if err != nil {
		return nil
	}
	if !hasChunkFilesIn(root, ".") {
		_ = root.Close()
		return nil
	}
	return vetted(root, base, rel)
}

// hasChunkFilesIn reports whether rel, resolved through root, is a directory
// holding chunk files.
//
// The root handle is what makes this different from hasChunkFiles: the
// candidate is reached relative to a directory already open rather than walked
// again as a string, so a path vetted a moment ago cannot be something else by
// the time it is read.
func hasChunkFilesIn(root *os.Root, rel string) bool {
	entries, err := fs.ReadDir(root.FS(), filepath.ToSlash(rel))
	if err != nil {
		return false
	}
	return holdsChunkFile(entries)
}

func holdsChunkFile[E fs.DirEntry](entries []E) bool {
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".chunk" {
			return true
		}
	}
	return false
}

// isFileComplete checks if a file exists and matches the expected
// size. Returns false if expectedSize is 0 (unknown).
func isFileComplete(path string, expectedSize int64) bool {
	if expectedSize <= 0 {
		return false
	}
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Size() == expectedSize
}

// ledgerDir returns dir when it holds ledger state files, and nil when it does
// not or when the name no longer denotes the directory that was read.
//
// This replaced a bool-returning hasLedgerFiles: every caller went on to use
// the directory, and a bool left each of them to name it again for themselves.
//
// This is hasLedgerFiles for the callers that go on to *use* the directory
// rather than merely assert something about it: the "already extracted,
// skipping" fast paths, which take the answer as licence to skip extraction and
// hand the tree on as the node's ledger state. The handle it was read through
// is what is returned, so the manifest verification and the ledger-state import
// downstream are about the directory this inspected rather than about whatever
// later holds its name. The caller must Close the result.
func ledgerDir(dir string) *vettedDir {
	root, err := openVerifiedDir(dir)
	if err != nil {
		return nil
	}
	if !hasLedgerFilesIn(root) {
		_ = root.Close()
		return nil
	}
	return vetted(root, dir, ".")
}

// hasLedgerFilesIn is hasLedgerFiles resolved through an open handle on the
// directory, so the tree that is read is the one that was vetted.
func hasLedgerFilesIn(root *os.Root) bool {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// Check for ledger/<subdir>/state or
		// ledger/<subdir>/<slot>/state
		if hasFileInSubdirsIn(root, e.Name(), "state") {
			return true
		}
	}
	return false
}

// truncateDigest safely truncates a digest to at most 16
// characters for use in filenames and log messages.
func truncateDigest(digest string) string {
	if len(digest) > 16 {
		return digest[:16]
	}
	return digest
}

// hasFileInSubdirsIn is hasFileInSubdirs resolved through an open handle on an
// ancestor, so every lookup stays inside the tree that was vetted.
func hasFileInSubdirsIn(root *os.Root, dir, name string) bool {
	isFile := func(rel string) bool {
		fi, err := root.Stat(rel)
		return err == nil && !fi.IsDir()
	}
	if isFile(path.Join(dir, name)) {
		return true
	}
	entries, err := fs.ReadDir(root.FS(), dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if isFile(path.Join(dir, e.Name(), name)) {
			return true
		}
	}
	return false
}
