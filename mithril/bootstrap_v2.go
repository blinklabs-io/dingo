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
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// immutableDownloadWorkers is the number of concurrent immutable
// archive downloads during a v2 bootstrap. Paired with a shared
// keep-alive client (see downloadImmutables) so each worker holds a
// warm pooled connection; the connection pool in
// newPooledDownloadTransport is sized to this value. 16 is a safe
// default across constrained pods; bandwidth/CPU-rich hosts can go
// higher with diminishing returns past ~32.
const immutableDownloadWorkers = 16

// ancillaryManifestFilename is the signed manifest file inside a v2
// ancillary archive.
const ancillaryManifestFilename = "ancillary_manifest.json"

// immutableFileExtensions are the three files of an immutable trio.
var immutableFileExtensions = []string{"chunk", "primary", "secondary"}

// bootstrapV2 restores a node database from Mithril CardanoDatabase
// (v2) artifacts: per-immutable-file archives verified against the
// certified merkle root, plus an ancillary archive carrying the
// ledger state verified via the signed ancillary manifest. It
// produces the same BootstrapResult shape as the v1 flow so that
// downstream loading is backend-agnostic.
func bootstrapV2(
	ctx context.Context,
	cfg BootstrapConfig,
	aggregatorURL string,
) (*BootstrapResult, error) {
	client := newMithrilClient(aggregatorURL, cfg.AllowInsecureHTTP)

	// Step 1: Fetch latest artifact and verify its self-hash
	artifact, err := client.GetLatestCardanoDatabaseSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"fetching latest Cardano database snapshot: %w",
			err,
		)
	}
	if computed := artifact.ComputeHash(); computed != artifact.Hash {
		return nil, fmt.Errorf(
			"artifact hash mismatch for Cardano database snapshot: computed %s, artifact has %s",
			computed,
			artifact.Hash,
		)
	}
	if err := validateSnapshotIdentity(
		cfg.Network, artifact.Network, artifact.Hash,
	); err != nil {
		return nil, fmt.Errorf("validating artifact metadata: %w", err)
	}
	if cfg.Network != "" && artifact.Network != "" &&
		cfg.Network != artifact.Network {
		return nil, fmt.Errorf(
			"mithril artifact network mismatch: requested=%s artifact=%s",
			cfg.Network,
			artifact.Network,
		)
	}
	// A certificate authenticates the immutable database content, while the
	// ledger state and the final in-progress immutable trio are authenticated
	// by the separately signed ancillary manifest. Do not start downloading
	// immutable data when the second half of that trust boundary cannot be
	// checked.
	if cfg.VerifyCertificateChain {
		if len(artifact.Ancillary.Locations) == 0 {
			return nil, errors.New(
				"verified Mithril bootstrap requires ancillary locations",
			)
		}
		if cfg.AncillaryVerificationKey == "" {
			return nil, errors.New(
				"verified Mithril bootstrap requires an ancillary verification key",
			)
		}
	}

	cfg.Logger.Info(
		"found latest Cardano database snapshot",
		"component", "mithril",
		"network", artifact.Network,
		"hash", artifact.Hash,
		"snapshot_hash", artifact.Hash,
		"epoch", artifact.Beacon.Epoch,
		"immutable_file_number", artifact.Beacon.ImmutableFileNumber,
		"total_db_size", artifact.TotalDbSizeUncompressed,
	)
	cfg.Logger = cfg.Logger.With(
		"network", artifact.Network,
		"snapshot_hash", artifact.Hash,
		"snapshot_epoch", artifact.Beacon.Epoch,
		"snapshot_immutable_file_number", artifact.Beacon.ImmutableFileNumber,
		"artifact_backend", BackendV2,
	)

	if len(artifact.Immutables.Locations) == 0 {
		return nil, fmt.Errorf(
			"no immutable locations in Cardano database snapshot %s",
			artifact.Hash,
		)
	}

	// Step 1b: Verify certificate chain (optional)
	if cfg.VerifyCertificateChain {
		if err := verifyArtifactCertificateV2(
			ctx, cfg, client, artifact,
		); err != nil {
			return nil, err
		}
		cfg.Logger.Info(
			"certificate chain verified",
			"component", "mithril",
			"phase", "certificate_verification",
			"artifact", "cardano_database",
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

	// Unlike v1, the ancillary download and the immutable archive
	// pool report progress concurrently. Downstream OnProgress
	// callbacks are not required to be thread-safe, so serialize
	// emissions here.
	if cfg.OnProgress != nil {
		var progressMu sync.Mutex
		onProgress := cfg.OnProgress
		cfg.OnProgress = func(p DownloadProgress) {
			progressMu.Lock()
			defer progressMu.Unlock()
			onProgress(p)
		}
	}

	// Step 3: Fetch the digest list and verify it against the
	// certified merkle root. The digest map then authenticates every
	// downloaded immutable file.
	digests, err := fetchVerifiedDigests(
		ctx, client, cfg, artifact, downloadDir,
	)
	if err != nil {
		return nil, err
	}

	extractDir := filepath.Join(
		downloadDir,
		filepath.Base("immutable-"+artifact.Hash),
	)
	var ancillaryTree *vettedDir
	var ancillaryDigests map[string]string
	var ancillaryArchivePath string
	var ancillaryErr error
	// Closed only when the result that would own it is never returned; on
	// success it is carried to the ledger-state import and released by Cleanup.
	defer func() {
		if !success {
			ancillaryTree.Close()
		}
	}()

	// Steps 4+5: Download immutable archives and the ancillary archive in
	// parallel. A verified bootstrap records ancillary failures and returns
	// them after the immutable download completes; the sync caller disables
	// database-copy pipelining for this path, so no imported state is exposed
	// as ready before both trust inputs are present.
	ancCtx, ancCancel := context.WithCancel(ctx)
	var ancWg sync.WaitGroup
	defer ancWg.Wait()
	defer ancCancel()
	if len(artifact.Ancillary.Locations) > 0 {
		ancWg.Go(func() {
			candidateDir := filepath.Join(
				downloadDir,
				filepath.Base("ancillary-"+artifact.Hash),
			)
			candidateArchive := filepath.Join(
				downloadDir,
				filepath.Base(fmt.Sprintf(
					"%s-%s-ancillary.tar.zst",
					artifact.Network,
					truncateDigest(artifact.Hash),
				)),
			)
			// One handle spans the cache check, the manifest verification,
			// and the ledger-state import downstream. A directory substituted
			// at any point after the check is therefore not what gets
			// verified *or* loaded — where re-resolving the name at each step
			// would leave each step describing a possibly different tree.
			if cached := ledgerDir(candidateDir); cached != nil {
				cachedDigests, err := verifyAncillaryExtraction(cfg, cached)
				if err != nil {
					cached.Close()
					cfg.Logger.Warn(
						"cached ancillary data failed "+
							"verification, redownloading",
						"component", "mithril",
						"path", candidateDir,
						"error", err,
					)
					if err := os.RemoveAll(candidateDir); err != nil {
						cfg.Logger.Warn(
							"failed to remove unverified ancillary data",
							"component", "mithril",
							"path", candidateDir,
							"error", err,
						)
					}
					if err := os.Remove(candidateArchive); err != nil &&
						!errors.Is(err, os.ErrNotExist) {
						cfg.Logger.Warn(
							"failed to remove stale ancillary archive",
							"component", "mithril",
							"path", candidateArchive,
							"error", err,
						)
					}
				} else {
					cfg.Logger.Info(
						"ancillary data already "+
							"extracted, skipping",
						"component", "mithril",
						"path", cached.Path(),
					)
					ancillaryTree = cached
					ancillaryDigests = cachedDigests
					if _, err := os.Stat(candidateArchive); err == nil {
						ancillaryArchivePath = candidateArchive
					}
					return
				}
			}
			tree, treeDigests, archPath, ancErr := downloadAncillaryV2(
				ancCtx, cfg, artifact, downloadDir,
			)
			// Recorded whether or not the tree turned out usable, so a
			// downloaded archive still gets cleaned up.
			if archPath != "" {
				ancillaryArchivePath = archPath
			}
			if ancErr != nil {
				ancillaryErr = ancErr
				return
			}
			// The handle the manifest was checked through, carried straight
			// across. Reopening the directory by name here would hand the
			// import a tree nothing verified, under a flag saying otherwise.
			ancillaryTree = tree
			ancillaryDigests = treeDigests
		})
	}

	// Step 4: Download, extract, and verify the immutable archives
	if err := downloadImmutables(
		ctx, cfg, artifact, digests, downloadDir, extractDir,
	); err != nil {
		return nil, fmt.Errorf(
			"downloading immutable archives: %w",
			err,
		)
	}
	// Both components are checked, not just the last one. The extraction
	// directory is itself derived inside the download directory and no more
	// trustworthy than what it contains, so it is vetted first and the
	// immutable directory is then taken from that handle.
	//
	// One handle, because the ledger-state import falls back to this same
	// directory: vetting it twice would be two resolutions of one name, and the
	// tree whose ImmutableDB was accepted could then differ from the tree whose
	// ledger state gets imported.
	//
	// Both are held until Cleanup and closed on every error path below, since
	// the result that would own them is never returned.
	extractTree, err := openVerifiedDir(extractDir)
	if err != nil {
		return nil, fmt.Errorf(
			"verifying extraction directory %s: %w", extractDir, err,
		)
	}
	immutableTree := chunkDirIn(extractTree, extractDir, "immutable")
	defer func() {
		if !success {
			immutableTree.Close()
			_ = extractTree.Close()
		}
	}()
	if immutableTree == nil {
		return nil, fmt.Errorf(
			"immutable DB directory not found at %s after download",
			filepath.Join(extractDir, "immutable"),
		)
	}

	// Wait for ancillary download to finish (also deferred above
	// for the error-return path; calling Wait twice is safe).
	ancWg.Wait()
	if ancillaryErr != nil {
		if cfg.VerifyCertificateChain {
			return nil, fmt.Errorf(
				"verified Mithril ancillary data unavailable: %w",
				ancillaryErr,
			)
		}
		cfg.Logger.Warn(
			"failed to download ancillary data; continuing without ledger state",
			"component",
			"mithril",
			"error",
			ancillaryErr,
		)
	}
	if cfg.VerifyCertificateChain && ancillaryTree == nil {
		return nil, errors.New(
			"verified Mithril bootstrap produced no ancillary data",
		)
	}

	cfg.Logger.Info(
		"Mithril bootstrap ready for loading",
		"component", "mithril",
		"phase", "bootstrap_ready",
		"immutable_dir", immutableTree.Path(),
		"ancillary_dir", ancillaryTree.Path(),
	)

	success = true
	result := &BootstrapResult{
		// Synthesize a SnapshotListItem so downstream consumers
		// (sync flow, metrics) stay backend-agnostic.
		Snapshot: &SnapshotListItem{
			SnapshotBase: SnapshotBase{
				Digest:             artifact.Hash,
				Network:            artifact.Network,
				Beacon:             artifact.Beacon,
				CertificateHash:    artifact.CertificateHash,
				Size:               artifact.TotalDbSizeUncompressed,
				AncillarySize:      artifact.Ancillary.SizeUncompressed,
				CreatedAt:          artifact.CreatedAt,
				CardanoNodeVersion: artifact.CardanoNodeVersion,
			},
		},
		ImmutableDir:  immutableTree.Path(),
		ImmutableRoot: immutableTree.Root(),
		// The digest list the certificate's merkle root covers, carried on so
		// the load can re-check each file from the descriptor it reads through
		// rather than trusting a check that ran before the file was closed.
		ImmutableDigests: digests,
		ExtractDir:       extractDir,
		ExtractRoot:      extractTree,
		AncillaryDir:     ancillaryTree.Path(),
		AncillaryRoot:    ancillaryTree.Root(),
		// Both paths that accept an ancillary tree verify it through the very
		// handle carried here — the cache-reuse path opens it once and keeps
		// it, and downloadAncillaryV2 returns the handle it checked. So the
		// flag is a claim about the directory this handle refers to, which is
		// the only thing that makes it worth anything.
		AncillaryVerified:    ancillaryTree != nil && cfg.VerifyCertificateChain,
		AncillaryDigests:     ancillaryDigests,
		AncillaryArchivePath: ancillaryArchivePath,
	}
	if createdTempDir {
		result.TempDir = downloadDir
	}
	return result, nil
}

// verifyArtifactCertificateV2 walks and verifies the certificate
// chain for a v2 artifact and checks that the leaf certificate binds
// to the artifact: signed entity kind, certified merkle root, beacon,
// and network.
func verifyArtifactCertificateV2(
	ctx context.Context,
	cfg BootstrapConfig,
	client *Client,
	artifact *CardanoDatabaseSnapshot,
) error {
	if artifact.CertificateHash == "" {
		return fmt.Errorf(
			"certificate chain verification requested "+
				"but Cardano database snapshot %s has no certificate hash",
			artifact.Hash,
		)
	}
	cfg.Logger.Info(
		"verifying certificate chain",
		"component", "mithril",
		"phase", "certificate_verification",
		"artifact", "cardano_database",
		"certificate_hash", artifact.CertificateHash,
	)
	verificationMode := VerificationModeStructural
	if cfg.GenesisVerificationKey != "" {
		verificationMode = VerificationModeSTM
	}
	verificationResult, err := VerifyCertificateChainWithMode(
		ctx,
		client,
		artifact.CertificateHash,
		"", // v2 leaf binding uses cardano_database_merkle_root below
		verificationMode,
	)
	if err != nil {
		return fmt.Errorf(
			"certificate chain verification failed: %w",
			err,
		)
	}
	if cfg.GenesisVerificationKey != "" {
		if verificationResult == nil ||
			verificationResult.GenesisCertificate == nil {
			return errors.New(
				"genesis verification key provided but no genesis certificate found in chain",
			)
		}
		if err := VerifyGenesisCertificateSignature(
			verificationResult.GenesisCertificate,
			cfg.GenesisVerificationKey,
		); err != nil {
			return fmt.Errorf(
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
		return fmt.Errorf(
			"building verification material failed: %w",
			err,
		)
	}
	if err := ValidateVerificationMaterial(verificationMaterial); err != nil {
		return fmt.Errorf(
			"verification material validation failed: %w",
			err,
		)
	}
	if verificationResult.SignedEntityKind !=
		signedEntityTypeCardanoDatabase {
		return fmt.Errorf(
			"unexpected signed entity kind for Cardano database bootstrap: %s",
			verificationResult.SignedEntityKind,
		)
	}
	leaf := verificationResult.LeafCertificate
	certifiedRoot := leaf.ProtocolMessage.
		MessageParts["cardano_database_merkle_root"]
	if certifiedRoot == "" {
		return fmt.Errorf(
			"leaf certificate %s is missing cardano_database_merkle_root",
			leaf.Hash,
		)
	}
	if certifiedRoot != artifact.MerkleRoot {
		return fmt.Errorf(
			"certificate cardano_database_merkle_root mismatch: cert has %q, artifact has %q",
			certifiedRoot,
			artifact.MerkleRoot,
		)
	}
	if artifact.Network != "" &&
		(leaf.Metadata.Network == "" ||
			leaf.Metadata.Network != artifact.Network) {
		return fmt.Errorf(
			"certificate network mismatch: certificate=%s artifact=%s",
			leaf.Metadata.Network,
			artifact.Network,
		)
	}
	if beacon := leaf.SignedEntityType.CardanoDatabase(); beacon != nil {
		if beacon.Epoch != artifact.Beacon.Epoch ||
			beacon.ImmutableFileNumber != artifact.Beacon.ImmutableFileNumber {
			return fmt.Errorf(
				"signed entity beacon mismatch: certificate=(epoch=%d, immutable=%d) artifact=(epoch=%d, immutable=%d)",
				beacon.Epoch,
				beacon.ImmutableFileNumber,
				artifact.Beacon.Epoch,
				artifact.Beacon.ImmutableFileNumber,
			)
		}
	}
	return nil
}

// fetchVerifiedDigests downloads the immutable-file digest list
// (snapshot-specific cloud archive preferred, aggregator route as
// fallback), verifies its merkle root against the artifact's
// certified root, and returns a file name -> digest map.
func fetchVerifiedDigests(
	ctx context.Context,
	client *Client,
	cfg BootstrapConfig,
	artifact *CardanoDatabaseSnapshot,
	downloadDir string,
) (map[string]string, error) {
	var entries []CardanoDatabaseDigestEntry
	var lastErr error
	fetched := false
	for _, loc := range artifact.Digests.Locations {
		switch loc.Type {
		case locationTypeCloudStorage:
			if loc.URI == "" {
				continue
			}
			entries, lastErr = downloadDigestsArchive(
				ctx, cfg, loc.URI, artifact, downloadDir,
			)
		case locationTypeAggregator:
			entries, lastErr = client.GetCardanoDatabaseDigests(ctx)
		default:
			cfg.Logger.Debug(
				"skipping unsupported digest location type",
				"component", "mithril",
				"type", loc.Type,
			)
			continue
		}
		if lastErr != nil {
			cfg.Logger.Warn(
				"digest location failed, trying next",
				"component", "mithril",
				"type", loc.Type,
				"error", lastErr,
			)
			continue
		}
		lastErr = verifyDigestMerkleRoot(entries, artifact)
		if lastErr != nil {
			cfg.Logger.Warn(
				"digest location failed merkle validation, trying next",
				"component", "mithril",
				"type", loc.Type,
				"error", lastErr,
			)
			removeDigestsCache(artifact, downloadDir)
			continue
		}
		fetched = true
		break
	}
	if !fetched {
		if lastErr == nil {
			lastErr = errors.New("no usable digest locations")
		}
		return nil, fmt.Errorf("fetching digest list: %w", lastErr)
	}

	digests := make(map[string]string, len(entries))
	for _, entry := range entries {
		num, ok := immutableFileNumberFromName(entry.ImmutableFileName)
		if !ok || num > artifact.Beacon.ImmutableFileNumber {
			continue
		}
		digests[entry.ImmutableFileName] = entry.Digest
	}
	cfg.Logger.Info(
		"digest list verified against certified merkle root",
		"component", "mithril",
		"phase", "digest_verification",
		"artifact", "immutable_digest_list",
		"digests", len(digests),
	)
	return digests, nil
}

func verifyDigestMerkleRoot(
	entries []CardanoDatabaseDigestEntry,
	artifact *CardanoDatabaseSnapshot,
) error {
	leaves, err := digestMerkleLeaves(
		entries, artifact.Beacon.ImmutableFileNumber,
	)
	if err != nil {
		return fmt.Errorf("building digest merkle leaves: %w", err)
	}
	if len(leaves) == 0 {
		return errors.New("digest list is empty")
	}
	root, err := computeMMRRoot(leaves)
	if err != nil {
		return fmt.Errorf("computing digest merkle root: %w", err)
	}
	if computed := hex.EncodeToString(root); computed != artifact.MerkleRoot {
		return fmt.Errorf(
			"digest merkle root mismatch: computed %s, artifact has %s",
			computed,
			artifact.MerkleRoot,
		)
	}
	return nil
}

func removeDigestsCache(
	artifact *CardanoDatabaseSnapshot,
	downloadDir string,
) {
	suffix := truncateDigest(artifact.Hash)
	_ = os.Remove(filepath.Join(downloadDir, filepath.Base(fmt.Sprintf(
		"digests-%s.tar.zst",
		suffix,
	))))
	_ = os.RemoveAll(
		filepath.Join(downloadDir, filepath.Base("digests-"+suffix)),
	)
}

// downloadDigestsArchive downloads the snapshot-specific digest
// archive (tar.zst containing a single JSON file) and parses it.
func downloadDigestsArchive(
	ctx context.Context,
	cfg BootstrapConfig,
	uri string,
	artifact *CardanoDatabaseSnapshot,
	downloadDir string,
) ([]CardanoDatabaseDigestEntry, error) {
	archivePath, err := DownloadSnapshot(
		ctx, DownloadConfig{
			URL:     uri,
			DestDir: downloadDir,
			Filename: filepath.Base(fmt.Sprintf(
				"digests-%s.tar.zst",
				truncateDigest(artifact.Hash),
			)),
			Logger: cfg.Logger,
			OnProgress: withProgressContext(
				cfg.OnProgress,
				"immutable_digest_list",
				artifact.Hash,
			),
			IdleTimeout:         cfg.DownloadIdleTimeout,
			MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
			MaxTransientRetries: cfg.DownloadMaxTransientRetries,
			AllowInsecureHTTP:   cfg.AllowInsecureHTTP,
		},
	)
	if err != nil {
		return nil, err
	}
	destDir := filepath.Join(
		downloadDir,
		filepath.Base("digests-"+truncateDigest(artifact.Hash)),
	)
	// Replace: removeDigestsCache may not have run if a previous attempt
	// was interrupted, leaving a stale digests directory.
	if _, err := ExtractArchive(
		ctx,
		archivePath,
		destDir,
		cfg.Logger.With(
			"phase", "digest_extraction",
			"artifact", "immutable_digest_list",
		),
		WithReplaceDestination(),
	); err != nil {
		return nil, fmt.Errorf("extracting digests archive: %w", err)
	}
	dirEntries, err := os.ReadDir(destDir)
	if err != nil {
		return nil, fmt.Errorf("reading digests directory: %w", err)
	}
	jsonPath := ""
	for _, entry := range dirEntries {
		if entry.IsDir() ||
			!strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		jsonPath = filepath.Join(destDir, entry.Name())
		break
	}
	if jsonPath == "" {
		return nil, errors.New(
			"no digest JSON file found in digests archive",
		)
	}
	data, err := os.ReadFile(
		jsonPath,
	) //nolint:gosec // path is constructed from our own extraction directory
	if err != nil {
		return nil, fmt.Errorf("reading digest JSON: %w", err)
	}
	var entries []CardanoDatabaseDigestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing digest JSON: %w", err)
	}
	return entries, nil
}

// downloadImmutables downloads, extracts, and digest-verifies the
// immutable archives 0..=beacon.ImmutableFileNumber into extractDir
// using a bounded worker pool. Trios whose files already exist with
// matching digests are skipped, providing resume support. Archives
// are deleted after successful extraction to bound disk usage.
func downloadImmutables(
	ctx context.Context,
	cfg BootstrapConfig,
	artifact *CardanoDatabaseSnapshot,
	digests map[string]string,
	downloadDir string,
	extractDir string,
) error {
	immutableDir, immutableRoot, err := openImmutableRoot(extractDir)
	if err != nil {
		return err
	}
	defer immutableRoot.Close()

	// archiveDir is not created here. A create-then-close-then-hand-off-
	// the-bare-path step would itself be a TOCTOU window: a symlink
	// swapped in for it between this function returning and the first
	// download would be followed by whatever opens it next. Instead,
	// DownloadSnapshot (called below, once per archive) creates and
	// verifies its own DestDir through a handle on its parent every time
	// it runs, closing that window at the point it actually matters.
	archiveDir := filepath.Join(
		downloadDir,
		filepath.Base("immutable-archives-"+truncateDigest(artifact.Hash)),
	)

	locations := make(
		[]*CardanoDatabaseLocation,
		0,
		len(artifact.Immutables.Locations),
	)
	for i := range artifact.Immutables.Locations {
		if artifact.Immutables.Locations[i].URITemplate != "" {
			locations = append(
				locations,
				&artifact.Immutables.Locations[i],
			)
		}
	}
	if len(locations) == 0 {
		return errors.New(
			"no usable immutable archive locations (URI template missing)",
		)
	}

	totalArchives := artifact.Beacon.ImmutableFileNumber + 1
	// startArchive is the lowest immutable number this run downloads. A catch-up
	// sets cfg.StartImmutable to the immutable-import marker so the archives
	// already present in the blob store are skipped; a fresh bootstrap leaves it
	// zero. Clamp defensively so a stale marker never exceeds the artifact.
	startArchive := min(cfg.StartImmutable, totalArchives)
	downloadArchives := totalArchives - startArchive
	// Progress denominator must be the immutable-only uncompressed size,
	// not TotalDbSizeUncompressed. The latter covers the whole database
	// (immutable archives plus the ancillary ledger state), but this pool
	// downloads only the immutable archives — the ancillary archive runs
	// concurrently and reports its own progress. Using the whole-DB size
	// here would cap immutable progress below 100%. Fall back to the
	// whole-DB size only when the per-file average is unavailable. The
	// denominator counts only the archives actually downloaded ([start..N]).
	immutableTotalBytes := artifact.Immutables.AverageSizeUncompressed *
		int64(downloadArchives) // #nosec G115 -- archive count, non-negative
	if immutableTotalBytes <= 0 {
		immutableTotalBytes = artifact.TotalDbSizeUncompressed
	}
	cfg.Logger.Info(
		"downloading immutable archives",
		"component", "mithril",
		"phase", "immutable_download",
		"artifact", "immutable_archives",
		"immutable_file_start", startArchive,
		"immutable_file_end", totalArchives-1,
		"immutable_archives_total", downloadArchives,
		"estimated_bytes", immutableTotalBytes,
		"destination", extractDir,
	)
	onArchiveDone := newImmutableProgressWithContext(
		cfg,
		downloadArchives,
		immutableTotalBytes,
		"immutable_archives",
		artifact.Hash,
	)
	// Per-archive extraction is too chatty for the main log at
	// mainnet scale (tens of thousands of archives); aggregate
	// progress is reported by onArchiveDone instead.
	quietLogger := slog.New(slog.DiscardHandler)

	// One shared keep-alive client for the whole pool so workers reuse
	// pooled connections instead of paying a TCP+TLS handshake per
	// archive. Sized to the worker count; idle connections are closed
	// when the pool finishes. Carried on the (by-value) cfg so each
	// fetchImmutableArchive call reuses it without a signature change.
	dlTransport := newPooledDownloadTransport(immutableDownloadWorkers)
	defer dlTransport.CloseIdleConnections()
	cfg.httpClient = &http.Client{
		Timeout:       0,
		Transport:     dlTransport,
		CheckRedirect: httpsOnlyRedirect,
	}

	// Optional download<->processing pipeline: chunks are fetched in
	// parallel (out of order) but seq invokes OnChunkContiguous in strict
	// 0,1,2,... order as each prefix completes, so the caller can copy
	// blocks while later chunks still download. nil hook => no sequencer,
	// legacy "download everything, then process" behaviour.
	// errgroup only cancels its context when a download worker (a g.Go) returns
	// an error. The pipelined copy runs in the sequencer's own goroutine, so a
	// copy failure would otherwise let the remaining archives finish
	// downloading before the error surfaced. Cancel the download context
	// explicitly when the sequencer's processing fails so downloads stop at
	// once.
	dlCtx := ctx
	var seq *inOrderSequencer
	// copyErr captures a pipelined-copy failure as the root cause. It is
	// written in the sequencer's single consumer goroutine and read only after
	// seq.Wait() returns, so the mutex inside the sequencer orders the access.
	var copyErr error
	if cfg.OnChunkContiguous != nil {
		var cancelDownloads context.CancelCauseFunc
		dlCtx, cancelDownloads = context.WithCancelCause(ctx)
		defer cancelDownloads(nil)
		seqProcess := func(num uint64) error {
			if err := cfg.OnChunkContiguous(ContiguousChunk{
				Dir:     immutableDir,
				Root:    immutableRoot,
				Digests: digests,
				Start:   startArchive,
				Num:     num,
			}); err != nil {
				copyErr = err
				cancelDownloads(err)
				return err
			}
			return nil
		}
		// A full sync starts the contiguous processing window at 0; a catch-up
		// starts it at the immutable-import marker so already-present archives
		// are neither downloaded nor re-processed.
		if startArchive == 0 {
			seq = newInOrderSequencer(totalArchives, seqProcess)
		} else {
			seq = newInOrderSequencerFrom(
				startArchive, totalArchives, seqProcess,
			)
		}
	}

	g, gctx := errgroup.WithContext(dlCtx)
	g.SetLimit(immutableDownloadWorkers)
	for num := range totalArchives {
		if num < startArchive {
			// Already present in the blob store from a prior sync; the catch-up
			// forward-copy fills only what is missing above this point.
			continue
		}
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			if bytes, err := checkImmutableTrio(
				immutableRoot, num, digests,
			); err == nil {
				onArchiveDone(bytes)
				if seq != nil {
					seq.Complete(num)
				}
				return nil
			}
			var bytes int64
			var lastErr error
			fetched := false
			for i, location := range locations {
				if err := fetchImmutableArchive(
					gctx, cfg, quietLogger, location, num,
					archiveDir, extractDir,
				); err != nil {
					lastErr = err
					removeImmutableTrio(immutableRoot, num)
					_ = os.Remove(
						immutableArchivePath(archiveDir, num),
					)
					cfg.Logger.Warn(
						"immutable archive location failed, trying next",
						"component", "mithril",
						"immutable_file_number", num,
						"location", i+1,
						"total", len(locations),
						"error", lastErr,
					)
					continue
				}
				bytes, lastErr = checkImmutableTrio(
					immutableRoot, num, digests,
				)
				if lastErr != nil {
					removeImmutableTrio(immutableRoot, num)
					_ = os.Remove(
						immutableArchivePath(archiveDir, num),
					)
					cfg.Logger.Warn(
						"immutable archive verification failed, trying next",
						"component", "mithril",
						"immutable_file_number", num,
						"location", i+1,
						"total", len(locations),
						"error", lastErr,
					)
					continue
				}
				fetched = true
				break
			}
			if !fetched {
				if lastErr == nil {
					lastErr = errors.New(
						"no usable immutable archive locations",
					)
				}
				return fmt.Errorf(
					"immutable archive %05d: %w",
					num,
					lastErr,
				)
			}
			onArchiveDone(bytes)
			if seq != nil {
				seq.Complete(num)
			}
			return nil
		})
	}
	err = g.Wait()
	if seq != nil {
		// A download failure (or the copy-triggered cancel above) means the
		// contiguous prefix can never advance, so unblock the consumer's Wait.
		if err != nil {
			seq.Cancel(err)
		}
		// Block until the consumer drains; its error is redundant here (copyErr
		// holds a processing failure and err holds a download failure).
		_ = seq.Wait()
		// A pipelined-copy failure is the root cause; surface it instead of the
		// context cancellation it triggered in the download workers.
		if copyErr != nil {
			return copyErr
		}
	}
	return err
}

// newImmutableProgressWithContext aggregates the concurrent immutable
// workers into one progress stream while retaining the artifact identity.
func newImmutableProgressWithContext(
	cfg BootstrapConfig,
	totalArchives uint64,
	totalBytes int64,
	artifact string,
	snapshotHash string,
) func(bytesAdded int64) {
	var mu sync.Mutex
	var doneArchives uint64
	var doneBytes int64
	var lastLog time.Time
	lastLoggedPercent := -5.0
	start := time.Now()
	return func(bytesAdded int64) {
		mu.Lock()
		defer mu.Unlock()
		doneArchives++
		doneBytes += bytesAdded
		// Archive-count fraction drives the per-archive log line and its
		// throttle below. Archives vary in size, so this does NOT equal
		// the byte fraction reported to OnProgress.
		archivePercent := float64(doneArchives) / float64(totalArchives) * 100
		if cfg.OnProgress != nil {
			var speed float64
			if elapsed := time.Since(start).Seconds(); elapsed > 0 {
				speed = float64(doneBytes) / elapsed
			}
			// Report a byte-based percent so Percent stays consistent
			// with the BytesDownloaded/TotalBytes pair it is emitted
			// alongside. Using the archive-count fraction here produced a
			// percentage that disagreed with the bytes (e.g. "45.3%
			// (8.2 GB / 14.2 GB)", where 8.2/14.2 is 57.7%). Fall back to
			// the archive-count fraction only when the total size is
			// unknown.
			percent := pctOf(doneBytes, totalBytes)
			if totalBytes <= 0 {
				percent = archivePercent
			}
			// The total is estimated from the per-file average, so the
			// accumulated bytes can marginally overshoot it on the final
			// archive. Clamp so the reported percent never exceeds 100.
			percent = min(percent, 100)
			cfg.OnProgress(DownloadProgress{
				BytesDownloaded:    doneBytes,
				TotalBytes:         totalBytes,
				Percent:            percent,
				BytesPerSecond:     speed,
				Artifact:           artifact,
				SnapshotHash:       snapshotHash,
				ArtifactsCompleted: doneArchives,
				ArtifactsTotal:     totalArchives,
			})
		}
		now := time.Now()
		if !lastLog.IsZero() &&
			now.Sub(lastLog) < 10*time.Second &&
			archivePercent-lastLoggedPercent < 5.0 &&
			doneArchives != totalArchives {
			return
		}
		cfg.Logger.Info(
			"immutable archives: progress",
			"component", "mithril",
			"phase", "immutable_download",
			"artifact", artifact,
			"immutable_archives_completed", doneArchives,
			"immutable_archives_total", totalArchives,
			"percent", archivePercent,
		)
		lastLog = now
		lastLoggedPercent = archivePercent
	}
}

// fetchImmutableArchive downloads and extracts a single immutable
// archive, then removes the archive file to bound disk usage.
//
// The download, the extraction, and the removal are all anchored to the
// same directory handle DownloadSnapshot's internal directory verification
// opens on archiveDir, rather than each re-resolving archiveDir by name.
// archiveDir is shared by every archive this pool downloads and is deleted
// out from under the process on every success (see the comment below), so a
// directory swapped in for its name between this call's steps would, with a
// bare path, redirect extraction to attacker-controlled content and let
// os.Remove delete an external file. Passing the retained root and a
// root-relative filename to extraction and removal instead means a later
// swap of archiveDir's name cannot affect either operation: they resolve
// through the handle opened before the swap could happen, not through the
// name.
func fetchImmutableArchive(
	ctx context.Context,
	cfg BootstrapConfig,
	extractLogger *slog.Logger,
	location *CardanoDatabaseLocation,
	num uint64,
	archiveDir string,
	extractDir string,
) error {
	// Use the main logger (not the per-archive quiet logger) for download
	// retries so that transient-error warnings include immutable_file_number
	// and reach the operator. extractLogger (discarded) is still used for
	// the extraction step to suppress per-file INFO noise.
	dlLogger := cfg.Logger.With("immutable_file_number", num)
	archivePath := immutableArchivePath(archiveDir, num)
	archiveFilename := filepath.Base(archivePath)
	_, root, err := downloadSnapshot(
		ctx, DownloadConfig{
			URL:                 location.ImmutableArchiveURI(num),
			DestDir:             archiveDir,
			Filename:            archiveFilename,
			Logger:              dlLogger,
			HTTPClient:          cfg.httpClient,
			IdleTimeout:         cfg.DownloadIdleTimeout,
			MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
			MaxTransientRetries: cfg.DownloadMaxTransientRetries,
			AllowInsecureHTTP:   cfg.AllowInsecureHTTP,
		},
	)
	if root != nil {
		defer root.Close()
	}
	if err != nil {
		return err
	}

	file, err := root.Open(archiveFilename)
	if err != nil {
		return fmt.Errorf("opening downloaded archive: %w", err)
	}
	defer file.Close()

	// Merge: every immutable archive extracts into one shared directory,
	// concurrently, so this destination accumulates across calls and must
	// not be staged-and-swapped.
	if _, err := extractArchiveFile(
		ctx, file, archivePath, extractDir, extractLogger,
		WithMergeIntoDestination(),
	); err != nil {
		return fmt.Errorf("extracting: %w", err)
	}
	if err := root.Remove(archiveFilename); err != nil {
		cfg.Logger.Warn(
			"failed to remove immutable archive after extraction",
			"component", "mithril",
			"path", archivePath,
			"error", err,
		)
	}
	return nil
}

func immutableArchivePath(archiveDir string, num uint64) string {
	return filepath.Join(archiveDir, fmt.Sprintf("%05d.tar.zst", num))
}

// checkImmutableTrio verifies the SHA-256 digests of the three files
// of an immutable file number against the verified digest map.
// Returns the cumulative file size on success.
//
// Hashed through the immutable directory's handle rather than by joining its
// name. Extraction writes through that handle, so hashing anything else would
// be a digest of a file this process did not necessarily write — and the
// mismatch a repointed name produces would be reported as a corrupt download,
// sending the pool round the locations again instead of refusing.
//
// What this cannot establish on its own is that the file it hashed is the file
// something later reads: it closes each one, and the load opens it again by
// name. That second half is immutable.NewFromRootVerified's, which re-checks
// these same digests from the descriptor the read goes through. Neither is
// redundant — this one decides whether a downloaded archive is kept and lets
// the pool retry another location, and it runs while the tree is still being
// assembled.
func checkImmutableTrio(
	root *os.Root,
	num uint64,
	digests map[string]string,
) (int64, error) {
	var totalBytes int64
	for _, ext := range immutableFileExtensions {
		name := fmt.Sprintf("%05d.%s", num, ext)
		expected, ok := digests[name]
		if !ok {
			return 0, fmt.Errorf("no digest entry for %s", name)
		}
		sum, size, err := sha256FileInRoot(root, name)
		if err != nil {
			return 0, err
		}
		if sum != expected {
			return 0, fmt.Errorf(
				"digest mismatch for %s: computed %s, expected %s",
				name,
				sum,
				expected,
			)
		}
		totalBytes += size
	}
	return totalBytes, nil
}

// openImmutableRoot creates the immutable directory under extractDir and
// returns its path along with a handle to resolve cleanup through.
//
// A symlink at that name is refused rather than followed. Extraction already
// refuses to write through one, but the retry path removes a failed trio by
// name, and removing `<extract>/immutable/00000.chunk` resolves `immutable` on
// the way to the file — through a symlink that unlinks somebody else's files
// as the cost of a failed download. The handle closes the same gap against a
// symlink planted later: it refers to the directory rather than to a name that
// can be repointed once the download is under way.
func openImmutableRoot(extractDir string) (string, *os.Root, error) {
	cleanDir := filepath.Clean(extractDir)
	// The same check ExtractArchive applies to its own destination. This runs
	// first, so without it the accumulation root would be created through a
	// symlinked extraction directory and only the later extraction would
	// notice — after the fact, and after creating a directory in whatever the
	// link pointed at.
	if err := assertSafeExtractRoot(cleanDir); err != nil {
		return "", nil, err
	}
	parent := filepath.Dir(cleanDir)
	if err := os.MkdirAll(parent, extractDirMode); err != nil {
		return "", nil, fmt.Errorf("creating extraction directory: %w", err)
	}
	// Both directories are created and opened through the handle above them,
	// so neither is resolved by a pathname a writer could repoint between the
	// check and the open.
	parentRoot, err := os.OpenRoot(parent)
	if err != nil {
		return "", nil, fmt.Errorf(
			"opening extraction parent %s: %w", parent, err,
		)
	}
	defer parentRoot.Close()
	extractRoot, err := openExtractRoot(parentRoot, filepath.Base(cleanDir))
	if err != nil {
		return "", nil, fmt.Errorf("creating extraction directory: %w", err)
	}
	defer extractRoot.Close()
	immutableRoot, err := openExtractRoot(extractRoot, "immutable")
	if err != nil {
		return "", nil, fmt.Errorf("creating immutable directory: %w", err)
	}
	return filepath.Join(cleanDir, "immutable"), immutableRoot, nil
}

// removeImmutableTrio deletes the three files of an immutable file
// number so a corrupted download is not reused on resume.
//
// Resolved through the immutable directory's handle so a failed download can
// only ever unlink files in the directory it was writing into.
func removeImmutableTrio(root *os.Root, num uint64) {
	for _, ext := range immutableFileExtensions {
		_ = root.Remove(fmt.Sprintf("%05d.%s", num, ext))
	}
}

// sha256FileInRoot returns the hex SHA-256 digest and size of a file directly
// beneath root, resolved through the handle rather than by name.
func sha256FileInRoot(root *os.Root, name string) (string, int64, error) {
	f, err := root.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	return sha256Reader(f, name)
}

func sha256Reader(r io.Reader, name string) (string, int64, error) {
	hasher := sha256.New()
	size, err := io.Copy(hasher, r)
	if err != nil {
		return "", 0, fmt.Errorf("hashing %s: %w", name, err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}

// downloadAncillaryV2 downloads and extracts the v2 ancillary archive
// (ledger state plus the next in-progress immutable trio) and, when
// certificate verification is enabled, verifies the signed ancillary
// manifest. A verified bootstrap fails closed when this data is unavailable.
// downloadAncillaryV2 downloads, extracts and verifies the v2 ancillary archive
// and returns the extracted tree as the handle its manifest was checked
// through.
//
// Returning the handle rather than the directory's name is what lets the caller
// claim the tree is verified. A name would have to be reopened, and the tree
// behind it need not be the one the ancillary key signed by the time it is —
// the check and the claim would then be about different directories. The caller
// closes the result.
func downloadAncillaryV2(
	ctx context.Context,
	cfg BootstrapConfig,
	artifact *CardanoDatabaseSnapshot,
	downloadDir string,
) (*vettedDir, map[string]string, string, error) {
	if len(artifact.Ancillary.Locations) == 0 {
		return nil, nil, "", errors.New(
			"no ancillary locations in Cardano database snapshot",
		)
	}

	cfg.Logger.Info(
		"downloading ancillary data (ledger state)",
		"component", "mithril",
		"size", artifact.Ancillary.SizeUncompressed,
		"phase", "ancillary_download",
		"artifact", "ancillary_ledger_state",
		"destination", downloadDir,
	)

	ancillaryFilename := filepath.Base(fmt.Sprintf(
		"%s-%s-ancillary.tar.zst",
		artifact.Network,
		truncateDigest(artifact.Hash),
	))

	var ancillaryPath string
	var err error
	for i, loc := range artifact.Ancillary.Locations {
		if loc.URI == "" {
			continue
		}
		ancillaryPath, err = DownloadSnapshot(
			ctx, DownloadConfig{
				URL:      loc.URI,
				DestDir:  downloadDir,
				Filename: ancillaryFilename,
				Logger:   cfg.Logger,
				OnProgress: withProgressContext(
					cfg.OnProgress,
					"ancillary_ledger_state",
					artifact.Hash,
				),
				IdleTimeout:         cfg.DownloadIdleTimeout,
				MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
				MaxTransientRetries: cfg.DownloadMaxTransientRetries,
				AllowInsecureHTTP:   cfg.AllowInsecureHTTP,
			},
		)
		if err == nil {
			break
		}
		cfg.Logger.Warn(
			"ancillary download location failed, trying next",
			"component", "mithril",
			"location", i+1,
			"total", len(artifact.Ancillary.Locations),
			"error", err,
		)
	}
	if ancillaryPath == "" && err == nil {
		err = errors.New("no usable ancillary locations")
	}
	if err != nil {
		// The destination path, not the (empty) return of a failed download:
		// DownloadSnapshot resumes, so a failed attempt leaves a partial file
		// there for Cleanup to remove.
		//
		// Asked of the downloader rather than assembled here, so the two
		// cannot name different files — see downloadAncillary for what that
		// costs when the aggregator picks the network name.
		dest := downloadDestinationPath(DownloadConfig{
			DestDir:  downloadDir,
			Filename: ancillaryFilename,
		})
		return nil, nil, dest, fmt.Errorf(
			"downloading ancillary archive: %w",
			err,
		)
	}

	ancillaryDir := filepath.Join(
		downloadDir,
		filepath.Base("ancillary-"+artifact.Hash),
	)
	// Replace: the resume path above may have removed an unverified
	// extraction, but an interrupted run can still leave one behind.
	if _, extractErr := ExtractArchive(
		ctx,
		ancillaryPath,
		ancillaryDir,
		cfg.Logger.With(
			"phase", "ancillary_extraction",
			"artifact", "ancillary_ledger_state",
		),
		WithReplaceDestination(),
	); extractErr != nil {
		return nil, nil, ancillaryPath, fmt.Errorf(
			"extracting ancillary archive: %w",
			extractErr,
		)
	}

	// Vetted and then verified through one handle, which is the same handle
	// returned. Anything else leaves the manifest check describing a tree the
	// caller does not go on to read.
	extracted := ledgerDir(ancillaryDir)
	if extracted == nil {
		// Unverified bootstraps reach here too — verifyAncillaryExtraction is
		// a no-op for them — so an ancillary archive carrying no ledger state
		// has to be caught on its own.
		os.RemoveAll(ancillaryDir)
		// The archive path goes back even so, since it was downloaded and
		// still wants cleaning up.
		return nil, nil, ancillaryPath, fmt.Errorf(
			"extracted ancillary data at %s holds no ledger state",
			ancillaryDir,
		)
	}
	digests, verifyErr := verifyAncillaryExtraction(cfg, extracted)
	if verifyErr != nil {
		// Remove the unverified extraction so it cannot be
		// picked up by the resume path on a later run.
		extracted.Close()
		os.RemoveAll(ancillaryDir)
		// The extraction goes, the archive stays — and stays reported.
		// Cleanup removes the two separately, so an unverified manifest that
		// cleared this path would leave the download behind in a directory the
		// operator supplied and nothing else sweeps.
		return nil, nil, ancillaryPath, verifyErr
	}

	cfg.Logger.Info(
		"ancillary data extracted",
		"component", "mithril",
		"path", extracted.Path(),
	)

	return extracted, digests, ancillaryPath, nil
}

// verifyAncillaryExtraction checks a verified bootstrap's ancillary tree
// against the signed manifest.
//
// It takes the directory as the handle it was vetted through, and every read
// below — the ledger-state presence check, the manifest, the per-file digests,
// the completeness walk — resolves through that one handle. The handle then
// goes on to the ledger-state import, so what was verified and what is loaded
// are the same directory. Verifying by name would not carry that: the importer
// resolves the name again, and nothing links the tree it reads to the tree that
// satisfied the signature.
func verifyAncillaryExtraction(
	cfg BootstrapConfig,
	ancillary *vettedDir,
) (map[string]string, error) {
	if !cfg.VerifyCertificateChain {
		return nil, nil
	}
	if cfg.AncillaryVerificationKey == "" {
		return nil, errors.New(
			"ancillary verification key is required for verified bootstrap",
		)
	}
	if ancillary == nil {
		return nil, errors.New(
			"verified ancillary archive was not vetted",
		)
	}
	if !hasLedgerFilesIn(ancillary.Root()) {
		return nil, errors.New(
			"verified ancillary archive contains no ledger state",
		)
	}
	digests, err := verifyAncillaryManifest(
		ancillary.Root(), cfg.AncillaryVerificationKey,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"ancillary manifest verification failed: %w",
			err,
		)
	}
	cfg.Logger.Info(
		"ancillary manifest verified",
		"component", "mithril",
	)
	return digests, nil
}

// ancillaryManifest is the signed manifest shipped inside a v2
// ancillary archive.
type ancillaryManifest struct {
	Data      map[string]string `json:"data"`
	Signature string            `json:"signature"`
}

// computeHash matches the upstream Mithril ancillary manifest hash:
// sha256 over (path || digest-hex) pairs sorted by path, returned as
// the raw 32-byte sum (this is the Ed25519-signed message).
func (m *ancillaryManifest) computeHash() []byte {
	hasher := sha256.New()
	for _, path := range slices.Sorted(maps.Keys(m.Data)) {
		hasher.Write([]byte(path))
		hasher.Write([]byte(m.Data[path]))
	}
	return hasher.Sum(nil)
}

// verifyAncillaryManifest reads the ancillary manifest in dir,
// verifies the Ed25519 signature over its file digest map with the
// configured ancillary verification key, and checks every listed
// file's SHA-256 digest.
//
// It returns the signed digest map on success. The caller carries it to the
// import, because this pass hashes each file and closes it while the import
// opens the state and table it selects afterwards — so the bytes that get
// parsed have to be checked against these digests again, from the descriptors
// the import reads through.
func verifyAncillaryManifest(
	root *os.Root,
	ancillaryVerificationKey string,
) (map[string]string, error) {
	data, err := readFileIn(root, ancillaryManifestFilename)
	if err != nil {
		return nil, fmt.Errorf("reading ancillary manifest: %w", err)
	}
	var manifest ancillaryManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parsing ancillary manifest: %w", err)
	}
	if len(manifest.Data) == 0 {
		return nil, errors.New("ancillary manifest lists no files")
	}
	if manifest.Signature == "" {
		return nil, errors.New("ancillary manifest has no signature")
	}

	key, err := ParseVerificationKey(ancillaryVerificationKey)
	if err != nil {
		return nil, fmt.Errorf(
			"parsing ancillary verification key: %w",
			err,
		)
	}
	if len(key.RawKeyBytes) != ed25519.PublicKeySize {
		return nil, fmt.Errorf(
			"ancillary verification key has unexpected size %d",
			len(key.RawKeyBytes),
		)
	}
	signature, err := decodeHexString(manifest.Signature)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding ancillary manifest signature: %w",
			err,
		)
	}
	if len(signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf(
			"ancillary manifest signature has unexpected size %d",
			len(signature),
		)
	}
	if !ed25519.Verify(
		ed25519.PublicKey(key.RawKeyBytes),
		manifest.computeHash(),
		signature,
	) {
		return nil, errors.New("ancillary manifest signature is invalid")
	}

	for _, relPath := range slices.Sorted(maps.Keys(manifest.Data)) {
		if !filepath.IsLocal(relPath) {
			return nil, fmt.Errorf(
				"ancillary manifest contains non-local path %q",
				relPath,
			)
		}
		sum, err := sha256FileIn(root, relPath)
		if err != nil {
			return nil, fmt.Errorf(
				"hashing ancillary file %s: %w",
				relPath,
				err,
			)
		}
		if sum != manifest.Data[relPath] {
			return nil, fmt.Errorf(
				"ancillary file %s digest mismatch",
				relPath,
			)
		}
	}
	// The signed manifest must cover the complete extracted payload. An
	// attacker must not be able to add an unlisted ledger or immutable file
	// that the importer could later select.
	if err := fs.WalkDir(
		root.FS(),
		".",
		func(relPath string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if relPath == ancillaryManifestFilename {
				return nil
			}
			if _, ok := manifest.Data[relPath]; !ok {
				return fmt.Errorf(
					"ancillary file %s is not covered by manifest",
					relPath,
				)
			}
			return nil
		},
	); err != nil {
		return nil, err
	}
	return manifest.Data, nil
}

// readFileIn reads a slash-separated path relative to root.
func readFileIn(root *os.Root, rel string) ([]byte, error) {
	f, err := root.Open(filepath.FromSlash(rel))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// sha256FileIn hashes a slash-separated path relative to root.
//
// Through the handle rather than by name, because the digest has to describe
// the file in the tree that was vetted. Hashing a name would leave the manifest
// check and everything downstream of it about whatever occupied that name at
// two different instants.
func sha256FileIn(root *os.Root, rel string) (string, error) {
	f, err := root.Open(filepath.FromSlash(rel))
	if err != nil {
		return "", err
	}
	defer f.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", fmt.Errorf("hashing %s: %w", rel, err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
