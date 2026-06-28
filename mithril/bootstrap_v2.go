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
	client := NewClient(aggregatorURL)

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

	cfg.Logger.Info(
		"found latest Cardano database snapshot",
		"component", "mithril",
		"hash", artifact.Hash,
		"epoch", artifact.Beacon.Epoch,
		"immutable_file_number", artifact.Beacon.ImmutableFileNumber,
		"total_db_size", artifact.TotalDbSizeUncompressed,
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
		"immutable-"+artifact.Hash,
	)
	var ancillaryDir string
	var ancillaryArchivePath string

	// Steps 4+5: Download immutable archives and the ancillary
	// archive in parallel. The ancillary download is non-fatal; the
	// node can close the gap from the network without ledger state.
	ancCtx, ancCancel := context.WithCancel(ctx)
	var ancWg sync.WaitGroup
	defer ancWg.Wait()
	defer ancCancel()
	if len(artifact.Ancillary.Locations) > 0 {
		ancWg.Go(func() {
			candidateDir := filepath.Join(
				downloadDir,
				"ancillary-"+artifact.Hash,
			)
			candidateArchive := filepath.Join(
				downloadDir,
				fmt.Sprintf(
					"%s-%s-ancillary.tar.zst",
					artifact.Network,
					truncateDigest(artifact.Hash),
				),
			)
			if hasLedgerFiles(candidateDir) {
				if err := verifyAncillaryExtraction(
					cfg, candidateDir,
				); err != nil {
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
						"path", candidateDir,
					)
					ancillaryDir = candidateDir
					if _, err := os.Stat(candidateArchive); err == nil {
						ancillaryArchivePath = candidateArchive
					}
					return
				}
			}
			dir, archPath, ancErr := downloadAncillaryV2(
				ancCtx, cfg, artifact, downloadDir,
			)
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
			ancillaryDir = dir
			ancillaryArchivePath = archPath
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
	immutableDir := filepath.Join(extractDir, "immutable")
	if !hasChunkFiles(immutableDir) {
		return nil, fmt.Errorf(
			"immutable DB directory not found at %s after download",
			immutableDir,
		)
	}

	// Wait for ancillary download to finish (also deferred above
	// for the error-return path; calling Wait twice is safe).
	ancWg.Wait()

	cfg.Logger.Info(
		"Mithril bootstrap ready for loading",
		"component", "mithril",
		"immutable_dir", immutableDir,
		"ancillary_dir", ancillaryDir,
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
		ImmutableDir:         immutableDir,
		ExtractDir:           extractDir,
		AncillaryDir:         ancillaryDir,
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
	_ = os.Remove(filepath.Join(downloadDir, fmt.Sprintf(
		"digests-%s.tar.zst",
		suffix,
	)))
	_ = os.RemoveAll(
		filepath.Join(downloadDir, "digests-"+suffix),
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
			Filename: fmt.Sprintf(
				"digests-%s.tar.zst",
				truncateDigest(artifact.Hash),
			),
			Logger:              cfg.Logger,
			IdleTimeout:         cfg.DownloadIdleTimeout,
			MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
			MaxTransientRetries: cfg.DownloadMaxTransientRetries,
		},
	)
	if err != nil {
		return nil, err
	}
	destDir := filepath.Join(
		downloadDir,
		"digests-"+truncateDigest(artifact.Hash),
	)
	if _, err := ExtractArchive(
		ctx, archivePath, destDir, cfg.Logger,
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
	data, err := os.ReadFile(jsonPath) //nolint:gosec // path is constructed from our own extraction directory
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
	immutableDir := filepath.Join(extractDir, "immutable")
	if err := os.MkdirAll(immutableDir, 0o750); err != nil {
		return fmt.Errorf("creating immutable directory: %w", err)
	}
	archiveDir := filepath.Join(
		downloadDir,
		"immutable-archives-"+truncateDigest(artifact.Hash),
	)
	if err := os.MkdirAll(archiveDir, 0o750); err != nil {
		return fmt.Errorf("creating archive directory: %w", err)
	}

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
	// Progress denominator must be the immutable-only uncompressed size,
	// not TotalDbSizeUncompressed. The latter covers the whole database
	// (immutable archives plus the ancillary ledger state), but this pool
	// downloads only the immutable archives — the ancillary archive runs
	// concurrently and reports its own progress. Using the whole-DB size
	// here would cap immutable progress below 100%. Fall back to the
	// whole-DB size only when the per-file average is unavailable.
	immutableTotalBytes := artifact.Immutables.AverageSizeUncompressed *
		int64(totalArchives) // #nosec G115 -- archive count, non-negative
	if immutableTotalBytes <= 0 {
		immutableTotalBytes = artifact.TotalDbSizeUncompressed
	}
	onArchiveDone := newImmutableProgress(
		cfg, totalArchives, immutableTotalBytes,
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
	var seq *inOrderSequencer
	if cfg.OnChunkContiguous != nil {
		seq = newInOrderSequencer(
			totalArchives,
			func(num uint64) error {
				return cfg.OnChunkContiguous(immutableDir, num)
			},
		)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(immutableDownloadWorkers)
	for num := range totalArchives {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			if bytes, err := checkImmutableTrio(
				immutableDir, num, digests,
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
					removeImmutableTrio(immutableDir, num)
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
					immutableDir, num, digests,
				)
				if lastErr != nil {
					removeImmutableTrio(immutableDir, num)
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
	err := g.Wait()
	if seq != nil {
		// A fetch failure means the contiguous prefix can never advance
		// past the gap; cancel so the consumer's Wait unblocks. On
		// success, Wait drains any processing still trailing the last
		// downloads.
		if err != nil {
			seq.Cancel(err)
		}
		if procErr := seq.Wait(); procErr != nil && err == nil {
			err = procErr
		}
	}
	return err
}

// newImmutableProgress returns a callback that aggregates per-archive
// completion into OnProgress updates and throttled log lines.
func newImmutableProgress(
	cfg BootstrapConfig,
	totalArchives uint64,
	totalBytes int64,
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
				BytesDownloaded: doneBytes,
				TotalBytes:      totalBytes,
				Percent:         percent,
				BytesPerSecond:  speed,
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
			fmt.Sprintf(
				"immutable archives: %d/%d (%.1f%%)",
				doneArchives,
				totalArchives,
				archivePercent,
			),
			"component", "mithril",
		)
		lastLog = now
		lastLoggedPercent = archivePercent
	}
}

// fetchImmutableArchive downloads and extracts a single immutable
// archive, then removes the archive file to bound disk usage.
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
	archivePath, err := DownloadSnapshot(
		ctx, DownloadConfig{
			URL:                 location.ImmutableArchiveURI(num),
			DestDir:             archiveDir,
			Filename:            filepath.Base(immutableArchivePath(archiveDir, num)),
			Logger:              dlLogger,
			HTTPClient:          cfg.httpClient,
			IdleTimeout:         cfg.DownloadIdleTimeout,
			MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
			MaxTransientRetries: cfg.DownloadMaxTransientRetries,
		},
	)
	if err != nil {
		return err
	}
	if _, err := ExtractArchive(
		ctx, archivePath, extractDir, extractLogger,
	); err != nil {
		return fmt.Errorf("extracting: %w", err)
	}
	if err := os.Remove(archivePath); err != nil {
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
func checkImmutableTrio(
	dir string,
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
		sum, size, err := sha256File(filepath.Join(dir, name))
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

// removeImmutableTrio deletes the three files of an immutable file
// number so a corrupted download is not reused on resume.
func removeImmutableTrio(dir string, num uint64) {
	for _, ext := range immutableFileExtensions {
		path := filepath.Join(dir, fmt.Sprintf("%05d.%s", num, ext))
		_ = os.Remove(path) //nolint:gosec // path is constructed from our own extraction directory
	}
}

// sha256File returns the hex SHA-256 digest and size of a file.
func sha256File(path string) (string, int64, error) {
	f, err := os.Open(path) //nolint:gosec // callers construct the path from controlled directories
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return "", 0, fmt.Errorf("hashing %s: %w", path, err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}

// downloadAncillaryV2 downloads and extracts the v2 ancillary archive
// (ledger state plus the next in-progress immutable trio) and, when
// certificate verification is enabled, verifies the signed ancillary
// manifest. Mirrors the v1 non-fatal contract: the caller logs and
// continues without ledger state on error.
func downloadAncillaryV2(
	ctx context.Context,
	cfg BootstrapConfig,
	artifact *CardanoDatabaseSnapshot,
	downloadDir string,
) (string, string, error) {
	if len(artifact.Ancillary.Locations) == 0 {
		return "", "", errors.New(
			"no ancillary locations in Cardano database snapshot",
		)
	}

	cfg.Logger.Info(
		"downloading ancillary data (ledger state)",
		"component", "mithril",
		"size", artifact.Ancillary.SizeUncompressed,
	)

	ancillaryFilename := fmt.Sprintf(
		"%s-%s-ancillary.tar.zst",
		artifact.Network,
		truncateDigest(artifact.Hash),
	)

	var ancillaryPath string
	var err error
	for i, loc := range artifact.Ancillary.Locations {
		if loc.URI == "" {
			continue
		}
		ancillaryPath, err = DownloadSnapshot(
			ctx, DownloadConfig{
				URL:                 loc.URI,
				DestDir:             downloadDir,
				Filename:            ancillaryFilename,
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
		return "", "", fmt.Errorf(
			"downloading ancillary archive: %w",
			err,
		)
	}

	ancillaryDir := filepath.Join(
		downloadDir,
		"ancillary-"+artifact.Hash,
	)
	if _, extractErr := ExtractArchive(
		ctx, ancillaryPath, ancillaryDir, cfg.Logger,
	); extractErr != nil {
		return "", "", fmt.Errorf(
			"extracting ancillary archive: %w",
			extractErr,
		)
	}

	if err := verifyAncillaryExtraction(cfg, ancillaryDir); err != nil {
		// Remove the unverified extraction so it cannot be
		// picked up by the resume path on a later run.
		os.RemoveAll(ancillaryDir)
		return "", "", err
	}

	cfg.Logger.Info(
		"ancillary data extracted",
		"component", "mithril",
		"path", ancillaryDir,
	)

	return ancillaryDir, ancillaryPath, nil
}

func verifyAncillaryExtraction(
	cfg BootstrapConfig,
	ancillaryDir string,
) error {
	if !cfg.VerifyCertificateChain {
		return nil
	}
	if cfg.AncillaryVerificationKey == "" {
		cfg.Logger.Warn(
			"no ancillary verification key configured, "+
				"skipping ancillary manifest verification",
			"component", "mithril",
		)
		return nil
	}
	if err := verifyAncillaryManifest(
		ancillaryDir, cfg.AncillaryVerificationKey,
	); err != nil {
		return fmt.Errorf(
			"ancillary manifest verification failed: %w",
			err,
		)
	}
	cfg.Logger.Info(
		"ancillary manifest verified",
		"component", "mithril",
	)
	return nil
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
func verifyAncillaryManifest(
	dir string,
	ancillaryVerificationKey string,
) error {
	data, err := os.ReadFile( //nolint:gosec // path rooted in our own extraction directory
		filepath.Join(dir, ancillaryManifestFilename),
	)
	if err != nil {
		return fmt.Errorf("reading ancillary manifest: %w", err)
	}
	var manifest ancillaryManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("parsing ancillary manifest: %w", err)
	}
	if len(manifest.Data) == 0 {
		return errors.New("ancillary manifest lists no files")
	}
	if manifest.Signature == "" {
		return errors.New("ancillary manifest has no signature")
	}

	key, err := ParseVerificationKey(ancillaryVerificationKey)
	if err != nil {
		return fmt.Errorf(
			"parsing ancillary verification key: %w",
			err,
		)
	}
	if len(key.RawKeyBytes) != ed25519.PublicKeySize {
		return fmt.Errorf(
			"ancillary verification key has unexpected size %d",
			len(key.RawKeyBytes),
		)
	}
	signature, err := decodeHexString(manifest.Signature)
	if err != nil {
		return fmt.Errorf(
			"decoding ancillary manifest signature: %w",
			err,
		)
	}
	if len(signature) != ed25519.SignatureSize {
		return fmt.Errorf(
			"ancillary manifest signature has unexpected size %d",
			len(signature),
		)
	}
	if !ed25519.Verify(
		ed25519.PublicKey(key.RawKeyBytes),
		manifest.computeHash(),
		signature,
	) {
		return errors.New("ancillary manifest signature is invalid")
	}

	for _, relPath := range slices.Sorted(maps.Keys(manifest.Data)) {
		if !filepath.IsLocal(relPath) {
			return fmt.Errorf(
				"ancillary manifest contains non-local path %q",
				relPath,
			)
		}
		sum, _, err := sha256File(
			filepath.Join(dir, filepath.FromSlash(relPath)),
		)
		if err != nil {
			return fmt.Errorf(
				"hashing ancillary file %s: %w",
				relPath,
				err,
			)
		}
		if sum != manifest.Data[relPath] {
			return fmt.Errorf(
				"ancillary file %s digest mismatch",
				relPath,
			)
		}
	}
	return nil
}
