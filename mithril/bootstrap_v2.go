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
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// immutableDownloadWorkers is the number of concurrent immutable
// archive downloads during a v2 bootstrap, and a chunk feed's default.
// Paired with a shared keep-alive client (see ChunkFeed.start) so each
// worker holds a warm pooled connection; the connection pool in
// newPooledDownloadTransport is sized to this value.
const immutableDownloadWorkers = 48

// Compressed-size limits for one v2 object, used when no operator limit is
// set. They are constants rather than values read from the artifact because
// the aggregator chooses the artifact's advertised sizes.
const (
	// A mainnet immutable archive is tens of megabytes compressed.
	immutableArchiveMaxBytes int64 = 1 << 30
	// The digest list is JSON that grows by a few hundred bytes per immutable;
	// mainnet holds about 3 MB of it.
	digestListMaxBytes int64 = 256 << 20
	// digestListMaxMemberBytes bounds the expanded JSON, with room for years
	// of growth over today's size.
	digestListMaxMemberBytes int64 = 64 << 20
	// The ancillary archive holds the ledger state, which is the largest v2
	// object.
	ancillaryMaxBytes int64 = 64 << 30
	// Ancillary data holds the signed ledger state and in-progress trio.
	ancillaryExpandedMaxBytes int64 = 64 << 30
	maxAncillaryEntries             = 32 << 10

	// immutableInflightBytes bounds the compressed bytes the immutable pool
	// may have in flight, counting each download at its size limit. At the
	// default limit the whole pool fits; raising the limit lowers concurrency
	// until only one download fits. An override above this budget requires
	// enough capacity for that single download.
	immutableInflightBytes = immutableDownloadWorkers * immutableArchiveMaxBytes
)

// ancillaryMemberPattern matches the immutable trio an ancillary archive may
// carry alongside its ledger state.
var ancillaryMemberPattern = regexp.MustCompile(
	`^immutable/[0-9]+\.(chunk|primary|secondary)$`,
)

func immutableDownloadBudget(cfg BootstrapConfig) (*semaphore.Weighted, int64) {
	weight := cfg.objectMaxBytes(immutableArchiveMaxBytes)
	return semaphore.NewWeighted(max(immutableInflightBytes, weight)), weight
}

// immutableArchiveLimits admits only an immutable archive's own certified
// trio under immutable/, and hashes each file as it is written. Any other
// member, including a ledger/ tree, is refused: the ledger state is imported
// from the ancillary archive only.
func immutableArchiveLimits(
	num uint64,
	digests map[string]string,
) archiveLimits {
	own := map[string]string{}
	for _, ext := range immutableFileExtensions {
		name := fmt.Sprintf("%05d.%s", num, ext)
		if digest, ok := digests[name]; ok {
			own["immutable/"+name] = digest
		}
	}
	return archiveLimits{
		maxEntries:     64,
		maxMemberBytes: maxExtractFileSize,
		maxTotalBytes:  maxTotalExtractSize,
		digests:        own,
		allow: func(name string) bool {
			if _, ok := own[name]; ok {
				return true
			}
			return name == "immutable"
		},
	}
}

// digestListLimits admits a single top-level JSON file.
func digestListLimits() archiveLimits {
	return archiveLimits{
		maxEntries:     4,
		maxMemberBytes: digestListMaxMemberBytes,
		maxTotalBytes:  digestListMaxMemberBytes,
		allow: func(name string) bool {
			return !strings.Contains(name, "/") &&
				strings.HasSuffix(name, ".json")
		},
	}
}

// ancillaryLimits admits the signed manifest, the ledger tree and the next
// in-progress immutable trio.
func ancillaryLimits() archiveLimits {
	return archiveLimits{
		maxEntries:     maxAncillaryEntries,
		maxMemberBytes: maxExtractFileSize,
		maxTotalBytes:  ancillaryExpandedMaxBytes,
		allow: func(name string) bool {
			return name == ancillaryManifestFilename ||
				name == "ledger" || strings.HasPrefix(name, "ledger/") ||
				name == "immutable" || ancillaryMemberPattern.MatchString(name)
		},
	}
}

func withBootstrapExtractionBudget(
	limits archiveLimits,
	budget *bootstrapExtractionBudget,
) archiveLimits {
	limits.budget = budget
	return limits
}

// objectMaxBytes is the compressed-size limit for a v2 object whose built-in
// limit is def. An operator-set DownloadMaxBytes replaces it.
func (cfg BootstrapConfig) objectMaxBytes(def int64) int64 {
	if cfg.DownloadMaxBytes > 0 {
		return cfg.DownloadMaxBytes
	}
	return def
}

// ancillaryManifestFilename is the signed manifest file inside a v2
// ancillary archive.
const ancillaryManifestFilename = "ancillary_manifest.json"

// immutableFileExtensions are the three files of an immutable trio.
var immutableFileExtensions = []string{"chunk", "primary", "secondary"}

// selectArtifactV2 fetches the selected artifact -- the pinned one for a
// resumed sync, otherwise the latest -- and checks its self-hash, identity and
// network. It does not authenticate the artifact against a certificate.
func selectArtifactV2(
	ctx context.Context,
	cfg BootstrapConfig,
	client *Client,
) (*CardanoDatabaseSnapshot, error) {
	var artifact *CardanoDatabaseSnapshot
	var err error
	if cfg.PinnedDigest != "" {
		artifact, err = client.GetCardanoDatabaseSnapshot(
			ctx, cfg.PinnedDigest,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"fetching pinned Cardano database snapshot %s: %w",
				cfg.PinnedDigest,
				err,
			)
		}
		if artifact != nil && artifact.Hash != cfg.PinnedDigest {
			return nil, fmt.Errorf(
				"aggregator returned Cardano database snapshot %s for "+
					"pinned artifact %s",
				artifact.Hash,
				cfg.PinnedDigest,
			)
		}
	} else {
		artifact, err = client.GetLatestCardanoDatabaseSnapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf(
				"fetching latest Cardano database snapshot: %w",
				err,
			)
		}
	}
	if artifact == nil {
		return nil, errors.New(
			"aggregator returned no Cardano database snapshot",
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
	if artifact.Beacon.ImmutableFileNumber >= maxCardanoDatabaseArchives {
		return nil, fmt.Errorf(
			"cardano database snapshot has too many immutable archives: %d",
			artifact.Beacon.ImmutableFileNumber+1,
		)
	}
	locationPolicy, err := newArtifactLocationPolicy(
		client.aggregatorURL, cfg.AllowInsecureHTTP,
	)
	if err != nil {
		return nil, err
	}
	if err := locationPolicy.apply(artifact); err != nil {
		return nil, err
	}
	return artifact, nil
}

// newArtifactDownloadClient returns a keep-alive client for artifact
// downloads, pooling connections for workers concurrent fetches. Its redirects
// are held to the artifact location policy for aggregatorURL, and when budget
// is set every request and body byte it carries is charged to budget.
func newArtifactDownloadClient(
	aggregatorURL string,
	allowInsecureHTTP bool,
	workers int,
	budget *artifactDownloadBudget,
) (*http.Client, *restrictedHTTPTransport, error) {
	policy, err := newArtifactLocationPolicy(aggregatorURL, allowInsecureHTTP)
	if err != nil {
		return nil, nil, err
	}
	transport := newPooledDownloadTransport(workers, allowInsecureHTTP)
	transport.downloadBudget = budget
	return &http.Client{
		Transport:     transport,
		CheckRedirect: policy.checkRedirect,
	}, transport, nil
}

// withOneShotDownloadClient returns bcfg with the client, download budget and
// extraction budget for the downloads of a run that fetches its immutable
// files through chunk feeds: the digest list and, with ancillary set, the
// ancillary archive. The returned func releases the client's idle connections.
func withOneShotDownloadClient(
	bcfg BootstrapConfig,
	aggregatorURL string,
	downloadDir string,
	ancillary bool,
) (BootstrapConfig, func(), error) {
	objects := uint64(1)
	var ancillaryBytes int64
	if ancillary {
		objects++
		ancillaryBytes = bcfg.objectMaxBytes(ancillaryMaxBytes)
	}
	budget, err := newArtifactDownloadBudget(
		objects, 0, bcfg.objectMaxBytes(digestListMaxBytes), ancillaryBytes,
	)
	if err != nil {
		return bcfg, nil, err
	}
	extraction, err := newBootstrapExtractionBudget(downloadDir)
	if err != nil {
		return bcfg, nil, err
	}
	httpClient, transport, err := newArtifactDownloadClient(
		aggregatorURL, bcfg.AllowInsecureHTTP, 1, budget,
	)
	if err != nil {
		return bcfg, nil, err
	}
	bcfg.httpClient = httpClient
	bcfg.downloadBudget = budget
	bcfg.extractionBudget = extraction
	bcfg.decoderBudget = newDecoderMemoryBudget()
	return bcfg, transport.CloseIdleConnections, nil
}

// bootstrapV2 restores a node database from Mithril CardanoDatabase
// (v2) artifacts: per-immutable-file archives verified against the
// certified merkle root, plus an ancillary archive carrying the
// ledger state verified via the signed ancillary manifest. Its
// result carries the verified immutable and ancillary handles.
func bootstrapV2(
	ctx context.Context,
	cfg BootstrapConfig,
	aggregatorURL string,
) (*BootstrapResult, error) {
	client := newMithrilClient(aggregatorURL, cfg.AllowInsecureHTTP)

	// Step 1: Fetch the selected artifact, verify its self-hash and hold its
	// locations to the artifact location policy.
	artifact, err := selectArtifactV2(ctx, cfg, client)
	if err != nil {
		return nil, err
	}
	// The v2 flow imports ledger state only from the ancillary artifact. Refuse
	// an incomplete artifact before downloading its immutable files.
	if len(artifact.Ancillary.Locations) == 0 {
		return nil, errors.New(
			"mithril bootstrap requires ancillary locations",
		)
	}
	// A certificate authenticates the immutable database content, while the
	// ledger state and the final in-progress immutable trio are authenticated
	// by the separately signed ancillary manifest. Do not start downloading
	// immutable data when the second half of that trust boundary cannot be
	// checked.
	if cfg.VerifyCertificateChain {
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

	// Report the selected artifact before the first download so the caller can
	// pin it durably; an interruption anywhere below then has an identity to
	// resume against, including the artifact-keyed download cache.
	if cfg.OnArtifactSelected != nil {
		if err := cfg.OnArtifactSelected(SelectedArtifact{
			Backend:         BackendV2,
			Network:         artifact.Network,
			Digest:          artifact.Hash,
			Beacon:          artifact.Beacon,
			CertificateHash: artifact.CertificateHash,
		}); err != nil {
			return nil, err
		}
	}

	totalArchives := artifact.Beacon.ImmutableFileNumber + 1
	downloadArchives := totalArchives - min(cfg.StartImmutable, totalArchives)
	immutableBytes, ok := multiplyWithinInt64(
		cfg.objectMaxBytes(immutableArchiveMaxBytes), downloadArchives,
	)
	if !ok {
		return nil, errArtifactDownloadBudget
	}
	artifactObjects := downloadArchives + 1
	var ancillaryBytes int64
	if len(artifact.Ancillary.Locations) > 0 {
		artifactObjects++
		ancillaryBytes = cfg.objectMaxBytes(ancillaryMaxBytes)
	}
	cfg.downloadBudget, err = newArtifactDownloadBudget(
		artifactObjects,
		immutableBytes,
		cfg.objectMaxBytes(digestListMaxBytes),
		ancillaryBytes,
	)
	if err != nil {
		return nil, err
	}
	var downloadTransport *restrictedHTTPTransport
	cfg.httpClient, downloadTransport, err = newArtifactDownloadClient(
		aggregatorURL,
		cfg.AllowInsecureHTTP,
		immutableDownloadWorkers,
		cfg.downloadBudget,
	)
	if err != nil {
		return nil, err
	}
	defer downloadTransport.CloseIdleConnections()
	client.httpClient = aggregatorHTTPClient(cfg.httpClient)
	downloadCtx, cancelDownloads := context.WithTimeout(
		ctx, artifactDownloadTotalTimeout,
	)
	defer cancelDownloads()
	ctx = downloadCtx

	// Step 2: Set up download directory
	downloadDir := cfg.DownloadDir
	createdTempDir := false
	var tempRoot *os.Root
	var tempOwned *bootstrapDir
	var downloadOwned *bootstrapDir
	if downloadDir == "" {
		var err error
		downloadDir, tempOwned, err = createBootstrapDir(
			"", "dingo-mithril-*",
		)
		if err != nil {
			return nil, fmt.Errorf(
				"creating temp directory: %w",
				err,
			)
		}
		createdTempDir = true
		tempRoot = tempOwned.root
	} else {
		var err error
		downloadDir, downloadOwned, err = retainOrCreateBootstrapDir(downloadDir)
		if err != nil {
			return nil, fmt.Errorf(
				"retaining bootstrap download directory %s: %w",
				downloadDir,
				err,
			)
		}
	}
	downloadRoot, err := os.OpenRoot(downloadDir)
	if err != nil {
		if createdTempDir && tempOwned != nil {
			tempOwned.root = tempRoot
			_ = tempOwned.cleanup()
		} else if downloadOwned != nil {
			_ = downloadOwned.close()
		}
		return nil, fmt.Errorf(
			"retaining bootstrap download directory %s: %w",
			downloadDir,
			err,
		)
	}
	// Clean up temp dir on error to avoid leaking disk space
	success := false
	defer func() {
		if downloadRoot != nil {
			_ = downloadRoot.Close()
			downloadRoot = nil
		}
		if !success && createdTempDir {
			tempOwned.root = tempRoot
			_ = tempOwned.cleanup()
			tempRoot = tempOwned.root
		}
		if downloadOwned != nil {
			_ = downloadOwned.close()
			downloadOwned = nil
		}
	}()
	cfg.extractionBudget, err = newBootstrapExtractionBudget(downloadDir)
	if err != nil {
		return nil, err
	}
	cfg.downloadSpaceBudget, err = newDownloadSpaceBudget(downloadDir)
	if err != nil {
		return nil, err
	}
	cfg.decoderBudget = newDecoderMemoryBudget()

	// The ancillary download and the immutable archive
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
	var ancillaryArchive *bootstrapFile
	var ancillaryErr error
	// Closed only when the result that would own it is never returned; on
	// success it is carried to the ledger-state import and released by Cleanup.
	defer func() {
		if !success {
			ancillaryTree.Close()
			_ = ancillaryArchive.close()
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
					cfg.Logger.Warn(
						"cached ancillary data failed "+
							"verification, redownloading",
						"component", "mithril",
						"path", candidateDir,
						"error", err,
					)
					if err := cleanupVettedDir(cached); err != nil {
						cfg.Logger.Warn(
							"failed to remove unverified ancillary data",
							"component", "mithril",
							"path", candidateDir,
							"error", err,
						)
					}
					archiveRoot, openErr := downloadRoot.OpenRoot(".")
					var cachedArchive *bootstrapFile
					if openErr == nil {
						cachedArchive, openErr = retainBootstrapFile(
							candidateArchive,
							archiveRoot,
						)
					}
					if openErr == nil {
						openErr = cachedArchive.cleanup()
					}
					if openErr != nil && !errors.Is(openErr, os.ErrNotExist) {
						cfg.Logger.Warn(
							"failed to remove stale ancillary archive",
							"component", "mithril",
							"path", candidateArchive,
							"error", openErr,
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
					archiveRoot, openErr := downloadRoot.OpenRoot(".")
					if openErr == nil {
						ancillaryArchive, openErr = retainBootstrapFile(
							candidateArchive,
							archiveRoot,
						)
					}
					if openErr != nil && !errors.Is(openErr, os.ErrNotExist) {
						ancillaryErr = fmt.Errorf(
							"retaining cached ancillary archive: %w",
							openErr,
						)
					}
					return
				}
			}
			tree, treeDigests, archive, ancErr := downloadAncillaryV2(
				ancCtx, cfg, artifact, downloadDir,
			)
			// Recorded whether or not the tree turned out usable, so a
			// downloaded archive still gets cleaned up.
			if archive != nil {
				ancillaryArchive = archive
			}
			if ancErr != nil {
				ancillaryErr = ancErr
				// A fully downloaded archive that failed extraction or
				// verification is removed here because no result is
				// returned for Cleanup to act on. Left in place, the next
				// run's resume request is answered 416 with a matching
				// total and the same bad archive is accepted again. A
				// failed download keeps its partial file so it can resume,
				// and a cancelled run keeps its archive: an interrupted
				// extraction says nothing about the bytes.
				var dlErr *ancillaryDownloadError
				if archive != nil && ancCtx.Err() == nil &&
					!errors.As(ancErr, &dlErr) {
					if cleanupErr := archive.cleanup(); cleanupErr != nil {
						cfg.Logger.Warn(
							"failed to remove bad ancillary archive",
							"component", "mithril",
							"path", archive.path,
							"error", cleanupErr,
						)
					} else {
						ancillaryArchive = nil
					}
				}
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
		ctx, cfg, client, artifact, digests, downloadDir, extractDir,
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
		return nil, fmt.Errorf(
			"mithril ancillary data unavailable: %w",
			ancillaryErr,
		)
	}
	if ancillaryTree == nil {
		return nil, errors.New(
			"mithril bootstrap produced no ancillary data",
		)
	}

	cfg.Logger.Info(
		"Mithril bootstrap ready for loading",
		"component", "mithril",
		"phase", "bootstrap_ready",
		"immutable_dir", immutableTree.Path(),
		"ancillary_dir", ancillaryTree.Path(),
	)

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
		ImmutableDir:        immutableTree.Path(),
		ImmutableRoot:       immutableTree.Root(),
		CertificateVerified: cfg.VerifyCertificateChain,
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
		AncillaryVerified: ancillaryTree != nil &&
			cfg.VerifyCertificateChain,
		AncillaryDigests: ancillaryDigests,
	}
	if ancillaryArchive != nil {
		result.AncillaryArchivePath = ancillaryArchive.path
		result.ancillaryArchiveRoot = ancillaryArchive.root
		result.ancillaryArchiveInfo = ancillaryArchive.info
		result.ancillaryArchiveGuard = ancillaryArchive.guard
		ancillaryArchive.root = nil
		ancillaryArchive.guard = nil
	}
	if createdTempDir {
		result.TempDir = downloadDir
		result.TempRoot = tempRoot
		result.tempOwned = tempOwned
	}
	success = true
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
	cache := cfg.CertificateCache
	if leaf := cache.acceptedLeaf(
		cfg.GenesisVerificationKey, artifact.CertificateHash,
	); leaf != nil {
		cfg.Logger.Debug(
			"certificate chain already verified",
			"component", "mithril",
			"phase", "certificate_verification",
			"artifact", "cardano_database",
			"certificate_hash", artifact.CertificateHash,
		)
		kind, err := leaf.SignedEntityType.Kind()
		if err != nil {
			return fmt.Errorf(
				"certificate %s has invalid signed entity type: %w",
				leaf.Hash, err,
			)
		}
		return checkLeafBindsArtifact(leaf, kind, artifact)
	}
	cfg.Logger.Info(
		"verifying certificate chain",
		"component", "mithril",
		"phase", "certificate_verification",
		"artifact", "cardano_database",
		"certificate_hash", artifact.CertificateHash,
	)
	var anchors *certificateAnchors
	if cache != nil {
		anchors = &certificateAnchors{
			cache: cache, genesisKey: cfg.GenesisVerificationKey,
		}
	}
	verificationResult, err := verifyCertificateChainAnchored(
		ctx,
		client,
		artifact.CertificateHash,
		VerificationModeSTM,
		newCertificateChainBudget(),
		anchors,
	)
	if err != nil {
		return fmt.Errorf(
			"certificate chain verification failed: %w",
			err,
		)
	}
	if verificationResult == nil ||
		verificationResult.GenesisCertificate == nil {
		return errors.New(
			"verified certificate chain has no genesis certificate",
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
	if err := checkLeafBindsArtifact(
		verificationResult.LeafCertificate,
		verificationResult.SignedEntityKind,
		artifact,
	); err != nil {
		return err
	}
	cache.accept(cfg.GenesisVerificationKey, verificationResult)
	return nil
}

// checkLeafBindsArtifact checks that a verified leaf certificate certifies
// artifact: its signed entity kind, merkle root, network and beacon.
func checkLeafBindsArtifact(
	leaf *Certificate,
	kind string,
	artifact *CardanoDatabaseSnapshot,
) error {
	if kind != signedEntityTypeCardanoDatabase {
		return fmt.Errorf(
			"unexpected signed entity kind for Cardano database bootstrap: %s",
			kind,
		)
	}
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

// downloadDigestsArchive downloads the snapshot-specific digest
// archive (tar.zst containing a single JSON file) and parses it.
func downloadDigestsArchive(
	ctx context.Context,
	cfg BootstrapConfig,
	uri string,
	artifact *CardanoDatabaseSnapshot,
	downloadDir string,
) (_ []CardanoDatabaseDigestEntry, retErr error) {
	downloadCfg := DownloadConfig{
		MaxBytes:    cfg.objectMaxBytes(digestListMaxBytes),
		spaceBudget: cfg.downloadSpaceBudget,
		URL:         uri,
		DestDir:     downloadDir,
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
		HTTPClient:          cfg.httpClient,
	}
	archivePath, downloadRoot, err := downloadSnapshot(ctx, downloadCfg)
	if err != nil {
		if downloadRoot != nil {
			_ = downloadRoot.Close()
		}
		return nil, err
	}
	archive, err := retainBootstrapFile(archivePath, downloadRoot)
	if err != nil {
		return nil, fmt.Errorf("retaining digests archive: %w", err)
	}
	archiveFile, err := archive.root.Open(filepath.Base(archivePath))
	if err != nil {
		_ = archive.close()
		return nil, fmt.Errorf("opening digests archive: %w", err)
	}
	openedArchive, err := archiveFile.Stat()
	if err != nil || !os.SameFile(archive.info, openedArchive) {
		_ = archiveFile.Close()
		_ = archive.close()
		if err != nil {
			return nil, fmt.Errorf("inspecting digests archive: %w", err)
		}
		return nil, fmt.Errorf(
			"%w: %q", errBootstrapCleanupIdentity, archivePath,
		)
	}
	var extracted *vettedDir
	defer func() {
		closeErr := archiveFile.Close()
		if ctx.Err() != nil {
			extracted.Close()
			closeErr = errors.Join(closeErr, archive.close())
		} else {
			closeErr = errors.Join(
				closeErr,
				cleanupVettedDir(extracted),
				archive.cleanup(),
			)
		}
		retErr = errors.Join(retErr, closeErr)
	}()
	destDir := filepath.Join(
		downloadDir,
		filepath.Base("digests-"+truncateDigest(artifact.Hash)),
	)
	var published os.FileInfo
	if _, err := extractArchiveFile(
		ctx,
		archiveFile,
		archivePath,
		destDir,
		cfg.Logger.With(
			"phase", "digest_extraction",
			"artifact", "immutable_digest_list",
		),
		WithReplaceDestination(),
		withDecoderBudget(cfg.decoderBudget),
		withArchiveLimits(withBootstrapExtractionBudget(
			digestListLimits(), cfg.extractionBudget,
		)),
		withPublishedIdentity(&published),
	); err != nil {
		return nil, fmt.Errorf("extracting digests archive: %w", err)
	}
	extractedRoot, err := openVerifiedDir(destDir)
	if err != nil {
		return nil, fmt.Errorf("retaining digests directory: %w", err)
	}
	openedExtracted, err := extractedRoot.Stat(".")
	if err != nil || published == nil ||
		!os.SameFile(published, openedExtracted) {
		_ = extractedRoot.Close()
		if err != nil {
			return nil, fmt.Errorf("inspecting digests directory: %w", err)
		}
		return nil, fmt.Errorf(
			"%w: %q", errBootstrapCleanupIdentity, destDir,
		)
	}
	extracted = &vettedDir{root: extractedRoot, path: destDir}
	dirEntries, err := fs.ReadDir(extracted.root.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("reading digests directory: %w", err)
	}
	jsonPath := ""
	for _, entry := range dirEntries {
		if entry.IsDir() ||
			!strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		jsonPath = entry.Name()
		break
	}
	if jsonPath == "" {
		return nil, errors.New(
			"no digest JSON file found in digests archive",
		)
	}
	jsonFile, err := extracted.root.Open(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("reading digest JSON: %w", err)
	}
	data, readErr := io.ReadAll(jsonFile)
	closeErr := jsonFile.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf(
			"reading digest JSON: %w", errors.Join(readErr, closeErr),
		)
	}
	var entries []CardanoDatabaseDigestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing digest JSON: %w", err)
	}
	return entries, nil
}

// downloadImmutables downloads, extracts, and digest-verifies the
// immutable archives StartImmutable..=beacon.ImmutableFileNumber into
// extractDir. It is a consumer of the same verified ChunkFeed that in-node
// stream catch-up reads, opened with an unbounded window over a stable
// directory that outlives the feed: the caller loads the extracted tree once
// the download returns, and a rerun resumes by reusing every trio already in
// place that matches the certified digest list. Archives are deleted after
// extraction to bound disk usage.
//
// When cfg.OnChunkContiguous is set it runs for each file in ascending order
// as the feed delivers it, so the caller can copy the contiguous prefix while
// later files are still downloading.
func downloadImmutables(
	ctx context.Context,
	cfg BootstrapConfig,
	client *Client,
	artifact *CardanoDatabaseSnapshot,
	digests map[string]string,
	downloadDir string,
	extractDir string,
) error {
	// archiveDir is not created here. A create-then-close-then-hand-off-
	// the-bare-path step would itself be a TOCTOU window: a symlink
	// swapped in for it between this function returning and the first
	// download would be followed by whatever opens it next. Instead,
	// DownloadSnapshot (called once per archive) creates and verifies its
	// own DestDir through a handle on its parent every time it runs,
	// closing that window at the point it actually matters.
	archiveDir := filepath.Join(
		downloadDir,
		filepath.Base("immutable-archives-"+truncateDigest(artifact.Hash)),
	)

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

	feed, err := newVerifiedChunkFeed(ctx, ChunkFeedConfig{
		Bootstrap:      cfg,
		StartImmutable: startArchive,
		keep: &feedKeep{
			extractDir: extractDir,
			archiveDir: archiveDir,
			onFetched:  onArchiveDone,
		},
	}, cfg, client, artifact, digests)
	if err != nil {
		return err
	}
	// The kept tree is the result, so Close has nothing to remove; it only
	// stops the workers and releases the directory handle.
	defer func() { _ = feed.Close() }()
	for {
		chunk, err := feed.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if cfg.OnChunkContiguous == nil {
			continue
		}
		// A copy failure is the root cause; returning it closes the feed,
		// which cancels the downloads still running.
		if err := cfg.OnChunkContiguous(ContiguousChunk{
			Dir:     feed.immDir,
			Root:    feed.immRoot,
			Digests: digests,
			Start:   startArchive,
			Num:     chunk.Num,
		}); err != nil {
			return err
		}
	}
}

// reuseCachedImmutableTrio reports whether the trio for num is already in
// root and matches the certified digests, so a rerun need not fetch it again.
// A trio that does not match is removed so it cannot be reported against bytes
// no location served; any other failure to check it is returned as archiveErr,
// since another location cannot fix it.
func reuseCachedImmutableTrio(
	logger *slog.Logger,
	root *os.Root,
	num uint64,
	digests map[string]string,
	archiveErr *ImmutableArchiveError,
) (int64, bool, error) {
	cachedBytes, cacheErr := checkImmutableTrio(root, num, digests)
	switch {
	case cacheErr == nil:
		return cachedBytes, true, nil
	case errors.Is(cacheErr, fs.ErrNotExist):
		// Nothing extracted for this number yet: the ordinary first pass,
		// not a rejected cache. No attempt is recorded.
		return 0, false, nil
	}
	archiveErr.Attempts = append(
		archiveErr.Attempts,
		ImmutableArchiveAttempt{
			Source: immutableSourceCache,
			Err:    cacheErr,
		},
	)
	if !errors.As(cacheErr, new(*DigestMismatchError)) {
		// The certified digest list carries no entry for this file, or the
		// cached files cannot be read. Neither is something another location
		// can fix, and neither says the cached bytes are wrong, so the trio is
		// left in place.
		return 0, false, archiveErr
	}
	// A trio left by an earlier run that does not match the digest list this
	// run verified. It is removed before the first download rather than left
	// for extraction to overwrite, so a later failure cannot be reported
	// against bytes no location served, and a partially overwritten trio
	// cannot survive into the next attempt.
	removeImmutableTrio(root, num)
	logger.Warn(
		"cached immutable trio does not match the certified "+
			"digest list, removing and refetching",
		"component", "mithril",
		"immutable_file_number", num,
		"source", immutableSourceCache,
		"error", cacheErr,
	)
	return 0, false, nil
}

// fetchVerifiedImmutableTrio tries each location in turn until one yields a
// trio whose files match the certified digest list, recording every rejected
// attempt on archiveErr. It returns archiveErr when no location does, and the
// trio is removed after each rejected attempt so a failed or partially
// overwritten download never survives into the next one.
func fetchVerifiedImmutableTrio(
	ctx context.Context,
	cfg BootstrapConfig,
	quietLogger *slog.Logger,
	locations []*CardanoDatabaseLocation,
	num uint64,
	archiveDir string,
	extractDir string,
	immutableRoot *os.Root,
	digests map[string]string,
	archiveErr *ImmutableArchiveError,
) (int64, error) {
	var bytes int64
	fetched := false
	for i, location := range locations {
		source := redactLocationURI(location.ImmutableArchiveURI(num))
		attempt := ImmutableArchiveAttempt{
			Source:   source,
			Location: i + 1,
		}
		if err := fetchImmutableArchive(
			ctx, cfg, quietLogger, location, num,
			archiveDir, extractDir,
		); err != nil {
			attempt.Err = err
			archiveErr.Attempts = append(archiveErr.Attempts, attempt)
			// fetchImmutableArchive has already removed its own
			// archive file, through the root it downloaded
			// through, on every exit -- no bare-path cleanup of
			// archiveDir needed here.
			removeImmutableTrio(immutableRoot, num)
			cfg.Logger.Warn(
				"immutable archive location failed, trying next",
				"component", "mithril",
				"immutable_file_number", num,
				"location", i+1,
				"total", len(locations),
				"location_uri", source,
				"error", err,
			)
			continue
		}
		var checkErr error
		bytes, checkErr = checkImmutableTrio(
			immutableRoot, num, digests,
		)
		if checkErr != nil {
			attempt.Err = checkErr
			archiveErr.Attempts = append(archiveErr.Attempts, attempt)
			removeImmutableTrio(immutableRoot, num)
			logAttrs := []any{
				"component", "mithril",
				"immutable_file_number", num,
				"location", i + 1,
				"total", len(locations),
				"location_uri", source,
				"error", checkErr,
			}
			if mismatch := attempt.Mismatch(); mismatch != nil {
				logAttrs = append(
					logAttrs,
					"immutable_file", mismatch.FileName,
					"expected_digest", mismatch.Expected,
					"observed_digest", mismatch.Observed,
				)
			}
			cfg.Logger.Warn(
				"immutable archive verification failed, trying next",
				logAttrs...,
			)
			continue
		}
		fetched = true
		break
	}
	if !fetched {
		if len(archiveErr.Attempts) == 0 {
			archiveErr.Attempts = append(
				archiveErr.Attempts,
				ImmutableArchiveAttempt{
					Source: "none",
					Err: errors.New(
						"no usable immutable archive locations",
					),
				},
			)
		}
		return 0, archiveErr
	}
	return bytes, nil
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
	_, root, dlErr := downloadSnapshot(
		ctx, DownloadConfig{
			MaxBytes:            cfg.objectMaxBytes(immutableArchiveMaxBytes),
			spaceBudget:         cfg.downloadSpaceBudget,
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
		// Removed here, through the same root every other operation in
		// this function uses, on every exit -- success, download failure
		// leaving a partial file, or extraction failure leaving a
		// complete one. The caller used to repeat this cleanup itself by
		// joining archiveDir (a bare path) with the filename after this
		// function returned; a directory swapped in for archiveDir's name
		// in that gap made that cleanup delete a same-named external file
		// through the replacement instead of this one. Doing it here,
		// still anchored to root, closes that gap on the failure path the
		// same way the success path was already closed.
		defer func() {
			if removeErr := root.Remove(archiveFilename); removeErr != nil &&
				!os.IsNotExist(removeErr) {
				cfg.Logger.Warn(
					"failed to remove immutable archive after fetch",
					"component", "mithril",
					"path", archivePath,
					"error", removeErr,
				)
			}
			root.Close()
		}()
	}
	if dlErr != nil {
		return dlErr
	}

	file, err := root.Open(archiveFilename)
	if err != nil {
		return fmt.Errorf("opening downloaded archive: %w", err)
	}
	defer file.Close()

	// Merge: every immutable archive extracts into one shared directory,
	// concurrently, so this destination accumulates across calls and must
	// not be staged-and-swapped.
	extractOpts := []ExtractOption{
		WithMergeIntoDestination(),
		withDecoderBudget(cfg.decoderBudget),
	}
	if cfg.immutableDigests != nil {
		extractOpts = append(
			extractOpts,
			withArchiveLimits(
				withBootstrapExtractionBudget(
					immutableArchiveLimits(num, cfg.immutableDigests),
					cfg.extractionBudget,
				),
			),
		)
	}
	if _, err := extractArchiveFile(
		ctx, file, archivePath, extractDir, extractLogger,
		extractOpts...,
	); err != nil {
		return fmt.Errorf("extracting: %w", err)
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
			return 0, &DigestMismatchError{
				FileName: name,
				Expected: expected,
				Observed: sum,
			}
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
	_ = removeImmutableTrioErr(root, num)
}

// removeImmutableTrioErr is removeImmutableTrio for a caller that must know
// the files are gone. Files that are already absent are not an error.
func removeImmutableTrioErr(root *os.Root, num uint64) error {
	var errs []error
	for _, ext := range immutableFileExtensions {
		if err := root.Remove(fmt.Sprintf("%05d.%s", num, ext)); err != nil &&
			!errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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

// ancillaryDownloadError marks a failure to fetch the ancillary archive, as
// opposed to a failure to use a complete one.
type ancillaryDownloadError struct{ err error }

func (e *ancillaryDownloadError) Error() string {
	return "downloading ancillary archive: " + e.err.Error()
}

func (e *ancillaryDownloadError) Unwrap() error { return e.err }

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
) (*vettedDir, map[string]string, *bootstrapFile, error) {
	if len(artifact.Ancillary.Locations) == 0 {
		return nil, nil, nil, errors.New(
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

	var archive *bootstrapFile
	var archiveFile *os.File
	var err error
	downloadFailed := false
	for i, loc := range artifact.Ancillary.Locations {
		if archive != nil {
			_ = archive.close()
			archive = nil
		}
		if loc.URI == "" {
			continue
		}
		downloadCfg := DownloadConfig{
			MaxBytes:    cfg.objectMaxBytes(ancillaryMaxBytes),
			spaceBudget: cfg.downloadSpaceBudget,
			URL:         loc.URI,
			DestDir:     downloadDir,
			Filename:    ancillaryFilename,
			Logger:      cfg.Logger,
			OnProgress: withProgressContext(
				cfg.OnProgress,
				"ancillary_ledger_state",
				artifact.Hash,
			),
			IdleTimeout:         cfg.DownloadIdleTimeout,
			MaxIdleRetries:      cfg.DownloadMaxIdleRetries,
			MaxTransientRetries: cfg.DownloadMaxTransientRetries,
			AllowInsecureHTTP:   cfg.AllowInsecureHTTP,
			HTTPClient:          cfg.httpClient,
		}
		ancillaryPath, downloadRoot, downloadErr := downloadSnapshot(ctx, downloadCfg)
		downloadFailed = downloadErr != nil
		if downloadRoot != nil {
			candidatePath := ancillaryPath
			if candidatePath == "" {
				candidatePath = downloadDestinationPath(downloadCfg)
			}
			candidate, retainErr := retainBootstrapFile(
				candidatePath, downloadRoot,
			)
			if retainErr == nil {
				archive = candidate
			} else if !errors.Is(retainErr, os.ErrNotExist) && downloadErr == nil {
				downloadErr = retainErr
			}
		}
		err = downloadErr
		if err == nil && archive != nil {
			archiveFile, err = archive.root.Open(ancillaryFilename)
			if err == nil {
				opened, statErr := archiveFile.Stat()
				if statErr != nil || !os.SameFile(archive.info, opened) {
					if statErr != nil {
						err = statErr
					} else {
						err = fmt.Errorf(
							"%w: %q", errBootstrapCleanupIdentity,
							archive.path,
						)
					}
					_ = archiveFile.Close()
					archiveFile = nil
				}
			}
		}
		if err == nil && archiveFile != nil {
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
	if archive == nil {
		if err == nil {
			err = errors.New("no usable ancillary locations")
			downloadFailed = true
		}
		if downloadFailed {
			return nil, nil, nil, &ancillaryDownloadError{err: err}
		}
		return nil, nil, nil, fmt.Errorf(
			"retaining downloaded ancillary archive: %w", err,
		)
	}
	if err != nil {
		// The destination path, not the (empty) return of a failed download:
		// DownloadSnapshot resumes, so a failed attempt leaves a partial file
		// there for Cleanup to remove.
		//
		// Asked of the downloader rather than assembled here, so the two
		// cannot name different files.
		if downloadFailed {
			return nil, nil, archive, &ancillaryDownloadError{err: err}
		}
		return nil, nil, archive, fmt.Errorf(
			"retaining downloaded ancillary archive: %w", err,
		)
	}
	ancillaryPath := archive.path

	ancillaryDir := filepath.Join(
		downloadDir,
		filepath.Base("ancillary-"+artifact.Hash),
	)
	defer archiveFile.Close()
	limits := withBootstrapExtractionBudget(
		ancillaryLimits(), cfg.extractionBudget,
	)
	if cfg.VerifyCertificateChain {
		manifest, manifestData, preflightErr := readVerifiedAncillaryManifest(
			ctx, archiveFile, cfg.AncillaryVerificationKey,
			cfg.decoderBudget,
		)
		if preflightErr != nil {
			return nil, nil, archive, fmt.Errorf(
				"verifying ancillary manifest before extraction: %w",
				preflightErr,
			)
		}
		limits = verifiedAncillaryLimits(
			manifest, manifestData, cfg.extractionBudget,
		)
	}
	if _, err := archiveFile.Seek(0, io.SeekStart); err != nil {
		return nil, nil, archive, fmt.Errorf(
			"rewinding ancillary archive: %w", err,
		)
	}
	// Replace: the resume path above may have removed an unverified
	// extraction, but an interrupted run can still leave one behind.
	var published os.FileInfo
	if _, extractErr := extractArchiveFile(
		ctx,
		archiveFile,
		ancillaryPath,
		ancillaryDir,
		cfg.Logger.With(
			"phase", "ancillary_extraction",
			"artifact", "ancillary_ledger_state",
		),
		WithReplaceDestination(),
		withDecoderBudget(cfg.decoderBudget),
		withArchiveLimits(limits),
		withPublishedIdentity(&published),
	); extractErr != nil {
		return nil, nil, archive, fmt.Errorf(
			"extracting ancillary archive: %w",
			extractErr,
		)
	}

	// Vetted and then verified through one handle, which is the same handle
	// returned. Anything else leaves the manifest check describing a tree the
	// caller does not go on to read.
	extracted := ledgerDir(ancillaryDir)
	if extracted != nil {
		opened, statErr := extracted.root.Stat(".")
		if statErr != nil || published == nil ||
			!os.SameFile(published, opened) {
			extracted.Close()
			if statErr != nil {
				return nil, nil, archive, fmt.Errorf(
					"inspecting ancillary directory: %w", statErr,
				)
			}
			return nil, nil, archive, fmt.Errorf(
				"%w: %q", errBootstrapCleanupIdentity, ancillaryDir,
			)
		}
	}
	if extracted == nil {
		// Unverified bootstraps reach here too — verifyAncillaryExtraction is
		// a no-op for them — so an ancillary archive carrying no ledger state
		// has to be caught on its own.
		unusable, cleanupErr := openVerifiedDir(ancillaryDir)
		if cleanupErr == nil {
			opened, statErr := unusable.Stat(".")
			if statErr != nil || published == nil ||
				!os.SameFile(published, opened) {
				_ = unusable.Close()
				if statErr != nil {
					cleanupErr = statErr
				} else {
					cleanupErr = fmt.Errorf(
						"%w: %q", errBootstrapCleanupIdentity,
						ancillaryDir,
					)
				}
			} else {
				cleanupErr = cleanupVettedDir(&vettedDir{
					root: unusable,
					path: ancillaryDir,
				})
			}
		}
		// The archive path goes back even so, since it was downloaded and
		// still wants cleaning up.
		return nil, nil, archive, errors.Join(
			fmt.Errorf(
				"extracted ancillary data at %s holds no ledger state",
				ancillaryDir,
			),
			cleanupErr,
		)
	}
	digests, verifyErr := verifyAncillaryExtraction(cfg, extracted)
	if verifyErr != nil {
		// Remove the unverified extraction so it cannot be
		// picked up by the resume path on a later run.
		cleanupErr := cleanupVettedDir(extracted)
		// The extraction goes, the archive stays — and stays reported.
		// Cleanup removes the two separately, so an unverified manifest that
		// cleared this path would leave the download behind in a directory the
		// operator supplied and nothing else sweeps.
		return nil, nil, archive, errors.Join(verifyErr, cleanupErr)
	}

	cfg.Logger.Info(
		"ancillary data extracted",
		"component", "mithril",
		"path", extracted.Path(),
	)

	return extracted, digests, archive, nil
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
	manifest, err := parseAndVerifyAncillaryManifest(
		data, ancillaryVerificationKey,
	)
	if err != nil {
		return nil, err
	}

	for _, relPath := range slices.Sorted(maps.Keys(manifest.Data)) {
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
// maxAncillaryManifestBytes bounds a manifest read out of an extracted
// snapshot before it is JSON decoded. The archive is remote input, so its
// manifest is sized by whoever produced it; the bound matches the one
// applied to the snapshot manifest itself.
const maxAncillaryManifestBytes = 1 << 20

func readFileIn(root *os.Root, rel string) ([]byte, error) {
	f, err := root.Open(filepath.FromSlash(rel))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxAncillaryManifestBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxAncillaryManifestBytes {
		return nil, fmt.Errorf(
			"%s exceeds %d bytes",
			rel,
			maxAncillaryManifestBytes,
		)
	}
	return data, nil
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
