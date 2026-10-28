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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Mithril artifact backends.
const (
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
	return []string{BackendV2}
}

// BootstrapConfig holds configuration for the Mithril bootstrap
// process.
type BootstrapConfig struct {
	// Network is the Cardano network name (e.g., "mainnet",
	// "preprod", "preview").
	Network string
	// Backend selects the Mithril artifact backend. BackendV2 restores
	// per-immutable archives verified against the certified merkle
	// root and is the only backend; empty selects it.
	Backend string
	// AggregatorURL overrides the default aggregator URL for the
	// network. If empty, the default URL for the network is used.
	AggregatorURL string
	// AllowInsecureHTTP permits AggregatorURL and snapshot artifact locations
	// to use plain HTTP or resolve to local/private addresses. Defaults to
	// false; this is an explicit escape hatch for local development and tests
	// and should not be set in production.
	AllowInsecureHTTP bool
	// DownloadDir is the directory where the snapshot archive will
	// be downloaded. If empty, a temporary directory is created.
	DownloadDir string
	// CleanupAfterLoad controls whether temporary files are removed
	// after loading completes.
	CleanupAfterLoad bool
	// VerifyCertificateChain enables STM certificate verification against the
	// aggregator. When true, GenesisVerificationKey is required and the
	// bootstrap process verifies the chain back to that pinned genesis key.
	VerifyCertificateChain bool
	// GenesisVerificationKey is the pinned Mithril trust anchor used by STM
	// certificate verification. It is loaded from Cardano network config and
	// validated for parseability before any aggregator request.
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
	// DownloadMaxBytes bounds each compressed object, including resumed
	// bytes. Zero uses DefaultMaxDownloadBytes. It is not a cumulative
	// budget across archives, mirrors, or retries.
	DownloadMaxBytes int64
	// DownloadMaxTransientRetries is the maximum number of retry attempts
	// for transient network errors (TLS handshake failures, connection
	// resets, HTTP 429, HTTP 5xx) per download. Zero uses the downloader
	// default. Negative disables transient retries.
	DownloadMaxTransientRetries int
	// httpClient is the shared keep-alive client the v2 bootstrap reuses across
	// artifact fetches, including the immutable downloads of its kept chunk
	// feed. Callers do not populate it.
	httpClient *http.Client
	// downloadBudget bounds all artifact transfer requests and bytes in one v2
	// bootstrap. It is initialized after the certified artifact is selected.
	downloadBudget *artifactDownloadBudget
	// extractionBudget bounds expanded bytes and entries across all archives in
	// one v2 bootstrap and preserves free space on their destination filesystem.
	extractionBudget *bootstrapExtractionBudget
	// downloadSpaceBudget preserves the same filesystem reserve while compressed
	// artifacts are being written, including concurrent immutable downloads.
	downloadSpaceBudget *downloadSpaceBudget
	// decoderBudget bounds the zstd history memory of the archive decoders
	// running at once in one v2 bootstrap. Nil reserves nothing.
	decoderBudget *decoderMemoryBudget
	// immutableDigests is the verified digest list, set by the chunk feed
	// so each archive's members are checked as they are extracted. When nil,
	// an immutable archive is extracted without a member allowlist.
	immutableDigests map[string]string
	// CertificateCache, when set, lets certificate verification stop at
	// certificates an earlier verification in this process accepted, and
	// skip the walk for a leaf it already accepted. Nil walks every chain to
	// genesis.
	CertificateCache *CertificateCache
	// OnArtifactSelected, when set, is invoked once this run's aggregator
	// artifact has been resolved, identity-checked and (when enabled)
	// certificate-verified, and before anything is downloaded. A non-nil
	// return aborts the bootstrap.
	//
	// It runs before the first download rather than after the bootstrap so the
	// caller can record the artifact identity durably while the run still owns
	// nothing: an interruption at any later point then has an artifact to
	// resume against, including the artifact-keyed download cache.
	OnArtifactSelected func(SelectedArtifact) error
	// PinnedDigest, when set, selects that exact artifact from the aggregator
	// instead of the latest one: it is resolved as a Cardano database
	// artifact hash. A resumed
	// Mithril sync sets it from the artifact pin the interrupted run recorded,
	// so the resume imports the artifact its partial database rows and phase
	// checkpoints belong to. Empty selects the latest artifact, the normal
	// first-run behaviour.
	PinnedDigest string
	// StartImmutable is the lowest immutable file number to download and
	// extract. Files below it are assumed already present (a Mithril v2
	// catch-up sets this to the immutable-import marker so it only fetches the
	// archives missing from the existing blob store). Zero downloads the full
	// 0..N range, the normal bootstrap behaviour.
	StartImmutable uint64
	// OnChunkContiguous, when set, enables download<->processing
	// pipelining: chunks are fetched in parallel (out of order) by the
	// shared chunk feed, and this callback is invoked from the feed's
	// in-order consumer for each immutable file number in strict
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
	// Metrics, when not nil, records the chunk downloads of this bootstrap
	// and of any chunk feed or stream catch-up configured with it.
	Metrics *PipelineMetrics
}

// SelectedArtifact is the identity of the aggregator artifact a bootstrap run
// resolved, reported to BootstrapConfig.OnArtifactSelected before any download
// begins. Digest is the Cardano database artifact hash, matching what
// BootstrapConfig.PinnedDigest accepts.
type SelectedArtifact struct {
	Backend         string
	Network         string
	Digest          string
	Beacon          Beacon
	CertificateHash string
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
	// VerificationModeStructural verifies certificate chain linkage.
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
	// The bootstrap always sets it. A result without it did not come from a
	// vetted lookup, and loading refuses rather than falling back to the
	// pathname — a silent fallback would reinstate exactly the open this
	// replaces.
	ImmutableRoot *os.Root
	// ImmutableDigests is the certified SHA-256 of every file in ImmutableDir,
	// keyed by the name beneath it ("00000.chunk"). Set by the v2 backend,
	// which downloads each immutable trio against a digest list covered by the
	// certificate's merkle root.
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
	// tables/tvar}. Set on every successful bootstrap.
	AncillaryDir string
	// AncillaryRoot is an open handle on AncillaryDir, on the same terms as
	// ImmutableRoot: the ledger-state import discovers and reads through it, so
	// the tree the signed manifest was checked against is the tree that gets
	// loaded. Set on every successful bootstrap.
	AncillaryRoot *os.Root
	// AncillaryVerified reports that the ancillary tree's contents were checked
	// against the signed ancillary manifest (a verified v2 bootstrap).
	//
	// The ledger-state import accepts a verified tree's state even when it is
	// newer than the certified tip, and refuses a verified tree that yields no
	// state at all.
	AncillaryVerified bool
	// CertificateVerified reports that the bootstrap was asked to verify the
	// certificate chain, so a missing ancillary tree is a verification failure
	// rather than an unverified run that simply produced none.
	CertificateVerified bool
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
	// ExtractDir is derived inside the download directory like everything else
	// here, so it is held through a handle rather than resolved by name.
	ExtractRoot *os.Root
	// AncillaryArchivePath is the path to the downloaded ancillary
	// archive file. Empty if no ancillary data was downloaded.
	AncillaryArchivePath string
	// ancillaryArchiveRoot retains the parent directory used to create the
	// ancillary archive, and ancillaryArchiveInfo records the file created
	// there. Cleanup uses both rather than resolving the saved path again.
	ancillaryArchiveRoot  *os.Root
	ancillaryArchiveInfo  os.FileInfo
	ancillaryArchiveGuard *bootstrapFileGuard
	// TempDir is the auto-created temporary directory that holds
	// all downloaded and extracted files. Set only when
	// BootstrapConfig.DownloadDir was empty. Cleanup() removes it
	// after removing its children.
	TempDir string
	// TempRoot is the retained handle for TempDir. It is set only when TempDir
	// is set and lets Cleanup remove that exact temporary tree.
	TempRoot  *os.Root
	tempOwned *bootstrapDir
}

// Bootstrap validates cfg, resolves the aggregator URL, and delegates to the
// v2 backend, which restores the ImmutableDB trio files and the ancillary
// ledger state from the aggregator's incremental database artifacts.
//
// The caller is responsible for invoking the immutable DB load using
// the returned ImmutableDir path. If CleanupAfterLoad is true, the
// caller should call Cleanup() on the result after loading.
func Bootstrap(
	ctx context.Context,
	cfg BootstrapConfig,
) (*BootstrapResult, error) {
	if err := (DownloadConfig{MaxBytes: cfg.DownloadMaxBytes}).Validate(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cfg.Backend = normalizeBackend(cfg.Backend)
	if cfg.VerifyCertificateChain {
		if strings.TrimSpace(cfg.GenesisVerificationKey) == "" {
			return nil, errors.New(
				"verified Mithril bootstrap requires a genesis verification key",
			)
		}
		if _, err := ParseVerificationKey(cfg.GenesisVerificationKey); err != nil {
			return nil, fmt.Errorf(
				"parsing Mithril genesis verification key: %w",
				err,
			)
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
		"aggregator", redactLocationURI(aggregatorURL),
		"backend", cfg.Backend,
	)

	if cfg.Backend != BackendV2 {
		return nil, fmt.Errorf(
			"unsupported Mithril backend %q (expected %q)",
			cfg.Backend,
			BackendV2,
		)
	}
	return bootstrapV2(ctx, cfg, aggregatorURL)
}

// Cleanup removes the temporary files created during bootstrap.
// It removes the archive, extract directory, and ancillary
// directory individually rather than the entire parent directory,
// to avoid deleting user-specified download directories.
func (r *BootstrapResult) Cleanup(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	if r == nil {
		return
	}
	// ImmutableRoot is beneath ExtractRoot. Close it before removing entries
	// through ExtractRoot so Windows does not retain a child handle.
	if r.ImmutableRoot != nil {
		_ = r.ImmutableRoot.Close()
		r.ImmutableRoot = nil
	}
	dirs := []struct {
		path string
		root **os.Root
	}{
		{path: r.ExtractDir, root: &r.ExtractRoot},
		{path: r.AncillaryDir, root: &r.AncillaryRoot},
	}
	keepInTemp := make(map[string]struct{}, len(dirs)+1)
	for _, dir := range dirs {
		if dir.path == "" {
			continue
		}
		keepInTemp[filepath.Base(filepath.Clean(dir.path))] = struct{}{}
		if err := cleanupBootstrapDir(dir.root, dir.path, nil); err != nil {
			logger.Warn(
				"failed to clean up Mithril temp file",
				"component", "mithril",
				"path", dir.path,
				"error", err,
			)
		} else {
			logger.Info(
				"cleaned up Mithril temp file",
				"component", "mithril",
				"path", dir.path,
			)
		}
	}
	if r.AncillaryArchivePath != "" {
		keepInTemp[filepath.Base(filepath.Clean(r.AncillaryArchivePath))] = struct{}{}
		if err := cleanupBootstrapFile(
			&r.ancillaryArchiveRoot,
			r.AncillaryArchivePath,
			r.ancillaryArchiveInfo,
			&r.ancillaryArchiveGuard,
		); err != nil {
			logger.Warn(
				"failed to clean up Mithril temp file",
				"component", "mithril",
				"path", r.AncillaryArchivePath,
				"error", err,
			)
		}
	}
	// Remove auto-created temp directory and any remaining contents
	// (e.g. a partially-extracted ancillary archive).
	if r.TempDir != "" {
		var err error
		if r.tempOwned != nil {
			r.tempOwned.root = r.TempRoot
			err = r.tempOwned.cleanupKeeping(keepInTemp)
			r.TempRoot = r.tempOwned.root
			r.tempOwned = nil
		} else {
			err = cleanupBootstrapDir(
				&r.TempRoot,
				r.TempDir,
				keepInTemp,
			)
		}
		if err != nil {
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
	r.CloseHandles()
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
	if r.ancillaryArchiveGuard != nil {
		_ = r.ancillaryArchiveGuard.Close()
		r.ancillaryArchiveGuard = nil
	}
	if r.ancillaryArchiveRoot != nil {
		_ = r.ancillaryArchiveRoot.Close()
		r.ancillaryArchiveRoot = nil
	}
	if r.tempOwned != nil {
		r.tempOwned.root = r.TempRoot
		if r.tempOwned.root != nil {
			_ = r.tempOwned.root.Close()
			r.tempOwned.root = nil
		}
		_ = r.tempOwned.closeParentHandles()
		r.tempOwned = nil
		r.TempRoot = nil
	} else if r.TempRoot != nil {
		_ = r.TempRoot.Close()
		r.TempRoot = nil
	}
}

// VerifyCertificateChain walks the Mithril certificate chain from
// the given hash back to the genesis certificate. This verifies
// the chain is unbroken. It does not verify STM cryptographic signatures
// (Phase 2).
func VerifyCertificateChain(
	ctx context.Context,
	client *Client,
	certificateHash string,
) error {
	_, err := VerifyCertificateChainWithMode(
		ctx,
		client,
		certificateHash,
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
	mode VerificationMode,
) (*CertificateChainVerificationResult, error) {
	return verifyCertificateChain(
		ctx,
		client,
		certificateHash,
		mode,
		newCertificateChainBudget(),
	)
}

func verifyCertificateChain(
	ctx context.Context,
	client *Client,
	certificateHash string,
	mode VerificationMode,
	budget *certificateChainBudget,
) (*CertificateChainVerificationResult, error) {
	return verifyCertificateChainAnchored(
		ctx, client, certificateHash, mode, budget, nil,
	)
}

// verifyCertificateChainAnchored is verifyCertificateChain stopping at the
// first certificate below the leaf that anchors already verified. The link
// from the last fetched certificate to it is checked like any other, and the
// result carries the anchor's verified chain down to genesis, so callers see
// the same chain a full walk returns.
func verifyCertificateChainAnchored(
	ctx context.Context,
	client *Client,
	certificateHash string,
	mode VerificationMode,
	budget *certificateChainBudget,
	anchors *certificateAnchors,
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

	currentHash := certificateHash
	seen := make(map[string]bool)
	isLeaf := true
	var childCert *Certificate
	result := &CertificateChainVerificationResult{}

	for {
		var anchored []*Certificate
		if !isLeaf {
			anchored, _ = anchors.chainFrom(currentHash)
		}
		if len(anchored) > 0 {
			cert := anchored[0]
			if err := checkCertificateLink(childCert, cert); err != nil {
				return nil, err
			}
			result.Certificates = append(result.Certificates, anchored...)
			result.GenesisCertificate = anchored[len(anchored)-1]
			return result, nil
		}
		if err := budget.chargeCertificate(); err != nil {
			return nil, err
		}
		if seen[currentHash] {
			return nil, fmt.Errorf(
				"certificate chain cycle detected at %s",
				currentHash,
			)
		}
		seen[currentHash] = true

		cert, size, err := client.getCertificateWithLimit(
			ctx,
			currentHash,
			budget.certificateByteLimit(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"fetching certificate %s: %w",
				currentHash,
				err,
			)
		}
		budget.chargeBytes(size)
		if len(cert.Metadata.Signers) > stmMaxSigners {
			return nil, fmt.Errorf(
				"%w: certificate %s lists %d signers, limit %d",
				errCertificateChainBudget,
				currentHash,
				len(cert.Metadata.Signers),
				stmMaxSigners,
			)
		}
		if err := budget.chargeSigners(len(cert.Metadata.Signers)); err != nil {
			return nil, fmt.Errorf(
				"certificate %s: %w",
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
			if err := verifySTMCertificate(cert, budget); err != nil {
				return nil, fmt.Errorf(
					"STM verification failed for certificate %s: %w",
					currentHash,
					err,
				)
			}
		}
		if err := checkCertificateLink(childCert, cert); err != nil {
			return nil, err
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
}

// checkCertificateLink checks that parent may precede child in a chain: the
// hashes link, the epochs are equal or adjacent, and the child's AVK and
// protocol parameters are the parent's within an epoch, or the ones the parent
// announced as next across one. A nil child (parent is the leaf) passes.
func checkCertificateLink(child, parent *Certificate) error {
	if child == nil {
		return nil
	}
	if child.PreviousHash != parent.Hash {
		return fmt.Errorf(
			"certificate chain previous hash mismatch: child=%s previous=%s parent=%s",
			child.Hash,
			child.PreviousHash,
			parent.Hash,
		)
	}
	if child.Epoch != parent.Epoch && child.Epoch != parent.Epoch+1 {
		return fmt.Errorf(
			"certificate chain missing epoch between child=%d and parent=%d",
			child.Epoch,
			parent.Epoch,
		)
	}
	if child.Epoch == parent.Epoch {
		if child.AggregateVerificationKey != parent.AggregateVerificationKey {
			return errors.New(
				"certificate chain aggregate verification key mismatch within epoch",
			)
		}
		if child.Metadata.Parameters != parent.Metadata.Parameters {
			return errors.New(
				"certificate chain protocol parameters mismatch within epoch",
			)
		}
		return nil
	}
	nextAVK, ok := parent.ProtocolMessage.MessageParts["next_aggregate_verification_key"]
	if !ok || nextAVK != child.AggregateVerificationKey {
		return errors.New(
			"certificate chain aggregate verification key mismatch across epoch",
		)
	}
	nextProtocolParameters, ok := parent.ProtocolMessage.MessageParts["next_protocol_parameters"]
	if !ok || nextProtocolParameters != child.Metadata.Parameters.ComputeHash() {
		return errors.New(
			"certificate chain protocol parameters mismatch across epoch",
		)
	}
	return nil
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

// snapshotDigestLength is the expected length of a Mithril snapshot digest:
// a SHA-256 (see CardanoDatabaseSnapshot.ComputeHash)
// hash, hex-encoded (32 bytes -> 64 hex characters).
const snapshotDigestLength = 64

// digestPattern matches a digest of exactly snapshotDigestLength hex
// characters.
var digestPattern = regexp.MustCompile(
	fmt.Sprintf(`^[0-9a-fA-F]{%d}$`, snapshotDigestLength),
)

// validateSnapshotIdentity rejects Mithril aggregator-supplied network and
// digest values before they are used to construct any filesystem path.
// Both fields arrive unauthenticated at this point in the flow —
// certificate/hash verification, when enabled, happens later — so they must
// be constrained to a known network name and the expected hex digest format
// first. expectedNetwork is the operator's own configured BootstrapConfig/
// SyncConfig.Network (already restricted to alphanumeric/hyphen/underscore
// by internal/config's ValidateNetworkName), which is trusted and accepted
// in addition to the fixed default-network list — this lets an operator
// bootstrap against a private/self-hosted aggregator for a non-default
// network (e.g. a devnet) without loosening what an untrusted aggregator
// response can put on a path. Pass "" when there is no such expectation.
func validateSnapshotIdentity(expectedNetwork, network, digest string) error {
	if network == "" ||
		(network != expectedNetwork && !slices.Contains(AcceptedNetworks(), network)) {
		return fmt.Errorf("unrecognized Mithril snapshot network %q", network)
	}
	if !digestPattern.MatchString(digest) {
		return fmt.Errorf("invalid Mithril snapshot digest %q", digest)
	}
	return nil
}

// hasFileInSubdirsIn checks whether a file with the given name exists in dir
// or any of its immediate subdirectories (one level deep), resolved through an
// open handle on an ancestor so every lookup stays inside the vetted tree.
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
