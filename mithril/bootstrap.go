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
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// BootstrapConfig holds configuration for the Mithril bootstrap
// process.
type BootstrapConfig struct {
	// Network is the Cardano network name (e.g., "mainnet",
	// "preprod", "preview").
	Network string
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
	// Logger is used for structured logging.
	Logger *slog.Logger
	// OnProgress is called during download with progress updates.
	OnProgress ProgressFunc
}

// BootstrapResult contains the result of a bootstrap operation.
type BootstrapResult struct {
	// Snapshot is the snapshot that was downloaded and extracted.
	Snapshot *SnapshotListItem
	// ImmutableDir is the path to the extracted ImmutableDB
	// directory.
	ImmutableDir string
	// ExtractDir is the root directory where the archive was
	// extracted. Contains db/immutable/, db/ledger/, etc.
	ExtractDir string
	// AncillaryDir is the root directory where the ancillary
	// archive was extracted. Contains ledger/<slot>/{meta,state,
	// tables/tvar}. Empty if no ancillary data was downloaded.
	AncillaryDir string
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
	)

	// Step 1: Fetch latest snapshot
	client := NewClient(aggregatorURL)
	snapshot, err := client.GetLatestSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"fetching latest snapshot: %w",
			err,
		)
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
		if err := VerifyCertificateChain(
			ctx, client, snapshot.CertificateHash,
			snapshot.Digest,
		); err != nil {
			return nil, fmt.Errorf(
				"certificate chain verification failed: %w",
				err,
			)
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

	// Step 3: Download snapshot archive (skip if already complete)
	archiveFilename := fmt.Sprintf(
		"%s-%s.tar.zst",
		snapshot.Network,
		truncateDigest(snapshot.Digest),
	)
	archivePath := filepath.Join(downloadDir, archiveFilename)

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
					URL:          loc,
					DestDir:      downloadDir,
					Filename:     archiveFilename,
					ExpectedSize: snapshot.Size,
					Logger:       cfg.Logger,
					OnProgress:   cfg.OnProgress,
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
	extractDir := filepath.Join(downloadDir, "immutable")
	var ancillaryDir string
	var ancillaryArchivePath string

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
		ancWg.Add(1)
		go func() {
			defer ancWg.Done()
			candidateDir := filepath.Join(
				downloadDir, "ancillary",
			)
			if hasLedgerFiles(candidateDir) {
				cfg.Logger.Info(
					"ancillary data already "+
						"extracted, skipping",
					"component", "mithril",
					"path", candidateDir,
				)
				ancillaryDir = candidateDir
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
			dir, archPath, ancErr := downloadAncillary(
				ancCtx, cfg, snapshot, downloadDir,
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
		}()
	}

	// Step 4: Extract main archive (skip if already extracted)
	immutableDir := findImmutableDir(extractDir)
	if immutableDir != "" {
		cfg.Logger.Info(
			"snapshot already extracted, skipping",
			"component", "mithril",
			"immutable_dir", immutableDir,
		)
	} else {
		_, err = ExtractArchive(
			ctx, archivePath, extractDir, cfg.Logger,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"extracting snapshot archive: %w",
				err,
			)
		}

		immutableDir = findImmutableDir(extractDir)
		if immutableDir == "" {
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
		"immutable_dir", immutableDir,
		"ancillary_dir", ancillaryDir,
	)

	success = true
	result := &BootstrapResult{
		Snapshot:             snapshot,
		ImmutableDir:         immutableDir,
		ExtractDir:           extractDir,
		AncillaryDir:         ancillaryDir,
		ArchivePath:          archivePath,
		AncillaryArchivePath: ancillaryArchivePath,
	}
	if createdTempDir {
		result.TempDir = downloadDir
	}
	return result, nil
}

// downloadAncillary downloads and extracts the ancillary archive
// which contains the ledger state in UTxO-HD format.
func downloadAncillary(
	ctx context.Context,
	cfg BootstrapConfig,
	snapshot *SnapshotListItem,
	downloadDir string,
) (dir string, archivePath string, err error) {
	if len(snapshot.AncillaryLocations) == 0 {
		return "", "", errors.New(
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

	var ancillaryPath string
	for i, loc := range snapshot.AncillaryLocations {
		ancillaryPath, err = DownloadSnapshot(
			ctx, DownloadConfig{
				URL:          loc,
				DestDir:      downloadDir,
				Filename:     ancillaryFilename,
				ExpectedSize: snapshot.AncillarySize,
				Logger:       cfg.Logger,
				OnProgress:   cfg.OnProgress,
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
		return "", "", fmt.Errorf(
			"downloading ancillary archive "+
				"(all %d locations failed): %w",
			len(snapshot.AncillaryLocations),
			err,
		)
	}

	ancillaryDir := filepath.Join(downloadDir, "ancillary")
	if _, extractErr := ExtractArchive(
		ctx, ancillaryPath, ancillaryDir, cfg.Logger,
	); extractErr != nil {
		return "", "", fmt.Errorf(
			"extracting ancillary archive: %w",
			extractErr,
		)
	}

	cfg.Logger.Info(
		"ancillary data extracted",
		"component", "mithril",
		"path", ancillaryDir,
	)

	return ancillaryDir, ancillaryPath, nil
}

// Cleanup removes the temporary files created during bootstrap.
// It removes the archive, extract directory, and ancillary
// directory individually rather than the entire parent directory,
// to avoid deleting user-specified download directories.
func (r *BootstrapResult) Cleanup(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
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

// findImmutableDir looks for the ImmutableDB directory in the
// extracted archive. It checks several common layouts:
//   - extractDir itself (contains .chunk files)
//   - extractDir/immutable/
//   - extractDir/db/immutable/
//   - any single top-level dir containing immutable/
func findImmutableDir(extractDir string) string {
	// Check if extractDir itself contains chunk files
	if hasChunkFiles(extractDir) {
		return extractDir
	}

	// Check common subdirectory layouts
	candidates := []string{
		filepath.Join(extractDir, "immutable"),
		filepath.Join(extractDir, "db", "immutable"),
	}
	for _, c := range candidates {
		if hasChunkFiles(c) {
			return c
		}
	}

	// Check for a single top-level directory
	entries, err := os.ReadDir(extractDir)
	if err != nil {
		return ""
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	if len(dirs) == 1 {
		// Check the single subdirectory
		subDir := filepath.Join(extractDir, dirs[0])
		if hasChunkFiles(subDir) {
			return subDir
		}
		// Check for immutable inside the single subdirectory
		immutableSub := filepath.Join(subDir, "immutable")
		if hasChunkFiles(immutableSub) {
			return immutableSub
		}
		// Check for db/immutable inside the single subdirectory
		dbImmutableSub := filepath.Join(
			subDir, "db", "immutable",
		)
		if hasChunkFiles(dbImmutableSub) {
			return dbImmutableSub
		}
	}

	return ""
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
	if client == nil {
		return errors.New("mithril client is nil")
	}
	if certificateHash == "" {
		return errors.New("certificate hash is empty")
	}

	const maxDepth = 100

	currentHash := certificateHash
	seen := make(map[string]bool)
	isLeaf := true

	for range maxDepth {
		if seen[currentHash] {
			return fmt.Errorf(
				"certificate chain cycle detected at %s",
				currentHash,
			)
		}
		seen[currentHash] = true

		cert, err := client.GetCertificate(ctx, currentHash)
		if err != nil {
			return fmt.Errorf(
				"fetching certificate %s: %w",
				currentHash,
				err,
			)
		}

		// Verify the leaf certificate binds to the snapshot
		if isLeaf && snapshotDigest != "" {
			certDigest := cert.ProtocolMessage.
				MessageParts["snapshot_digest"]
			if certDigest == "" {
				return fmt.Errorf(
					"leaf certificate %s is missing "+
						"snapshot_digest",
					currentHash,
				)
			}
			if certDigest != snapshotDigest {
				return fmt.Errorf(
					"certificate snapshot_digest "+
						"mismatch: cert has %q, "+
						"expected %q",
					certDigest,
					snapshotDigest,
				)
			}
		}
		isLeaf = false

		// Genesis certificate terminates the chain
		if cert.IsGenesis() || cert.IsChainingToItself() {
			return nil
		}

		if cert.PreviousHash == "" {
			return fmt.Errorf(
				"certificate %s has empty previous_hash "+
					"but is not genesis",
				currentHash,
			)
		}

		currentHash = cert.PreviousHash
	}

	return fmt.Errorf(
		"certificate chain exceeded maximum depth of %d",
		maxDepth,
	)
}

// hasChunkFiles checks if a directory contains ImmutableDB chunk
// files (*.chunk).
func hasChunkFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
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

// hasLedgerFiles checks if a directory contains ledger state files.
// It looks for any file named "state" in subdirectories, which is
// the UTxO-HD layout: ledger/<slot>/state.
func hasLedgerFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// Check for ledger/<subdir>/state or
		// ledger/<subdir>/<slot>/state
		sub := filepath.Join(dir, e.Name())
		if hasFileInSubdirs(sub, "state") {
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

// hasFileInSubdirs checks if a file with the given name exists in
// dir or any of its immediate subdirectories (one level deep).
func hasFileInSubdirs(dir string, name string) bool {
	target := filepath.Join(dir, name)
	if fi, err := os.Stat(target); err == nil && !fi.IsDir() {
		return true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		target := filepath.Join(dir, e.Name(), name)
		if fi, err := os.Stat(target); err == nil &&
			!fi.IsDir() {
			return true
		}
	}
	return false
}
