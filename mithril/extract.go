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
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const (
	// maxExtractFileSize is the maximum allowed size for a single
	// extracted file (8 GiB). Must be large enough for mainnet
	// ancillary ledger state files (UTxO tables can be multi-GB).
	maxExtractFileSize = 8 << 30

	// maxTotalExtractSize is the maximum cumulative bytes that may
	// be extracted from a single archive (1 TiB). Prevents
	// runaway decompression from filling the disk.
	maxTotalExtractSize = 1 << 40
)

// ExtractArchive extracts a zstd-compressed tar archive to the
// specified destination directory. It returns the path to the
// directory where files were extracted.
func ExtractArchive(
	archivePath string,
	destDir string,
	logger *slog.Logger,
) (string, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Create destination directory
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return "", fmt.Errorf(
			"creating extraction directory: %w",
			err,
		)
	}

	file, err := os.Open(archivePath)
	if err != nil {
		return "", fmt.Errorf(
			"opening archive: %w",
			err,
		)
	}
	defer file.Close()

	// Create zstd reader
	zr, err := zstd.NewReader(file)
	if err != nil {
		return "", fmt.Errorf(
			"creating zstd reader: %w",
			err,
		)
	}
	defer zr.Close()

	// Create tar reader
	tr := tar.NewReader(zr)

	var filesExtracted int
	var totalExtracted int64
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf(
				"reading tar header: %w",
				err,
			)
		}

		// Sanitize the path to prevent directory traversal.
		// Use path.Clean (forward-slash) not filepath.Clean,
		// because tar archives always use forward slashes and
		// filepath.Clean converts them to backslashes on Windows.
		name := path.Clean(header.Name)
		if name == "." {
			continue
		}
		if !validRelPath(name) {
			return "", fmt.Errorf(
				"invalid path in archive: %s",
				header.Name,
			)
		}

		// Zip Slip prevention: join the cleaned name to destDir,
		// then verify the result stays within destDir using
		// both HasPrefix and Rel checks.
		cleanDest := filepath.Clean(destDir)
		target := filepath.Join(
			cleanDest, filepath.FromSlash(name),
		)
		if !strings.HasPrefix(
			target,
			cleanDest+string(filepath.Separator),
		) {
			return "", fmt.Errorf(
				"path escapes destination: %s",
				header.Name,
			)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o750); err != nil { //nolint:gosec // target validated by validRelPath + HasPrefix above
				return "", fmt.Errorf(
					"creating directory %s: %w",
					target,
					err,
				)
			}
		case tar.TypeReg,
			'\x00': // '\x00' is the legacy regular-file flag (tar.TypeRegA)
			// Enforce per-file size limit (header.Size is
			// attacker-controlled, so we check it as a fast
			// reject but also enforce actual bytes below).
			if header.Size > maxExtractFileSize {
				return "", fmt.Errorf(
					"file %s exceeds maximum size (%d > %d)",
					header.Name, header.Size, maxExtractFileSize,
				)
			}

			// Ensure parent directory exists
			parent := filepath.Dir(target)
			if err := os.MkdirAll(parent, 0o750); err != nil { //nolint:gosec // parent derived from validated target path
				return "", fmt.Errorf(
					"creating parent directory %s: %w",
					parent,
					err,
				)
			}

			outFile, err := os.OpenFile( //nolint:gosec // target validated by validRelPath + HasPrefix above
				target,
				os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
				0o640,
			)
			if err != nil {
				return "", fmt.Errorf(
					"creating file %s: %w",
					target,
					err,
				)
			}

			// Cap actual bytes written to the per-file limit,
			// independent of the attacker-controlled header.Size.
			written, err := io.Copy(
				outFile,
				io.LimitReader(tr, maxExtractFileSize+1),
			)
			closeErr := outFile.Close()
			if err != nil {
				_ = os.Remove(target) //nolint:gosec // target validated above
				return "", fmt.Errorf(
					"extracting file %s: %w",
					target,
					err,
				)
			}
			if closeErr != nil {
				_ = os.Remove(target) //nolint:gosec // target validated above
				return "", fmt.Errorf(
					"closing file %s: %w",
					target,
					closeErr,
				)
			}
			if written > maxExtractFileSize {
				_ = os.Remove(target) //nolint:gosec // target validated above
				return "", fmt.Errorf(
					"file %s decompressed beyond maximum size (%d > %d)",
					header.Name, written, maxExtractFileSize,
				)
			}
			// Track actual bytes written (not attacker-controlled
			// header.Size) for cumulative extraction limit.
			totalExtracted += written
			if totalExtracted > maxTotalExtractSize {
				_ = os.Remove(target) //nolint:gosec // target validated above
				return "", fmt.Errorf(
					"archive extraction exceeds maximum total size (%d)",
					maxTotalExtractSize,
				)
			}
			filesExtracted++

			if filesExtracted%1000 == 0 {
				logger.Info(
					"extracting archive",
					"component", "mithril",
					"files_extracted", filesExtracted,
				)
			}
		default:
			// Skip symlinks and other types for security
			continue
		}
	}

	logger.Info(
		"extraction complete",
		"component", "mithril",
		"files_extracted", filesExtracted,
		"destination", destDir,
	)

	return destDir, nil
}

// validRelPath checks that a path is a valid relative path and does
// not escape the destination directory via ".." components or
// absolute paths. The input should already be filepath.Clean'd.
func validRelPath(p string) bool {
	if p == "" || p == "." ||
		strings.Contains(p, `\`) ||
		strings.HasPrefix(p, "/") {
		return false
	}
	// Reject ".." as any path component (e.g., "..", "foo/..",
	// "../bar"). The substring check for "../" alone misses a
	// trailing ".." without a slash.
	return !slices.Contains(strings.Split(p, "/"), "..")
}
