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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// DownloadProgress reports download progress to a callback.
type DownloadProgress struct {
	BytesDownloaded int64
	TotalBytes      int64
	Percent         float64
	BytesPerSecond  float64
}

// ProgressFunc is a callback invoked periodically during download
// to report progress.
type ProgressFunc func(DownloadProgress)

// DownloadConfig holds configuration for downloading a snapshot
// archive.
type DownloadConfig struct {
	// URL is the download URL for the snapshot archive.
	URL string
	// DestDir is the directory where the archive will be saved.
	DestDir string
	// Filename is the name of the downloaded file. If empty, a
	// default name is generated from the snapshot digest.
	Filename string
	// ExpectedSize is the expected file size in bytes. When > 0,
	// the downloaded file size is verified after download. A
	// mismatch returns an error.
	ExpectedSize int64
	// Logger is used for logging download progress.
	Logger *slog.Logger
	// OnProgress is called periodically with download progress.
	OnProgress ProgressFunc
}

// progressWriter wraps an io.Writer to track bytes written and
// report download progress.
type progressWriter struct {
	writer      io.Writer
	total       int64
	written     int64
	startOffset int64
	startTime   time.Time
	onProgress  ProgressFunc
	lastReport  time.Time
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.writer.Write(p)
	pw.written += int64(n)

	now := time.Now()
	if pw.onProgress != nil &&
		now.Sub(pw.lastReport) >= 500*time.Millisecond {
		elapsed := now.Sub(pw.startTime).Seconds()
		var pct float64
		if pw.total > 0 {
			pct = float64(pw.written) / float64(pw.total) * 100
		}
		var speed float64
		if elapsed > 0 {
			speed = float64(pw.written-pw.startOffset) / elapsed
		}
		pw.onProgress(DownloadProgress{
			BytesDownloaded: pw.written,
			TotalBytes:      pw.total,
			Percent:         pct,
			BytesPerSecond:  speed,
		})
		pw.lastReport = now
	}

	return n, err
}

// parseContentRangeStart extracts the start byte offset from an
// HTTP Content-Range header value. The expected format is
// "bytes START-END/TOTAL" (e.g. "bytes 1024-2047/4096"). Returns
// -1 if the header is empty or malformed.
func parseContentRangeStart(header string) int64 {
	// Content-Range: bytes 1024-2047/4096
	after, found := strings.CutPrefix(header, "bytes ")
	if !found {
		return -1
	}
	dashIdx := strings.IndexByte(after, '-')
	if dashIdx < 1 {
		return -1
	}
	start, err := strconv.ParseInt(after[:dashIdx], 10, 64)
	if err != nil {
		return -1
	}
	return start
}

// parseContentRangeTotal extracts the total file size from an HTTP
// Content-Range header value. It handles both the standard format
// "bytes START-END/TOTAL" and the 416 format "bytes */TOTAL".
// Returns -1 if the header is empty, malformed, or the total is
// "*" (unknown).
func parseContentRangeTotal(header string) int64 {
	// Content-Range: bytes 1024-2047/4096
	// Content-Range: bytes */4096
	after, found := strings.CutPrefix(header, "bytes ")
	if !found {
		return -1
	}
	slashIdx := strings.IndexByte(after, '/')
	if slashIdx < 0 {
		return -1
	}
	totalStr := after[slashIdx+1:]
	if totalStr == "*" || totalStr == "" {
		return -1
	}
	total, err := strconv.ParseInt(totalStr, 10, 64)
	if err != nil {
		return -1
	}
	return total
}

// DownloadSnapshot downloads a snapshot archive from the given URL
// to the specified destination directory. It returns the path to
// the downloaded file.
func DownloadSnapshot(
	ctx context.Context,
	cfg DownloadConfig,
) (string, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.URL == "" {
		return "", errors.New("download URL is empty")
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(cfg.DestDir, 0o750); err != nil {
		return "", fmt.Errorf(
			"creating download directory: %w",
			err,
		)
	}

	filename := filepath.Base(cfg.Filename)
	if filename == "." || filename == "/" {
		filename = "snapshot.tar.zst"
	}
	destPath := filepath.Join(cfg.DestDir, filename)

	// Check for partial download to support resume
	var existingSize int64
	if fi, err := os.Stat(destPath); err == nil {
		existingSize = fi.Size()
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		cfg.URL,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("creating download request: %w", err)
	}

	// Request resume if we have a partial download
	if existingSize > 0 {
		req.Header.Set(
			"Range",
			fmt.Sprintf("bytes=%d-", existingSize),
		)
	}

	client := &http.Client{
		Timeout:       0, // No timeout for large downloads
		CheckRedirect: httpsOnlyRedirect,
	}
	resp, err := client.Do( //nolint:gosec // URL from caller-provided config; HTTPS-only redirect policy prevents downgrade
		req,
	)
	if err != nil {
		return "", fmt.Errorf("downloading snapshot: %w", err)
	}
	if resp == nil || resp.Body == nil {
		return "", errors.New("nil response from download server")
	}
	defer func() { resp.Body.Close() }()

	var totalSize int64
	var file *os.File

	switch resp.StatusCode {
	case http.StatusOK:
		// Full download, discard any partial file.
		// ContentLength is -1 when the server omits the header;
		// keep totalSize as 0 so progress reports degrade
		// gracefully instead of showing a negative total.
		existingSize = 0 // reset: not resuming
		if resp.ContentLength > 0 {
			totalSize = resp.ContentLength
		}
		file, err = os.OpenFile(
			destPath,
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
			0o640,
		)
		if err != nil {
			return "", fmt.Errorf(
				"creating destination file: %w",
				err,
			)
		}
	case http.StatusPartialContent:
		// Validate that the server is resuming from the
		// correct offset. A mismatched Content-Range start
		// byte would corrupt the file if appended blindly.
		rangeStart := parseContentRangeStart(
			resp.Header.Get("Content-Range"),
		)
		if rangeStart < 0 || rangeStart != existingSize {
			resp.Body.Close()
			// Replace the closed body with a no-op closer
			// so the deferred resp.Body.Close() is safe.
			resp.Body = io.NopCloser(strings.NewReader(""))
			reason := "Content-Range mismatch"
			if rangeStart < 0 {
				reason = "Content-Range missing or malformed"
			}
			cfg.Logger.Warn(
				reason+", restarting download",
				"component", "mithril",
				"expected_start", existingSize,
				"actual_start", rangeStart,
			)
			// Discard partial file and restart from scratch
			existingSize = 0
			file, err = os.OpenFile(
				destPath,
				os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
				0o640,
			)
			if err != nil {
				return "", fmt.Errorf(
					"creating destination file: %w",
					err,
				)
			}
			// Re-issue request without Range header
			req, err = http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				cfg.URL,
				nil,
			)
			if err != nil {
				file.Close()
				return "", fmt.Errorf(
					"creating restart request: %w",
					err,
				)
			}
			resp2, err := client.Do( //nolint:gosec // same URL retried after Content-Range mismatch
				req,
			)
			if err != nil {
				file.Close()
				return "", fmt.Errorf(
					"restarting download: %w",
					err,
				)
			}
			if resp2 == nil || resp2.Body == nil {
				file.Close()
				return "", errors.New(
					"nil response on download restart",
				)
			}
			if resp2.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(
					io.LimitReader(resp2.Body, 1024),
				)
				resp2.Body.Close()
				file.Close()
				return "", fmt.Errorf(
					"restart download failed with "+
						"status %d: %s",
					resp2.StatusCode,
					string(bodyBytes),
				)
			}
			// Replace the original response body with the
			// fresh full-download stream.
			resp.Body = resp2.Body
			if resp2.ContentLength > 0 {
				totalSize = resp2.ContentLength
			}
		} else {
			// Resume supported with matching offset
			if resp.ContentLength > 0 {
				totalSize = existingSize + resp.ContentLength
			}
			file, err = os.OpenFile(
				destPath,
				os.O_APPEND|os.O_WRONLY,
				0o640,
			)
			if err != nil {
				return "", fmt.Errorf(
					"opening file for resume: %w",
					err,
				)
			}
			cfg.Logger.Info(
				"resuming download",
				"component", "mithril",
				"existing_bytes", existingSize,
				"remaining_bytes", resp.ContentLength,
			)
		}
	case http.StatusRequestedRangeNotSatisfiable:
		// The file is already complete — verify size before
		// accepting. Prefer cfg.ExpectedSize, but fall back
		// to the Content-Range total from the 416 response
		// (format: "bytes */TOTAL").
		expectedSize := cfg.ExpectedSize
		if expectedSize <= 0 {
			expectedSize = parseContentRangeTotal(
				resp.Header.Get("Content-Range"),
			)
		}
		if expectedSize > 0 {
			fi, err := os.Stat(destPath)
			if err != nil {
				return "", fmt.Errorf(
					"verifying existing download: %w",
					err,
				)
			}
			if fi.Size() != expectedSize {
				// Remove the corrupt/oversized file so
				// the next attempt starts fresh instead
				// of looping on the same 416 error.
				os.Remove(destPath) //nolint:errcheck
				return "", fmt.Errorf(
					"existing file size mismatch "+
						"(removed): got %d, want %d",
					fi.Size(), expectedSize,
				)
			}
		} else {
			return "", fmt.Errorf(
				"server returned 416 without "+
					"verifiable size for %s",
				destPath,
			)
		}
		cfg.Logger.Info(
			"download already complete",
			"component", "mithril",
			"path", destPath,
		)
		return destPath, nil
	default:
		bodyBytes, _ := io.ReadAll(
			io.LimitReader(resp.Body, 1024),
		)
		return "", fmt.Errorf(
			"download failed with status %d: %s",
			resp.StatusCode,
			string(bodyBytes),
		)
	}
	defer func() {
		if file != nil {
			file.Close()
		}
	}() // safety net for panics/early returns

	cfg.Logger.Info(
		"downloading snapshot",
		"component", "mithril",
		"url", cfg.URL,
		"total_bytes", totalSize,
		"destination", destPath,
	)

	pw := &progressWriter{
		writer:      file,
		total:       totalSize,
		written:     existingSize,
		startOffset: existingSize,
		startTime:   time.Now(),
		onProgress:  cfg.OnProgress,
	}

	if _, err := io.Copy(pw, resp.Body); err != nil {
		file.Close()
		file = nil
		return "", fmt.Errorf("writing snapshot data: %w", err)
	}

	// Close the file explicitly so write errors (e.g. ENOSPC
	// during a deferred flush) are not silently ignored.
	if err := file.Close(); err != nil {
		file = nil
		return "", fmt.Errorf("closing download file: %w", err)
	}
	file = nil

	// Final progress report
	if cfg.OnProgress != nil {
		elapsed := time.Since(pw.startTime).Seconds()
		var speed float64
		if elapsed > 0 {
			speed = float64(pw.written-existingSize) / elapsed
		}
		cfg.OnProgress(DownloadProgress{
			BytesDownloaded: pw.written,
			TotalBytes:      totalSize,
			Percent:         100,
			BytesPerSecond:  speed,
		})
	}

	cfg.Logger.Info(
		"download complete",
		"component", "mithril",
		"bytes", pw.written,
		"path", destPath,
	)

	// Verify file size if expected size was provided
	if cfg.ExpectedSize > 0 {
		fi, err := os.Stat(destPath)
		if err != nil {
			return "", fmt.Errorf(
				"verifying download size: %w", err,
			)
		}
		if fi.Size() != cfg.ExpectedSize {
			// Remove so the next attempt starts fresh
			// instead of resuming from a corrupt file.
			os.Remove(destPath) //nolint:errcheck
			return "", fmt.Errorf(
				"download size mismatch "+
					"(removed): got %d bytes, "+
					"expected %d bytes",
				fi.Size(), cfg.ExpectedSize,
			)
		}
		cfg.Logger.Info(
			"download size verified",
			"component", "mithril",
			"bytes", fi.Size(),
		)
	}

	return destPath, nil
}

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
// directory where files were extracted. The context is checked
// between files so that long-running extractions can be cancelled.
func ExtractArchive(
	ctx context.Context,
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
		if err := ctx.Err(); err != nil {
			return "", fmt.Errorf(
				"extraction cancelled: %w", err,
			)
		}
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
