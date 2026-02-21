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
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
	if filename == "" || filename == "." || filename == "/" {
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
		if rangeStart != existingSize {
			resp.Body.Close()
			// Replace the closed body with a no-op closer
			// so the deferred resp.Body.Close() is safe.
			resp.Body = io.NopCloser(strings.NewReader(""))
			cfg.Logger.Warn(
				"Content-Range mismatch, restarting download",
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
