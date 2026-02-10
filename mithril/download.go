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

	// Ensure destination directory exists
	if err := os.MkdirAll(cfg.DestDir, 0o750); err != nil {
		return "", fmt.Errorf(
			"creating download directory: %w",
			err,
		)
	}

	filename := cfg.Filename
	if filename == "" {
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
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("downloading snapshot: %w", err)
	}
	if resp == nil || resp.Body == nil {
		return "", errors.New("nil response from download server")
	}
	defer resp.Body.Close()

	var totalSize int64
	var file *os.File

	switch resp.StatusCode {
	case http.StatusOK:
		// Full download, discard any partial file
		totalSize = resp.ContentLength
		file, err = os.Create(destPath)
		if err != nil {
			return "", fmt.Errorf(
				"creating destination file: %w",
				err,
			)
		}
	case http.StatusPartialContent:
		// Resume supported
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
	case http.StatusRequestedRangeNotSatisfiable:
		// The file is already complete — verify size before
		// accepting.
		if cfg.ExpectedSize > 0 {
			fi, err := os.Stat(destPath)
			if err != nil {
				return "", fmt.Errorf(
					"verifying existing download: %w",
					err,
				)
			}
			if fi.Size() != cfg.ExpectedSize {
				return "", fmt.Errorf(
					"existing file size mismatch: "+
						"got %d bytes, expected %d",
					fi.Size(), cfg.ExpectedSize,
				)
			}
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
	defer file.Close()

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
		return "", fmt.Errorf("writing snapshot data: %w", err)
	}

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
			return "", fmt.Errorf(
				"download size mismatch: got %d bytes, "+
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
