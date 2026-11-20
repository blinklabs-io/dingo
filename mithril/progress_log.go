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
	"fmt"
	"log/slog"
	"math"
)

// logDownloadProgress emits the operator-facing download progress line. The
// raw byte counts stay machine-readable; rate and percent are rounded and
// human-readable renderings are added alongside.
func logDownloadProgress(
	logger *slog.Logger,
	network string,
	artifact string,
	p DownloadProgress,
) {
	logger.Info(
		"download progress",
		"component", "mithril",
		"network", network,
		"phase", "download",
		"artifact", artifact,
		"snapshot_hash", p.SnapshotHash,
		"bytes_downloaded", p.BytesDownloaded,
		"total_bytes", p.TotalBytes,
		"downloaded", fmt.Sprintf(
			"%s / %s",
			formatSIBytes(p.BytesDownloaded),
			formatSIBytes(p.TotalBytes),
		),
		"percent", roundPercent(p.Percent),
		"bytes_per_second", wholeBytesPerSecond(p.BytesPerSecond),
		"rate", formatSIBytes(wholeBytesPerSecond(p.BytesPerSecond))+"/s",
		"artifacts_completed", p.ArtifactsCompleted,
		"artifacts_total", p.ArtifactsTotal,
	)
}

// roundPercent rounds to two decimal places.
func roundPercent(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*100) / 100
}

// wholeBytesPerSecond truncates a rate to an integer so it never renders in
// exponent notation. Non-finite and negative rates become zero.
func wholeBytesPerSecond(v float64) int64 {
	if math.IsNaN(v) || v <= 0 {
		return 0
	}
	if v >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// formatSIBytes renders a byte count with decimal (SI) units: kB, MB, GB.
// Values below 10 of a unit keep one decimal place; larger ones are rounded
// to a whole number.
func formatSIBytes(n int64) string {
	if n < 0 {
		n = 0
	}
	if n < 1000 {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"kB", "MB", "GB", "TB", "PB", "EB"}
	v := float64(n)
	i := -1
	// Carry at 999.5 rather than 1000 so a value that would print as
	// "1000 kB" is shown as "1.0 MB" instead.
	for v >= 999.5 && i < len(units)-1 {
		v /= 1000
		i++
	}
	if v < 9.95 {
		return fmt.Sprintf("%.1f %s", v, units[i])
	}
	return fmt.Sprintf("%.0f %s", v, units[i])
}
