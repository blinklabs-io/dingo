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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewImmutableProgressByteBased verifies the percent reported to
// OnProgress tracks bytes (consistent with the BytesDownloaded/TotalBytes
// pair) rather than the archive count. The archive-count fraction
// previously disagreed with the byte counts logged alongside it, producing
// lines like "45.3% (8.2 GB / 14.2 GB)" where 8.2/14.2 is 57.7%.
func TestNewImmutableProgressByteBased(t *testing.T) {
	var last DownloadProgress
	cfg := BootstrapConfig{
		Logger:     slog.New(slog.DiscardHandler),
		OnProgress: func(p DownloadProgress) { last = p },
	}
	// 4 archives of uneven size summing to 1000 bytes.
	const totalArchives = 4
	const totalBytes = 1000
	onDone := newImmutableProgress(cfg, totalArchives, totalBytes)

	// 1 of 4 archives carrying 100 of 1000 bytes: archive-count fraction
	// is 25%, but the byte fraction is 10%. The reported percent must be
	// the byte fraction so it agrees with BytesDownloaded/TotalBytes.
	onDone(100)
	assert.Equal(t, int64(100), last.BytesDownloaded)
	assert.Equal(t, int64(totalBytes), last.TotalBytes)
	assert.InDelta(t, 10.0, last.Percent, 0.001,
		"percent must track bytes (10%%), not archive count (25%%)")

	// Completing all archives reaches the denominator and 100%.
	onDone(200)
	onDone(300)
	onDone(400)
	assert.Equal(t, int64(1000), last.BytesDownloaded)
	assert.InDelta(t, 100.0, last.Percent, 0.001)
}

// TestNewImmutableProgressClamp verifies the byte-percent is clamped to 100
// when accumulated bytes overshoot the estimated (average-derived) total.
func TestNewImmutableProgressClamp(t *testing.T) {
	var last DownloadProgress
	cfg := BootstrapConfig{
		Logger:     slog.New(slog.DiscardHandler),
		OnProgress: func(p DownloadProgress) { last = p },
	}
	onDone := newImmutableProgress(cfg, 2, 300) // estimated total 300
	onDone(200)
	onDone(200) // actual 400 > estimated 300
	assert.LessOrEqual(t, last.Percent, 100.0)
	assert.InDelta(t, 100.0, last.Percent, 0.001)
}

// TestNewImmutableProgressUnknownTotalFallback verifies that when the total
// size is unavailable the percent falls back to the archive-count fraction.
func TestNewImmutableProgressUnknownTotalFallback(t *testing.T) {
	var last DownloadProgress
	cfg := BootstrapConfig{
		Logger:     slog.New(slog.DiscardHandler),
		OnProgress: func(p DownloadProgress) { last = p },
	}
	onDone := newImmutableProgress(cfg, 4, 0) // total unknown
	onDone(123)
	assert.InDelta(t, 25.0, last.Percent, 0.001,
		"with unknown total, percent falls back to archive count")
}
