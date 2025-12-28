// Copyright 2025 Blink Labs Software
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

package badger

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

type BlobStoreBadgerOptionFunc func(*BlobStoreBadger)

// WithLogger specifies the logger object to use for logging messages
func WithLogger(logger *slog.Logger) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.logger = logger
	}
}

// WithPromRegistry specifies the prometheus registry to use for metrics
func WithPromRegistry(
	registry prometheus.Registerer,
) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.promRegistry = registry
	}
}

// WithDataDir specifies the data directory to use for storage
func WithDataDir(dataDir string) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.dataDir = dataDir
	}
}

// WithBlockCacheSize specifies the block cache size
func WithBlockCacheSize(size uint64) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.blockCacheSize = size
	}
}

// WithIndexCacheSize specifies the index cache size
func WithIndexCacheSize(size uint64) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.indexCacheSize = size
	}
}

// WithGc specifies whether garbage collection is enabled
func WithGc(enabled bool) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.gcEnabled = enabled
	}
}

// WithValueLogFileSize specifies the value log file size in bytes
func WithValueLogFileSize(size int64) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.valueLogFileSize = size
	}
}

// WithMemTableSize specifies the memtable size in bytes
func WithMemTableSize(size int64) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.memTableSize = size
	}
}

// WithValueThreshold specifies the value threshold for keeping values in LSM tree
func WithValueThreshold(threshold int64) BlobStoreBadgerOptionFunc {
	return func(b *BlobStoreBadger) {
		b.valueThreshold = threshold
	}
}
