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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	maxBootstrapExtractEntries = 1 << 20
	minimumFreeSpaceReserve    = 1 << 30
	freeSpaceCheckInterval     = 64 << 20
)

type bootstrapExtractionBudget struct {
	mu             sync.Mutex
	maxBytes       int64
	maxStreamBytes int64
	maxEntries     int
	bytes          int64
	// streamBytes counts bytes consumed below tar.Reader separately, so skipped
	// bodies and metadata use the stream budget without double-counting disk writes.
	streamBytes      int64
	entries          int
	nextDiskCheck    int64
	freeSpaceReserve uint64
	filesystemPath   string
}

func newBootstrapExtractionBudget(
	downloadDir string,
) (*bootstrapExtractionBudget, error) {
	filesystemPath := filepath.Clean(downloadDir)
	for {
		info, err := os.Stat(filesystemPath)
		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf(
					"checking Mithril extraction path: %s is not a directory",
					filesystemPath,
				)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("checking Mithril extraction path: %w", err)
		}
		parent := filepath.Dir(filesystemPath)
		if parent == filesystemPath {
			return nil, fmt.Errorf("checking Mithril extraction path: %w", err)
		}
		filesystemPath = parent
	}
	available, err := availableDiskBytes(filesystemPath)
	if err != nil {
		return nil, fmt.Errorf("checking Mithril extraction free space: %w", err)
	}
	reserve := max(uint64(minimumFreeSpaceReserve), available/10)
	if available <= reserve {
		return nil, errors.New("insufficient free space for Mithril extraction")
	}
	usable := available - reserve
	maxBytes := int64(min(usable, uint64(maxTotalExtractSize))) // #nosec G115 -- capped at 1 TiB
	return &bootstrapExtractionBudget{
		maxBytes:         maxBytes,
		maxStreamBytes:   archiveStreamLimit(maxBytes, maxBootstrapExtractEntries),
		maxEntries:       maxBootstrapExtractEntries,
		nextDiskCheck:    freeSpaceCheckInterval,
		freeSpaceReserve: reserve,
		filesystemPath:   filesystemPath,
	}, nil
}

func (b *bootstrapExtractionBudget) reserveStreamBytes(size int64) error {
	if b == nil || size == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if size < 0 || b.streamBytes > b.maxStreamBytes-size {
		return fmt.Errorf("%w: bootstrap tar stream limit reached", ErrExtractLimitExceeded)
	}
	b.streamBytes += size
	return nil
}

func (b *bootstrapExtractionBudget) reserveEntries(count int) error {
	if b == nil || count == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if count < 0 || count > b.maxEntries ||
		b.entries > b.maxEntries-count {
		return fmt.Errorf("%w: bootstrap entry limit reached", ErrExtractLimitExceeded)
	}
	b.entries += count
	return nil
}

func (b *bootstrapExtractionBudget) reserveBytes(size int64) error {
	if b == nil || size == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if size < 0 || b.bytes > b.maxBytes-size {
		return fmt.Errorf("%w: bootstrap byte limit reached", ErrExtractLimitExceeded)
	}
	if b.bytes+size >= b.nextDiskCheck {
		available, err := availableDiskBytes(b.filesystemPath)
		if err != nil {
			return fmt.Errorf("checking Mithril extraction free space: %w", err)
		}
		needed := b.freeSpaceReserve + freeSpaceCheckInterval
		if needed < b.freeSpaceReserve || uint64(size) > available ||
			available-uint64(size) < needed {
			return fmt.Errorf("%w: free-space reserve reached", ErrExtractLimitExceeded)
		}
		b.nextDiskCheck = b.bytes + size + freeSpaceCheckInterval
	}
	b.bytes += size
	return nil
}

type extractionBudgetWriter struct {
	writer io.Writer
	budget *bootstrapExtractionBudget
}

func (w *extractionBudgetWriter) Write(p []byte) (int, error) {
	if err := w.budget.reserveBytes(int64(len(p))); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}
