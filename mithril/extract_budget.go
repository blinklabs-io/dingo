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

var errDownloadFreeSpace = errors.New("mithril download free-space reserve reached")

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

type downloadSpaceBudget struct {
	mu               sync.Mutex
	bytesUntilCheck  int64
	freeSpaceReserve uint64
	filesystemPath   string
}

func freeSpacePolicy(path, purpose string) (string, uint64, uint64, error) {
	filesystemPath := filepath.Clean(path)
	for {
		info, err := os.Stat(filesystemPath)
		if err == nil {
			if !info.IsDir() {
				return "", 0, 0, fmt.Errorf(
					"checking Mithril %s path: %s is not a directory",
					purpose,
					filesystemPath,
				)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", 0, 0, fmt.Errorf("checking Mithril %s path: %w", purpose, err)
		}
		parent := filepath.Dir(filesystemPath)
		if parent == filesystemPath {
			return "", 0, 0, fmt.Errorf("checking Mithril %s path: %w", purpose, err)
		}
		filesystemPath = parent
	}
	available, err := availableDiskBytes(filesystemPath)
	if err != nil {
		return "", 0, 0, fmt.Errorf(
			"checking Mithril %s free space: %w", purpose, err,
		)
	}
	reserve := max(uint64(minimumFreeSpaceReserve), available/10)
	if available <= reserve {
		return "", 0, 0, fmt.Errorf(
			"insufficient free space for Mithril %s", purpose,
		)
	}
	return filesystemPath, available, reserve, nil
}

func newDownloadSpaceBudget(path string) (*downloadSpaceBudget, error) {
	filesystemPath, available, reserve, err := freeSpacePolicy(path, "download")
	if err != nil {
		return nil, err
	}
	if available-reserve < freeSpaceCheckInterval {
		return nil, errors.New("insufficient free space for Mithril download")
	}
	return &downloadSpaceBudget{
		freeSpaceReserve: reserve,
		filesystemPath:   filesystemPath,
	}, nil
}

func (b *downloadSpaceBudget) reserveWrite(size int) error {
	if b == nil || size == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if size < 0 {
		return errors.New("invalid Mithril download write size")
	}
	if int64(size) <= b.bytesUntilCheck {
		b.bytesUntilCheck -= int64(size)
		return nil
	}
	available, err := availableDiskBytes(b.filesystemPath)
	if err != nil {
		return fmt.Errorf("checking Mithril download free space: %w", err)
	}
	needed := b.freeSpaceReserve + freeSpaceCheckInterval
	if needed < b.freeSpaceReserve || available < needed {
		return errDownloadFreeSpace
	}
	b.bytesUntilCheck = max(freeSpaceCheckInterval-int64(size), 0)
	return nil
}

func (b *downloadSpaceBudget) invalidate() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.bytesUntilCheck = 0
	b.mu.Unlock()
}

type downloadSpaceWriter struct {
	writer io.Writer
	budget *downloadSpaceBudget
}

func (w *downloadSpaceWriter) Write(p []byte) (int, error) {
	if err := w.budget.reserveWrite(len(p)); err != nil {
		return 0, err
	}
	n, err := w.writer.Write(p)
	if err != nil || n != len(p) {
		w.budget.invalidate()
	}
	return n, err
}

func newBootstrapExtractionBudget(
	downloadDir string,
) (*bootstrapExtractionBudget, error) {
	filesystemPath, available, reserve, err := freeSpacePolicy(
		downloadDir, "extraction",
	)
	if err != nil {
		return nil, err
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
