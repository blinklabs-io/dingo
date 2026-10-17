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

package immutable

import (
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
)

const (
	chunkFileExtension = ".chunk"
	maxBlockSize       = int64(blockfetch.StreamingMaxPendingMessageBytes)
)

// ErrInvalidChunkOffset reports a secondary-index block offset that cannot be
// trusted to seek or slice the chunk file it names: it overflows int64,
// lands beyond the file's size, does not strictly follow the offset before it,
// or describes a block larger than the network permits. Callers can match it
// with errors.Is to distinguish a corrupt or tampered index from an ordinary
// I/O failure.
var ErrInvalidChunkOffset = errors.New("invalid chunk offset")

type chunk struct {
	file         entryReader
	secondary    *secondaryIndex
	currentEntry *secondaryIndexEntry
	nextEntry    *secondaryIndexEntry
	fileSize     int64
}

func newChunk() *chunk {
	return &chunk{}
}

// Open takes an already-open chunk file rather than a path so the caller
// decides how it was resolved — by name, or through a directory handle that
// binds the read to a directory somebody else cannot repoint.
func (c *chunk) Open(f entryReader, secondary *secondaryIndex) error {
	c.file = f
	c.secondary = secondary
	size, err := f.Size()
	if err != nil {
		return err
	}
	c.fileSize = size
	currentEntry, err := secondary.Next()
	if err != nil {
		return err
	}
	c.currentEntry = currentEntry
	nextEntry, err := secondary.Next()
	if err != nil {
		return err
	}
	c.nextEntry = nextEntry
	return nil
}

func (c *chunk) Close() error {
	if err := c.secondary.Close(); err != nil {
		return err
	}
	return c.file.Close()
}

func (c *chunk) Next() (*Block, error) {
	if c.currentEntry == nil {
		return nil, nil
	}
	last := c.nextEntry == nil
	endOffset := uint64(c.fileSize) // #nosec G115 -- file sizes cannot be negative
	if !last {
		endOffset = c.nextEntry.BlockOffset
	}
	currOffset, blockSize, err := validBlockSpan(
		c.currentEntry.BlockOffset,
		endOffset,
		c.fileSize,
	)
	if err != nil {
		return nil, err
	}
	blockData, err := c.readBlockData(currOffset, blockSize)
	if err != nil {
		return nil, err
	}
	blkType, blkBytes, err := c.unwrapBlock(blockData)
	if err != nil {
		return nil, err
	}
	ret := &Block{
		Type:  blkType,
		Slot:  c.currentEntry.BlockOrEbb,
		Hash:  c.currentEntry.HeaderHash[:],
		IsEbb: c.currentEntry.IsEbb,
		Cbor:  blkBytes,
	}
	if last {
		c.currentEntry = nil
		c.nextEntry = nil
	} else {
		c.currentEntry = c.nextEntry
		nextEntry, err := c.secondary.Next()
		if err != nil {
			return nil, err
		}
		c.nextEntry = nextEntry
	}
	return ret, nil
}

func validBlockSpan(start, end uint64, fileSize int64) (int64, int64, error) {
	if start > math.MaxInt64 {
		return 0, 0, fmt.Errorf(
			"%w: current block offset %d overflows int64",
			ErrInvalidChunkOffset,
			start,
		)
	}
	if end > math.MaxInt64 {
		return 0, 0, fmt.Errorf(
			"%w: next block offset %d overflows int64",
			ErrInvalidChunkOffset,
			end,
		)
	}
	current := int64(start) // #nosec G115 -- guarded above
	next := int64(end)      // #nosec G115 -- guarded above
	if current > fileSize {
		return 0, 0, fmt.Errorf(
			"%w: current block offset %d is beyond chunk size %d",
			ErrInvalidChunkOffset,
			current,
			fileSize,
		)
	}
	if next > fileSize {
		return 0, 0, fmt.Errorf(
			"%w: next block offset %d is beyond chunk size %d",
			ErrInvalidChunkOffset,
			next,
			fileSize,
		)
	}
	if next <= current {
		return 0, 0, fmt.Errorf(
			"%w: next block offset %d does not follow current block offset %d",
			ErrInvalidChunkOffset,
			next,
			current,
		)
	}
	blockSize := next - current
	if blockSize > maxBlockSize {
		return 0, 0, fmt.Errorf(
			"%w: block span %d exceeds maximum %d",
			ErrInvalidChunkOffset,
			blockSize,
			maxBlockSize,
		)
	}
	return current, blockSize, nil
}

type rangeEntryReader interface {
	RangeAt(offset, length int64) ([]byte, bool)
}

func (c *chunk) readBlockData(offset, size int64) ([]byte, error) {
	if reader, ok := c.file.(rangeEntryReader); ok {
		data, valid := reader.RangeAt(offset, size)
		if !valid {
			return nil, fmt.Errorf(
				"%w: block range [%d:%d] exceeds chunk size %d",
				ErrInvalidChunkOffset,
				offset,
				offset+size,
				c.fileSize,
			)
		}
		return data, nil
	}
	blockData := make([]byte, int(size))
	n, err := c.file.ReadAt(blockData, offset)
	if err != nil {
		return nil, err
	}
	if int64(n) < size {
		return nil, fmt.Errorf(
			"did not read expected amount of block data: expected %d, got %d",
			size,
			n,
		)
	}
	return blockData, nil
}

func (c *chunk) unwrapBlock(data []byte) (uint, []byte, error) {
	tmpData := struct {
		cbor.StructAsArray
		BlockType uint
		BlockCbor cbor.RawMessage
	}{}
	if _, err := cbor.Decode(data, &tmpData); err != nil {
		return 0, nil, err
	}
	return tmpData.BlockType, []byte(tmpData.BlockCbor), nil
}
