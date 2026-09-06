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
)

const (
	chunkFileExtension = ".chunk"
)

// ErrInvalidChunkOffset reports a secondary-index block offset that cannot be
// trusted to seek or slice the chunk file it names: it overflows int64,
// lands beyond the file's size, or does not strictly follow the offset
// before it. Callers can match it with errors.Is to distinguish a corrupt or
// tampered index from an ordinary I/O failure.
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
	if c.nextEntry == nil {
		if c.currentEntry.BlockOffset > math.MaxInt64 {
			return nil, fmt.Errorf(
				"%w: current block offset %d overflows int64",
				ErrInvalidChunkOffset,
				c.currentEntry.BlockOffset,
			)
		}
		// This triggers even though we check it above
		// #nosec G115
		currOffset := int64(c.currentEntry.BlockOffset)
		if currOffset > c.fileSize {
			return nil, fmt.Errorf(
				"%w: current block offset %d is beyond chunk size %d",
				ErrInvalidChunkOffset,
				currOffset,
				c.fileSize,
			)
		}

		// We've reached the last entry in the chunk, so we calculate
		// block size based on the size of the file
		blockSize := c.fileSize - currOffset
		if blockSize > int64(math.MaxInt) {
			return nil, fmt.Errorf(
				"block size %d exceeds platform allocation limit",
				blockSize,
			)
		}
		blockData := make([]byte, int(blockSize))
		// Seek to offset
		if _, err := c.file.Seek(currOffset, 0); err != nil {
			return nil, err
		}
		n, err := c.file.Read(blockData)
		if err != nil {
			return nil, err
		}
		if int64(n) < blockSize {
			return nil, fmt.Errorf(
				"did not read expected amount of block data: expected %d, got %d",
				blockSize,
				n,
			)
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
		c.currentEntry = nil
		c.nextEntry = nil
		return ret, nil
	} else {
		// Calculate block size based on the offsets for the current and next entries
		if c.currentEntry.BlockOffset > math.MaxInt64 {
			return nil, fmt.Errorf(
				"%w: current block offset %d overflows int64",
				ErrInvalidChunkOffset,
				c.currentEntry.BlockOffset,
			)
		}
		// This triggers even though we check it above
		// #nosec G115
		currOffset := int64(c.currentEntry.BlockOffset)

		if c.nextEntry.BlockOffset > math.MaxInt64 {
			return nil, fmt.Errorf(
				"%w: next block offset %d overflows int64",
				ErrInvalidChunkOffset,
				c.nextEntry.BlockOffset,
			)
		}
		// This triggers even though we check it above
		// #nosec G115
		nextOffset := int64(c.nextEntry.BlockOffset)
		if currOffset > c.fileSize {
			return nil, fmt.Errorf(
				"%w: current block offset %d is beyond chunk size %d",
				ErrInvalidChunkOffset,
				currOffset,
				c.fileSize,
			)
		}
		if nextOffset > c.fileSize {
			return nil, fmt.Errorf(
				"%w: next block offset %d is beyond chunk size %d",
				ErrInvalidChunkOffset,
				nextOffset,
				c.fileSize,
			)
		}
		if nextOffset <= currOffset {
			return nil, fmt.Errorf(
				"%w: next block offset %d does not follow current block offset %d",
				ErrInvalidChunkOffset,
				nextOffset,
				currOffset,
			)
		}

		blockSize := nextOffset - currOffset
		if blockSize > int64(math.MaxInt) {
			return nil, fmt.Errorf(
				"block size %d exceeds platform allocation limit",
				blockSize,
			)
		}
		blockData := make([]byte, int(blockSize))
		// Seek to offset
		if _, err := c.file.Seek(currOffset, 0); err != nil {
			return nil, err
		}
		n, err := c.file.Read(blockData)
		if err != nil {
			return nil, err
		}
		if int64(n) < blockSize {
			return nil, fmt.Errorf(
				"did not read expected amount of block data: expected %d, got %d",
				blockSize,
				n,
			)
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
		c.currentEntry = c.nextEntry
		nextEntry, err := c.secondary.Next()
		if err != nil {
			return nil, err
		}
		c.nextEntry = nextEntry
		return ret, nil
	}
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
