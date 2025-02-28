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
	"os"

	"github.com/blinklabs-io/gouroboros/cbor"
)

const (
	chunkFileExtension = ".chunk"
)

type chunk struct {
	file         *os.File
	fileSize     int64
	secondary    *secondaryIndex
	currentEntry *secondaryIndexEntry
	nextEntry    *secondaryIndexEntry
}

func newChunk() *chunk {
	return &chunk{}
}

func (c *chunk) Open(path string, secondary *secondaryIndex) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	c.file = f
	c.secondary = secondary
	if stat, err := f.Stat(); err != nil {
		return err
	} else {
		c.fileSize = stat.Size()
	}
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
			return nil, errors.New("current block offset integer overflow")
		}
		// This triggers even though we check it above
		// #nosec G115
		currOffset := int64(c.currentEntry.BlockOffset)

		// We've reached the last entry in the chunk, so we calculate
		// block size based on the size of the file
		blockSize := c.fileSize - currOffset
		blockData := make([]byte, blockSize)
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
			Cbor:  blkBytes[:],
		}
		c.currentEntry = nil
		c.nextEntry = nil
		return ret, nil
	} else {
		// Calculate block size based on the offsets for the current and next entries
		if c.currentEntry.BlockOffset > math.MaxInt64 {
			return nil, errors.New("current block offset integer overflow")
		}
		// This triggers even though we check it above
		// #nosec G115
		currOffset := int64(c.currentEntry.BlockOffset)

		if c.nextEntry.BlockOffset > math.MaxInt64 {
			return nil, errors.New("next block offset integer overflow")
		}
		// This triggers even though we check it above
		// #nosec G115
		nextOffset := int64(c.nextEntry.BlockOffset)

		blockSize := nextOffset - currOffset
		blockData := make([]byte, blockSize)
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
			Type: blkType,
			Slot: c.currentEntry.BlockOrEbb,
			Hash: c.currentEntry.HeaderHash[:],
			Cbor: blkBytes[:],
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
