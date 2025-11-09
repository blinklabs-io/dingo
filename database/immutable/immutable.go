// Copyright 2024 Blink Labs Software
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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type ImmutableDb struct {
	dataDir string
}

type Block struct {
	Hash  []byte
	Cbor  []byte
	Type  uint
	Slot  uint64
	IsEbb bool
}

// New returns a new ImmutableDb using the specified data directory or an error
func New(dataDir string) (*ImmutableDb, error) {
	if _, err := os.Stat(dataDir); err != nil {
		return nil, err
	}
	i := &ImmutableDb{
		dataDir: dataDir,
	}
	return i, nil
}

func (i *ImmutableDb) getChunkNames() ([]string, error) {
	ret := []string{}
	files, err := os.ReadDir(i.dataDir)
	if err != nil {
		return nil, err
	}
	for _, entry := range files {
		entryName := entry.Name()
		entryExt := filepath.Ext(entryName)
		if entryExt != chunkFileExtension {
			continue
		}
		chunkName := strings.TrimSuffix(entryName, entryExt)
		ret = append(ret, chunkName)
	}
	slices.Sort(ret)
	return ret, nil
}

func (i *ImmutableDb) getChunkNamesFromPoint(
	point ocommon.Point,
) ([]string, error) {
	chunkNames, err := i.getChunkNames()
	if err != nil {
		return nil, err
	}
	// Return all chunks for the origin
	if point.Slot == 0 {
		return chunkNames, nil
	}
	lowerBound := 0
	upperBound := len(chunkNames)
	for lowerBound <= upperBound {
		// Get chunk in the middle of the current bounds
		middlePoint := (lowerBound + upperBound) / 2
		middleChunkName := chunkNames[middlePoint]
		middleSecondary, err := i.getChunkSecondaryIndex(middleChunkName)
		if err != nil {
			return nil, err
		}
		next, err := middleSecondary.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		startSlot := next.BlockOrEbb
		var endSlot uint64
		for {
			next, err := middleSecondary.Next()
			if err != nil {
				return nil, err
			}
			if next == nil {
				break
			}
			endSlot = next.BlockOrEbb
		}
		if point.Slot < startSlot {
			// The slot we're looking for is less than the first slot in the
			// chunk, so
			// we can eliminate all later chunks
			upperBound = middlePoint - 1
		} else if point.Slot > endSlot {
			// The slot we're looking for is greater than the last slot in the
			// chunk, so
			// we can eliminate all earlier chunks
			lowerBound = middlePoint + 1
		} else {
			// We found the chunk that (probably) has the requested point
			break
		}
	}
	return chunkNames[lowerBound:], nil
}

func (i *ImmutableDb) getChunkPrimaryIndex(
	chunkName string,
) (*primaryIndex, error) {
	primaryFilePath := filepath.Join(
		i.dataDir,
		chunkName+primaryFileExtension,
	)
	primary := newPrimaryIndex()
	if err := primary.Open(primaryFilePath); err != nil {
		return nil, fmt.Errorf(
			"failed to read primary index: %s: %w",
			primaryFilePath,
			err,
		)
	}
	return primary, nil
}

func (i *ImmutableDb) getChunkSecondaryIndex(
	chunkName string,
) (*secondaryIndex, error) {
	primary, err := i.getChunkPrimaryIndex(chunkName)
	if err != nil {
		return nil, err
	}
	secondaryFilePath := filepath.Join(
		i.dataDir,
		chunkName+secondaryFileExtension,
	)
	secondary := newSecondaryIndex()
	if err := secondary.Open(secondaryFilePath, primary); err != nil {
		return nil, fmt.Errorf(
			"failed to read secondary index: %s: %w",
			secondaryFilePath,
			err,
		)
	}
	return secondary, nil
}

func (i *ImmutableDb) getChunk(chunkName string) (*chunk, error) {
	// Open secondary index
	secondary, err := i.getChunkSecondaryIndex(chunkName)
	if err != nil {
		return nil, err
	}
	// Open chunk
	chunkFilePath := filepath.Join(
		i.dataDir,
		chunkName+chunkFileExtension,
	)
	chunk := newChunk()
	if err := chunk.Open(chunkFilePath, secondary); err != nil {
		return nil, fmt.Errorf(
			"failed to read chunk: %s: %w",
			chunkFilePath,
			err,
		)
	}
	return chunk, nil
}

func (i *ImmutableDb) GetTip() (*ocommon.Point, error) {
	var ret *ocommon.Point
	chunkNames, err := i.getChunkNames()
	if err != nil {
		return nil, err
	}
	if len(chunkNames) == 0 {
		return nil, nil
	}
	secondary, err := i.getChunkSecondaryIndex(chunkNames[len(chunkNames)-1])
	if err != nil {
		return nil, err
	}
	var tmpPoint ocommon.Point
	for {
		next, err := secondary.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		tmpPoint = ocommon.NewPoint(
			next.BlockOrEbb,
			next.HeaderHash[:],
		)
		ret = &tmpPoint
	}
	return ret, nil
}

func (i *ImmutableDb) GetBlock(point ocommon.Point) (*Block, error) {
	var err error
	chunkNames, err := i.getChunkNamesFromPoint(point)
	if err != nil {
		return nil, err
	}
	chunk, err := i.getChunk(chunkNames[0])
	if err != nil {
		return nil, err
	}
	var tmpBlock *Block
	for {
		tmpBlock, err = chunk.Next()
		if err != nil {
			return nil, err
		}
		if tmpBlock == nil {
			break
		}
		if tmpBlock.Slot != point.Slot {
			continue
		}
		if string(tmpBlock.Hash) != string(point.Hash) {
			continue
		}
		return tmpBlock, nil
	}
	return nil, nil
}

func (i *ImmutableDb) TruncateChunksFromPoint(point ocommon.Point) error {
	chunkNames, err := i.getChunkNamesFromPoint(point)
	if err != nil {
		return err
	}
	for _, chunkName := range chunkNames {
		chunkPathPrefix := filepath.Join(
			i.dataDir,
			chunkName,
		)
		if err := os.Remove(chunkPathPrefix + chunkFileExtension); err != nil {
			return err
		}
		if err := os.Remove(chunkPathPrefix + secondaryFileExtension); err != nil {
			return err
		}
		if err := os.Remove(chunkPathPrefix + primaryFileExtension); err != nil {
			return err
		}
	}
	return nil
}

func (i *ImmutableDb) BlocksFromPoint(
	point ocommon.Point,
) (*BlockIterator, error) {
	chunkNames, err := i.getChunkNamesFromPoint(point)
	if err != nil {
		return nil, err
	}
	ret := &BlockIterator{
		db:         i,
		chunkNames: chunkNames,
		startPoint: point,
	}
	return ret, nil
}

type BlockIterator struct {
	db              *ImmutableDb
	chunk           *chunk
	startPoint      ocommon.Point
	chunkNames      []string
	chunkIdx        int
	foundStartPoint bool
}

func (b *BlockIterator) Next() (*Block, error) {
	var err error
	var tmpChunk *chunk
	if b.chunk == nil {
		if b.chunkIdx == 0 && len(b.chunkNames) > 0 {
			// Open initial chunk
			tmpChunk, err = b.db.getChunk(b.chunkNames[b.chunkIdx])
			if err != nil {
				return nil, err
			}
			b.chunk = tmpChunk
		} else {
			return nil, nil
		}
	}
	var tmpBlock *Block
	for {
		tmpBlock, err = b.chunk.Next()
		if err != nil {
			return nil, err
		}
		if tmpBlock == nil {
			// We've reached the end of the current chunk
			if err := b.chunk.Close(); err != nil {
				return nil, err
			}
			b.chunk = nil
			b.chunkIdx++
			if b.chunkIdx >= len(b.chunkNames) {
				return nil, nil
			}
			tmpChunk, err = b.db.getChunk(b.chunkNames[b.chunkIdx])
			if err != nil {
				return nil, err
			}
			b.chunk = tmpChunk
			continue
		}
		if !b.foundStartPoint {
			if tmpBlock.Slot < b.startPoint.Slot {
				continue
			}
			b.foundStartPoint = true
		}
		return tmpBlock, nil
	}
}

func (b *BlockIterator) Close() error {
	if b.chunk != nil {
		if err := b.chunk.Close(); err != nil {
			return err
		}
	}
	return nil
}
