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
	"encoding/binary"
	"fmt"
	"os"
)

const (
	secondaryFileExtension = ".secondary"
)

type secondaryIndex struct {
	file     *os.File
	primary  *primaryIndex
	fileSize int64
}

type secondaryIndexEntryInner struct {
	BlockOffset  uint64
	HeaderOffset uint16
	HeaderSize   uint16
	Checksum     uint32
	HeaderHash   [32]byte
	BlockOrEbb   uint64
}

type secondaryIndexEntry struct {
	secondaryIndexEntryInner
	IsEbb bool
}

func newSecondaryIndex() *secondaryIndex {
	return &secondaryIndex{}
}

func (s *secondaryIndex) Open(path string, primary *primaryIndex) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	s.file = f
	s.primary = primary
	if stat, err := f.Stat(); err != nil {
		return err
	} else {
		s.fileSize = stat.Size()
	}
	return nil
}

func (s *secondaryIndex) Close() error {
	if err := s.primary.Close(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *secondaryIndex) Next() (*secondaryIndexEntry, error) {
	nextOccupied, err := s.primary.NextOccupied()
	if err != nil {
		return nil, err
	}
	if nextOccupied == nil {
		return nil, nil
	}
	// Look for final offset
	if int64(nextOccupied.SecondaryOffset) == s.fileSize {
		return nil, nil
	}
	// Seek to offset
	if _, err := s.file.Seek(int64(nextOccupied.SecondaryOffset), 0); err != nil {
		return nil, fmt.Errorf("failed while seeking: %w", err)
	}
	// Read entry
	var tmpEntryInner secondaryIndexEntryInner
	if err := binary.Read(s.file, binary.BigEndian, &tmpEntryInner); err != nil {
		return nil, fmt.Errorf("failed while reading: %w", err)
	}
	ret := &secondaryIndexEntry{
		secondaryIndexEntryInner: tmpEntryInner,
	}
	// Check for EBB
	// A block with its secondary index in the first slot with a small BlockOrEbb value is *probably* an EBB
	if nextOccupied.RelativeSlot == 0 && ret.BlockOrEbb < 200 {
		ret.IsEbb = true
	}
	return ret, nil
}
