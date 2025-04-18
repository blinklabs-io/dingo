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
	"errors"
	"io"
	"os"
)

const (
	primaryFileExtension = ".primary"
)

type primaryIndex struct {
	file            *os.File
	slot            int
	lastOffset      uint32
	version         uint8
	seenFirstOffset bool
}

type primaryIndexEntry struct {
	RelativeSlot    int
	SecondaryOffset uint32
	empty           bool
}

func newPrimaryIndex() *primaryIndex {
	return &primaryIndex{}
}

func (e primaryIndexEntry) Empty() bool {
	return e.empty
}

func (p *primaryIndex) Open(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	p.file = f
	// Read version
	if err := binary.Read(f, binary.BigEndian, &p.version); err != nil {
		return err
	}
	return nil
}

func (p *primaryIndex) Close() error {
	return p.file.Close()
}

func (p *primaryIndex) Next() (*primaryIndexEntry, error) {
	var tmpOffset uint32
	if err := binary.Read(p.file, binary.BigEndian, &tmpOffset); err != nil {
		if errors.Is(err, io.EOF) {
			// We've reached the end of the file
			return nil, nil
		}
		return nil, err
	}
	empty := true
	if tmpOffset > p.lastOffset || !p.seenFirstOffset {
		empty = false
		p.lastOffset = tmpOffset
	}
	tmpEntry := primaryIndexEntry{
		RelativeSlot:    p.slot,
		SecondaryOffset: tmpOffset,
		empty:           empty,
	}
	p.slot++
	p.seenFirstOffset = true
	return &tmpEntry, nil
}

func (p *primaryIndex) NextOccupied() (*primaryIndexEntry, error) {
	for {
		next, err := p.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		if next.Empty() {
			continue
		}
		return next, nil
	}
	return nil, nil
}
