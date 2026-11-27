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

package testutil

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/database/types"
)

// PayloadArchive is one synthetic block that a store test writes its payloads
// into and reads them back from, standing in for the block stores a payload
// column's offset names. A payload is placed at the first position it already
// occupies in the block, or appended to it, so the offsets it returns resolve
// to exactly the bytes located.
type PayloadArchive struct {
	mu    sync.Mutex
	block []byte
}

var (
	_ types.PayloadLocator  = (*PayloadArchive)(nil)
	_ types.PayloadResolver = (*PayloadArchive)(nil)
)

// payloadArchiveSlot and payloadArchiveHash identify the archive's block.
const payloadArchiveSlot uint64 = 1

var payloadArchiveHash = [32]byte{0xa5, 0xa5}

// NewPayloadArchive returns an empty archive.
func NewPayloadArchive() *PayloadArchive {
	return &PayloadArchive{}
}

// LocatePayload implements types.PayloadLocator.
func (a *PayloadArchive) LocatePayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("locate payload: empty payload")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	idx := bytes.Index(a.block, payload)
	if idx < 0 {
		idx = len(a.block)
		a.block = append(a.block, payload...)
	}
	offset := types.CborOffset{
		BlockSlot:  payloadArchiveSlot,
		BlockHash:  payloadArchiveHash,
		ByteOffset: uint32(idx),          //nolint:gosec // test payloads are small
		ByteLength: uint32(len(payload)), //nolint:gosec // test payloads are small
	}
	return offset.Encode(), nil
}

// ResolvePayload implements types.PayloadResolver.
func (a *PayloadArchive) ResolvePayload(encoded []byte) ([]byte, error) {
	offset, err := types.DecodeCborOffset(encoded)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
	if end > uint64(len(a.block)) {
		return nil, fmt.Errorf(
			"payload offset %d+%d is outside the %d-byte archive",
			offset.ByteOffset, offset.ByteLength, len(a.block),
		)
	}
	return bytes.Clone(a.block[offset.ByteOffset:end]), nil
}
