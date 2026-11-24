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

package types

import (
	"bytes"
	"errors"
	"fmt"
	"math"
)

// ErrPayloadLocatorRequired reports a write of a payload column that was not
// given the block the payload was decoded from. The column holds a CBOR
// offset into that block, never a copy of the bytes.
var ErrPayloadLocatorRequired = errors.New(
	"payload column write has no block to locate the payload in",
)

// ErrPayloadResolverRequired reports a read of a payload column by a store
// that was not given a block source to resolve offsets through.
var ErrPayloadResolverRequired = errors.New(
	"payload column read has no block source to resolve the offset through",
)

// PayloadLocator finds a payload inside the block it was decoded from and
// returns the encoded CBOR offset that names it. It fails when the block does
// not contain the exact bytes.
type PayloadLocator interface {
	LocatePayload(payload []byte) ([]byte, error)
}

// ContainedPayloadLocator is implemented by a PayloadLocator that can prefer
// the occurrence of a payload inside a container, the CBOR of the output that
// carries it, over an earlier identical occurrence elsewhere in the block.
type ContainedPayloadLocator interface {
	LocatePayloadIn(payload, container []byte) ([]byte, error)
}

// PayloadResolver returns the bytes an encoded CBOR offset names, reading the
// block from the block stores. The result is a copy the caller may keep.
type PayloadResolver interface {
	ResolvePayload(offset []byte) ([]byte, error)
}

// PayloadLocatorSetter is implemented by metadata transactions and batch
// accumulators that record payload columns. The writer sets the locator of
// the block before it writes each transaction of it.
type PayloadLocatorSetter interface {
	SetPayloadLocator(PayloadLocator)
}

// PayloadLocatorSource is implemented by the metadata transactions that take a
// payload locator, and returns the one last set, or nil.
type PayloadLocatorSource interface {
	PayloadLocator() PayloadLocator
}

// PayloadResolverSetter is implemented by metadata stores whose readers
// resolve payload offsets.
type PayloadResolverSetter interface {
	SetPayloadResolver(PayloadResolver)
}

// blockPayloadLocator names payloads by their position in one block's CBOR.
// The offset it returns reads back exactly the bytes it was given, because
// the position is found by matching them in the block rather than by
// reconstructing where an encoder would have put them.
type blockPayloadLocator struct {
	block    []byte
	slot     uint64
	hash     [32]byte
	endorser bool
}

// NewBlockPayloadLocator returns the locator of the block at slot and hash
// whose CBOR is block. The locator reads block without copying it.
func NewBlockPayloadLocator(
	slot uint64,
	hash []byte,
	block []byte,
) (PayloadLocator, error) {
	loc := &blockPayloadLocator{block: block, slot: slot}
	if len(hash) != len(loc.hash) {
		return nil, fmt.Errorf(
			"invalid block hash: expected %d bytes, got %d",
			len(loc.hash),
			len(hash),
		)
	}
	if uint64(len(block)) > math.MaxUint32 {
		return nil, fmt.Errorf(
			"block of %d bytes exceeds the offset range", len(block),
		)
	}
	copy(loc.hash[:], hash)
	return loc, nil
}

// NewEndorserPayloadLocator returns the locator of the Leios endorser block at
// slot and hash whose raw transaction list encodes to backing, which is where
// the transactions of a certified closure live. Its offsets carry
// EndorserOffsetMagic. The locator reads backing without copying it.
func NewEndorserPayloadLocator(
	slot uint64,
	hash []byte,
	backing []byte,
) (PayloadLocator, error) {
	loc, err := NewBlockPayloadLocator(slot, hash, backing)
	if err != nil {
		return nil, err
	}
	loc.(*blockPayloadLocator).endorser = true
	return loc, nil
}

// LocatePayload returns the encoded CborOffset of the first occurrence of
// payload in the block.
func (l *blockPayloadLocator) LocatePayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("locate payload: empty payload")
	}
	idx := bytes.Index(l.block, payload)
	if idx < 0 {
		return nil, fmt.Errorf(
			"locate payload: %d bytes not found in block at slot %d",
			len(payload), l.slot,
		)
	}
	return l.encode(idx, len(payload)), nil
}

// LocatePayloadIn implements ContainedPayloadLocator. It returns the position
// of payload inside the first occurrence of container that holds it, so an
// inline datum or reference script is named by the bytes of its own output.
// Without such an occurrence it locates payload as LocatePayload does.
func (l *blockPayloadLocator) LocatePayloadIn(
	payload, container []byte,
) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("locate payload: empty payload")
	}
	if len(container) >= len(payload) {
		for from := 0; from < len(l.block); {
			at := bytes.Index(l.block[from:], container)
			if at < 0 {
				break
			}
			start := from + at
			if within := bytes.Index(container, payload); within >= 0 {
				return l.encode(start+within, len(payload)), nil
			}
			from = start + 1
		}
	}
	return l.LocatePayload(payload)
}

func (l *blockPayloadLocator) encode(idx, length int) []byte {
	offset := CborOffset{
		BlockSlot:  l.slot,
		BlockHash:  l.hash,
		ByteOffset: uint32(idx),    // #nosec G115 -- block length checked
		ByteLength: uint32(length), // #nosec G115 -- within the block
	}
	if l.endorser {
		return offset.EncodeEndorser()
	}
	return offset.Encode()
}
