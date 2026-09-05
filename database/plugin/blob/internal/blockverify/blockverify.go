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

// Package blockverify re-derives a block's identity from its bytes so a
// remote blob backend (S3, GCS) cannot silently hand back the wrong block
// for a requested (slot, hash) key. A cloud bucket has no content-addressing
// guarantee of its own -- object corruption, an eventual-consistency stale
// read, or a misdirected request could all return bytes for a different
// object than the one asked for -- so the returned bytes are decoded and
// re-hashed locally before any caller sees them.
package blockverify

import (
	"bytes"
	"errors"
	"fmt"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

// ErrUndecodable means the returned bytes could not be decoded as a block
// of the claimed type.
var ErrUndecodable = errors.New("block content could not be decoded")

// ErrHashMismatch means the returned bytes decode cleanly but hash to
// something other than what was requested.
var ErrHashMismatch = errors.New("block content hash mismatch")

// ErrSlotMismatch means the returned bytes decode and hash correctly but
// were produced for a different slot than requested.
var ErrSlotMismatch = errors.New("block content slot mismatch")

// ErrTypeMismatch means the returned bytes decode and hash correctly under
// the claimed type, but the type derived from the decoded header disagrees
// with it.
var ErrTypeMismatch = errors.New("block content era/type mismatch")

// Hash decodes cborData as a block of blockType and verifies that it hashes
// to wantHash and was produced at wantSlot, returning the decoded block on
// success. blockType is a decode hint, but is not trusted on its own: a
// wrong type usually either fails to decode (ErrUndecodable) or yields a
// different hash (ErrHashMismatch), but the hash alone does not pin the era
// for Shelley and later -- those hashes cover the header alone, and
// adjacent eras share its layout, so one set of bytes can decode under
// several eras with an identical hash and slot (see bark's
// verifyArchiveBlock/blockEraFromHeader, which this mirrors, for the same
// reasoning against an untrusted archive). So the era independently
// derived from the decoded header must also agree with blockType before
// the caller's claimed BlockMetadata.Type can be trusted.
func Hash(
	blockType uint,
	wantSlot uint64,
	cborData []byte,
	wantHash []byte,
) (gledger.Block, error) {
	decoded, err := gledger.NewBlockFromCbor(blockType, cborData)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: type %d: %w",
			ErrUndecodable,
			blockType,
			err,
		)
	}
	gotHash := decoded.Hash()
	if !bytes.Equal(gotHash[:], wantHash) {
		return nil, fmt.Errorf(
			"%w: got %x, requested %x",
			ErrHashMismatch,
			gotHash[:],
			wantHash,
		)
	}
	if decoded.SlotNumber() != wantSlot {
		return nil, fmt.Errorf(
			"%w: block %x is at slot %d, requested slot %d",
			ErrSlotMismatch,
			gotHash[:], decoded.SlotNumber(), wantSlot,
		)
	}
	if err := checkEra(decoded, blockType); err != nil {
		return nil, err
	}
	return decoded, nil
}

// checkEra independently derives blockType's decoded era from the block's
// own header and rejects a disagreement with the type blockType claims.
//
// Byron is exempt: its hash is taken over the block-type byte followed by
// the header, so its era is already bound by the hash check above and
// there is nothing further to derive.
func checkEra(decoded gledger.Block, blockType uint) error {
	if blockType == gledger.BlockTypeByronEbb ||
		blockType == gledger.BlockTypeByronMain {
		return nil
	}
	header := decoded.Header()
	if header == nil {
		return fmt.Errorf(
			"%w: block has no header to derive the era from",
			ErrTypeMismatch,
		)
	}
	derived, err := gledger.DetermineBlockType(header.Cbor())
	if err != nil {
		// Fail closed: an era that cannot be derived cannot be checked,
		// and falling back to the claimed type would hand era selection
		// back to whatever supplied blockType in the first place.
		return fmt.Errorf(
			"%w: deriving era from header: %w",
			ErrTypeMismatch, err,
		)
	}
	if derived != blockType {
		return fmt.Errorf(
			"%w: header is era %d, claimed %d",
			ErrTypeMismatch, derived, blockType,
		)
	}
	return nil
}
