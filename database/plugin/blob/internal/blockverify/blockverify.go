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

// Hash decodes cborData as a block of blockType and verifies that it hashes
// to wantHash, returning the decoded block on success. blockType is a
// decode hint only: a wrong type either fails to decode (ErrUndecodable) or
// yields a different hash (ErrHashMismatch), so it cannot be used to smuggle
// in substitute bytes.
func Hash(
	blockType uint,
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
	return decoded, nil
}
