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

	"github.com/blinklabs-io/dingo/database/models"
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

// Hash decodes cborData as a block of blockType and verifies that it hashes
// to wantHash and was produced at wantSlot, returning the decoded block on
// success. blockType is a decode hint, but is not trusted on its own: a
// wrong type usually either fails to decode (ErrUndecodable) or yields a
// different hash (ErrHashMismatch).
//
// Decoding goes through models.Block.Decode rather than calling
// gledger.NewBlockFromCbor directly, so a Conway-tagged block carrying the
// Musashi/Leios prototype's extended header (see DecodeConwayBlock) is
// accepted the same way the rest of the storage stack already accepts it,
// instead of this check alone rejecting it as undecodable.
//
// Hash does not independently re-derive blockType from the decoded header.
// An earlier version of this check did, via gledger.DetermineBlockType, to
// catch the case bark's verifyArchiveBlock/blockEraFromHeader also guards
// against: for Shelley and later, the block hash covers only the header,
// and adjacent eras share that header's layout, so the same bytes can
// decode -- with an identical hash and slot -- under more than one era.
// That check was dropped because DetermineBlockType classifies era from
// the header's announced protocol-major version, which is a block
// producer's hard-fork-readiness signal, not a record of which era the
// bytes are actually encoded in: a producer starts announcing the next
// era's protocol major before that era's own hard fork has triggered, so a
// genuine, correctly-encoded block in the current era can carry a protocol
// major outside the range DetermineBlockType expects for it. That made the
// check reject real mainnet blocks at every hard-fork boundary -- observed
// concretely in this repository's own immutable-chain testdata, where
// genuine Alonzo blocks (Shelley-shaped headers) carry protocol major 7
// (Babbage's own floor) -- a functional regression on the primary
// production GetBlock path, worse than the narrow mislabeling gap it
// closed. The gap is accepted rather than worked around further: hash and
// slot together already prove the returned bytes are the genuine,
// uncorrupted content for the requested key; what an accepted era
// disagreement could still leave open is BlockMetadata.Type naming an
// adjacent era that happens to share the same header layout, which a
// caller that decodes strictly under the recorded Type (rather than
// re-deriving it) would not be misled by.
//
// Separately, and still accepted: for a Byron main block specifically,
// gouroboros checks the transaction, delegation, and update proofs but not
// ssc_proof, because the SSC proof hashes cardano-ledger's own encoding of
// the sub-payloads rather than the bytes carried in the block -- an
// upstream limitation, not something derivable here. An alteration
// confined to that one payload therefore changes nothing Hash checks.
// Bark's own archive-fetch path hits the identical gap and closes it by
// rejecting Byron main blocks entirely (assertBodyFullyAuthenticated in
// bark/blob.go), but bark treats a remote archive as an optional,
// distrusted fallback behind a trusted local store, so refusing one era
// there only costs the availability of a path that has a fallback. Hash
// instead guards the *primary* GetBlock path for S3/GCS: rejecting Byron
// main blocks here would make every Byron-era block permanently
// unretrievable from an S3/GCS-backed node (needed for a from-genesis
// sync, or serving historical API queries), a full functional regression
// traded for closing a gap that is narrow -- confined to one payload, in
// one era, on a store the operator already configured and trusted enough
// to write real chain data into in the first place.
func Hash(
	blockType uint,
	wantSlot uint64,
	cborData []byte,
	wantHash []byte,
) (gledger.Block, error) {
	decoded, err := models.Block{Type: blockType, Cbor: cborData}.Decode()
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
	return decoded, nil
}
