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

package models

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// babbageHeaderBodyFieldCount is the number of fields in a standard Babbage
// (and Conway) block header body: block_number, slot, prev_hash, issuer_vkey,
// vrf_vkey, vrf_result, block_body_size, block_body_hash, operational_cert, and
// protocol_version.
const babbageHeaderBodyFieldCount = 10

// conwayBlockComponentCount is the number of top-level components in a Conway
// block: [header, transaction_bodies, transaction_witness_sets,
// transaction_metadata, invalid_transactions].
const conwayBlockComponentCount = 5

// DecodeConwayBlock decodes a Conway block, transparently accepting the
// Musashi/Leios prototype header extension. The respun Musashi network
// (network magic 164) tags its early chain as Conway (NtN block type 7) but its
// block headers carry a 12-field header body — the 10 standard Babbage fields
// plus leios_certified and leios_announcement — which gouroboros' strict Conway
// decoder rejects. Standard Conway blocks decode via gouroboros unchanged; only
// a strict-decode failure triggers the Leios-extended reconstruct, so real
// Conway networks (mainnet/preprod/preview) pay no cost and are unaffected.
func DecodeConwayBlock(raw []byte) (ledger.Block, error) {
	block, err := ledger.NewBlockFromCbor(ledger.BlockTypeConway, raw)
	if err == nil {
		return block, nil
	}
	extBlock, extErr := decodeLeiosExtendedConwayBlock(raw)
	if extErr != nil {
		// Not a recognizable Leios-extended Conway block either; surface the
		// strict-decode error, which is the meaningful one for real networks.
		return nil, err
	}
	return extBlock, nil
}

// decodeLeiosExtendedConwayBlock reconstructs a Conway block whose header body
// carries the Musashi/Leios extension. The extra trailing header fields are
// dropped only to satisfy gouroboros' strict Conway decoder; the original wire
// bytes are restored on the decoded block so that:
//   - block.Hash() hashes the real 12-field header (chain linkage / prev-hash),
//   - block.Cbor() returns the verbatim block (correct storage and re-serving),
//   - the header body retains its original bytes for KES verification.
//
// Field 7 (block_body_hash) lies within the preserved first 10 fields, so
// gouroboros' body-hash validation still holds against the untouched body
// components.
func decodeLeiosExtendedConwayBlock(raw []byte) (*conway.ConwayBlock, error) {
	var components []cbor.RawMessage
	if _, err := cbor.Decode(raw, &components); err != nil {
		return nil, fmt.Errorf("decode Conway block components: %w", err)
	}
	if len(components) != conwayBlockComponentCount {
		return nil, fmt.Errorf(
			"unexpected Conway block: expected %d components, got %d",
			conwayBlockComponentCount,
			len(components),
		)
	}
	headerRaw := components[0]
	// The header is [header_body, signature]; the Leios extension lives in
	// header_body after the 10 standard Babbage fields.
	var headerParts []cbor.RawMessage
	if _, err := cbor.Decode(headerRaw, &headerParts); err != nil {
		return nil, fmt.Errorf("decode Conway block header: %w", err)
	}
	if len(headerParts) != 2 {
		return nil, fmt.Errorf(
			"unexpected Conway block header: expected 2 elements, got %d",
			len(headerParts),
		)
	}
	var bodyElems []cbor.RawMessage
	if _, err := cbor.Decode(headerParts[0], &bodyElems); err != nil {
		return nil, fmt.Errorf("decode Conway block header body: %w", err)
	}
	if len(bodyElems) < babbageHeaderBodyFieldCount {
		return nil, fmt.Errorf(
			"unexpected Conway block header body: expected at least %d fields, got %d",
			babbageHeaderBodyFieldCount,
			len(bodyElems),
		)
	}
	// Re-encode the header with its body truncated to the 10 standard Babbage
	// fields so the strict Conway decoder accepts it.
	truncatedBody, err := cbor.Encode(bodyElems[:babbageHeaderBodyFieldCount])
	if err != nil {
		return nil, fmt.Errorf("re-encode truncated header body: %w", err)
	}
	truncatedHeader, err := cbor.Encode([]any{
		cbor.RawMessage(truncatedBody),
		headerParts[1],
	})
	if err != nil {
		return nil, fmt.Errorf("re-encode truncated header: %w", err)
	}
	reconstructed, err := cbor.Encode([]any{
		cbor.RawMessage(truncatedHeader),
		components[1],
		components[2],
		components[3],
		components[4],
	})
	if err != nil {
		return nil, fmt.Errorf("re-encode reconstructed Conway block: %w", err)
	}
	block, err := conway.NewConwayBlockFromCbor(reconstructed)
	if err != nil {
		return nil, fmt.Errorf("decode reconstructed Conway block: %w", err)
	}
	if block.BlockHeader == nil {
		return nil, errors.New("reconstructed Conway block has no header")
	}
	// Restore the original wire bytes. Hash() derives from the header's stored
	// CBOR, so this makes block.Hash() equal the real 12-field header hash that
	// chain-sync computed for the same block.
	block.BlockHeader.SetCbor([]byte(headerRaw))
	block.BlockHeader.Body.SetCbor([]byte(headerParts[0]))
	block.SetCbor(raw)
	return block, nil
}
