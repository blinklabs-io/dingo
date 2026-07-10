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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type envelopeTestHeader struct {
	cbor     []byte
	bodySize uint64
	slot     uint64
	number   uint64
	era      lcommon.Era
}

func (h *envelopeTestHeader) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (h *envelopeTestHeader) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (h *envelopeTestHeader) BlockNumber() uint64 { return h.number }
func (h *envelopeTestHeader) SlotNumber() uint64  { return h.slot }
func (h *envelopeTestHeader) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (h *envelopeTestHeader) BlockBodySize() uint64 { return h.bodySize }
func (h *envelopeTestHeader) Era() lcommon.Era      { return h.era }
func (h *envelopeTestHeader) Cbor() []byte          { return h.cbor }
func (h *envelopeTestHeader) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

type envelopeTestBlock struct {
	header *envelopeTestHeader
	cbor   []byte
}

func (b *envelopeTestBlock) Header() lcommon.BlockHeader { return b.header }
func (b *envelopeTestBlock) Hash() lcommon.Blake2b256    { return b.header.Hash() }
func (b *envelopeTestBlock) PrevHash() lcommon.Blake2b256 {
	return b.header.PrevHash()
}
func (b *envelopeTestBlock) BlockNumber() uint64 { return b.header.BlockNumber() }
func (b *envelopeTestBlock) SlotNumber() uint64  { return b.header.SlotNumber() }
func (b *envelopeTestBlock) IssuerVkey() lcommon.IssuerVkey {
	return b.header.IssuerVkey()
}
func (b *envelopeTestBlock) BlockBodySize() uint64 { return b.header.BlockBodySize() }
func (b *envelopeTestBlock) Era() lcommon.Era      { return b.header.Era() }
func (b *envelopeTestBlock) Cbor() []byte          { return b.cbor }
func (b *envelopeTestBlock) BlockBodyHash() lcommon.Blake2b256 {
	return b.header.BlockBodyHash()
}
func (b *envelopeTestBlock) Type() int { return 0 }
func (b *envelopeTestBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b *envelopeTestBlock) Utxorpc() (*utxorpc.Block, error) {
	return nil, nil
}

// TestValidateInboundBlockEnvelopeSizes covers header/body size limits and
// the requirement that the declared body size matches serialized block CBOR.
func TestValidateInboundBlockEnvelopeSizes(t *testing.T) {
	parent := envelopeParent{slot: 9, blockNumber: 41}
	pparams := &shelley.ShelleyProtocolParameters{
		MaxBlockBodySize:   3,
		MaxBlockHeaderSize: 4,
	}
	validHeader := mustEnvelopeCbor(t, "hdr")
	validBlockCbor := mustEnvelopeBlockCbor(t, validHeader,
		cbor.RawMessage{0x80},
		cbor.RawMessage{0x80},
		cbor.RawMessage{0xa0},
	)

	tests := []struct {
		name       string
		headerCbor []byte
		blockCbor  []byte
		bodySize   uint64
		pparams    *shelley.ShelleyProtocolParameters
		wantErr    string
	}{
		{
			name:       "valid exact limits",
			headerCbor: validHeader,
			blockCbor:  validBlockCbor,
			bodySize:   3,
			pparams:    pparams,
		},
		{
			name:       "header over limit",
			headerCbor: mustEnvelopeCbor(t, "long-header"),
			blockCbor:  validBlockCbor,
			bodySize:   3,
			pparams:    pparams,
			wantErr:    "exceeds maxBlockHeaderSize",
		},
		{
			name:       "declared body size mismatch",
			headerCbor: validHeader,
			blockCbor:  validBlockCbor,
			bodySize:   2,
			pparams:    pparams,
			wantErr:    "block body size mismatch",
		},
		{
			name:       "body over limit",
			headerCbor: validHeader,
			blockCbor:  validBlockCbor,
			bodySize:   3,
			pparams: &shelley.ShelleyProtocolParameters{
				MaxBlockBodySize:   2,
				MaxBlockHeaderSize: 4,
			},
			wantErr: "exceeds maxBlockBodySize",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := &envelopeTestBlock{
				header: &envelopeTestHeader{
					cbor:     tt.headerCbor,
					bodySize: tt.bodySize,
					slot:     10,
					number:   42,
					era:      shelley.EraShelley,
				},
				cbor: tt.blockCbor,
			}
			err := validateInboundBlockEnvelope(block, tt.pparams, parent)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestValidateInboundBlockEnvelopeOrdering checks normal block number and
// slot ordering failures, including Byron main blocks reusing parent numbers.
func TestValidateInboundBlockEnvelopeOrdering(t *testing.T) {
	parent := envelopeParent{slot: 10, blockNumber: 5}
	pparams := &shelley.ShelleyProtocolParameters{
		MaxBlockBodySize:   3,
		MaxBlockHeaderSize: 4,
	}
	headerCbor := mustEnvelopeCbor(t, "hdr")
	blockCbor := mustEnvelopeBlockCbor(t, headerCbor,
		cbor.RawMessage{0x80},
		cbor.RawMessage{0x80},
		cbor.RawMessage{0xa0},
	)

	tests := []struct {
		name    string
		slot    uint64
		number  uint64
		era     lcommon.Era
		wantErr string
	}{
		{
			name:    "normal block equal slot rejected",
			slot:    10,
			number:  6,
			era:     shelley.EraShelley,
			wantErr: "does not follow parent slot",
		},
		{
			name:    "normal block non-successor number rejected",
			slot:    11,
			number:  7,
			era:     shelley.EraShelley,
			wantErr: "does not follow parent block number",
		},
		{
			name:    "Byron main block cannot reuse parent number",
			slot:    11,
			number:  5,
			era:     byron.EraByron,
			wantErr: "does not follow parent block number",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := &envelopeTestBlock{
				header: &envelopeTestHeader{
					cbor:     headerCbor,
					bodySize: 3,
					slot:     tt.slot,
					number:   tt.number,
					era:      tt.era,
				},
				cbor: blockCbor,
			}
			err := validateInboundBlockEnvelope(block, pparams, parent)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestValidateBlockOrderAllowsOriginParent verifies the first block is not
// compared against a synthetic parent when the chain tip is origin.
func TestValidateBlockOrderAllowsOriginParent(t *testing.T) {
	block := &envelopeTestBlock{
		header: &envelopeTestHeader{
			slot:   0,
			number: 0,
			era:    shelley.EraShelley,
		},
	}

	require.NoError(t, validateBlockOrder(block, envelopeParent{origin: true}))
}

// TestValidateInboundBlockEnvelopeByronEbbOrdering covers the Byron EBB rule
// that an EBB shares its parent's block number instead of incrementing it.
func TestValidateInboundBlockEnvelopeByronEbbOrdering(t *testing.T) {
	parent := envelopeParent{
		slot:        byron.ByronSlotsPerEpoch,
		blockNumber: 7,
	}
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{},
	}
	ebb.BlockHeader.ConsensusData.Epoch = 1
	ebb.BlockHeader.ConsensusData.Difficulty.Value = parent.blockNumber
	require.NoError(t, validateInboundBlockEnvelope(ebb, nil, parent))

	ebb.BlockHeader.ConsensusData.Difficulty.Value = parent.blockNumber + 1
	err := validateInboundBlockEnvelope(ebb, nil, parent)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match parent block number")
}

// TestValidateByronEbbPlacementRejectsNilHeader ensures malformed Byron EBBs
// fail before placement or ordering logic reads header consensus data.
func TestValidateByronEbbPlacementRejectsNilHeader(t *testing.T) {
	err := validateByronEbbPlacement(&byron.ByronEpochBoundaryBlock{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil header")

	err = validateInboundBlockEnvelope(
		&byron.ByronEpochBoundaryBlock{},
		nil,
		envelopeParent{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil header")
}

func mustEnvelopeCbor(t *testing.T, value any) []byte {
	t.Helper()
	data, err := cbor.Encode(value)
	require.NoError(t, err)
	return data
}

func mustEnvelopeBlockCbor(
	t *testing.T,
	header []byte,
	bodyFields ...cbor.RawMessage,
) []byte {
	t.Helper()
	fields := make([]cbor.RawMessage, 0, 1+len(bodyFields))
	fields = append(fields, cbor.RawMessage(header))
	fields = append(fields, bodyFields...)
	data, err := cbor.Encode(fields)
	require.NoError(t, err)
	return data
}
