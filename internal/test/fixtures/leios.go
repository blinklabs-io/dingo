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

package fixtures

import (
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// CertifiedEndorserBlock is a Dijkstra ranking block that announces an
// endorser block, the ranking block that certifies it, and the endorser
// block's manifest and transactions.
type CertifiedEndorserBlock struct {
	Announcer *dijkstra.DijkstraBlock
	Certifier *dijkstra.DijkstraBlock
	// EndorserHash is the hash the announcer carries, the hash of Manifest.
	EndorserHash common.Blake2b256
	// Manifest is the CBOR of the endorser block manifest. Its transaction
	// reference hashes the full block_transaction CBOR.
	Manifest []byte
	// Transactions is the CBOR of each block_transaction of the endorser
	// block, in manifest order.
	Transactions []cbor.RawMessage
}

// GenerateCertifiedEndorserBlock returns a Dijkstra announcing block, a
// certifying child block, and the endorser block with one transaction that
// the certifying block represents.
func GenerateCertifiedEndorserBlock(
	announcerSlot uint64,
) (*CertifiedEndorserBlock, error) {
	tx := &dijkstra.DijkstraTransaction{
		TxIsValid: true,
		Body:      dijkstra.DijkstraTransactionBody{TxFee: 200_000},
	}
	txParts, err := tx.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("encode endorser transaction: %w", err)
	}
	// An endorser transaction travels as a block_transaction: the encoded
	// [body, witnesses, auxiliary_data] gains the trailing is_valid flag.
	var parts []cbor.RawMessage
	if _, err := cbor.Decode(txParts, &parts); err != nil {
		return nil, fmt.Errorf("decode endorser transaction: %w", err)
	}
	txCbor, err := cbor.Encode(append(parts, cbor.RawMessage{0xf5}))
	if err != nil {
		return nil, fmt.Errorf("encode endorser block transaction: %w", err)
	}
	manifest := &common.LeiosEndorserBlock{
		TransactionReferences: []common.LeiosTransactionReference{{
			TransactionHash: common.Blake2b256Hash(txCbor),
			TransactionSize: uint32(len(txCbor)), // #nosec G115
		}},
	}
	manifestCbor, err := manifest.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("encode endorser manifest: %w", err)
	}
	ebHash := manifest.Hash()
	announcement, err := cbor.Encode([]any{ebHash.Bytes(), uint64(4096)})
	if err != nil {
		return nil, err
	}
	announcer, err := leiosRankingBlock(
		1, announcerSlot, common.Blake2b256{}, false, announcement,
	)
	if err != nil {
		return nil, err
	}
	certifier, err := leiosRankingBlock(
		2, announcerSlot+1, announcer.Hash(), true, []byte{0xf6},
	)
	if err != nil {
		return nil, err
	}
	return &CertifiedEndorserBlock{
		Announcer:    announcer,
		Certifier:    certifier,
		EndorserHash: ebHash,
		Manifest:     manifestCbor,
		Transactions: []cbor.RawMessage{txCbor},
	}, nil
}

func leiosRankingBlock(
	number, slot uint64,
	prevHash common.Blake2b256,
	certified bool,
	announcement []byte,
) (*dijkstra.DijkstraBlock, error) {
	certifiedFlag, err := cbor.Encode(certified)
	if err != nil {
		return nil, err
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: number,
					Slot:        slot,
					PrevHash:    prevHash,
					VrfKey:      make([]byte, 32),
					VrfResult: common.VrfResult{
						Output: make([]byte, 64),
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
				Signature: make([]byte, 448),
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				certifiedFlag,
				announcement,
			},
		},
	}
	if certified {
		block.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
			Signers:             []byte{0x80},
			AggregatedSignature: make([]byte, 48),
		}
	}
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	// Decode the encoded block so its header carries the CBOR its hash is
	// computed from.
	return dijkstra.NewDijkstraBlockFromCbor(blockCbor)
}
