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
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// DecodeDijkstraBlock accepts current Dijkstra blocks and the earlier
// four-component block body retained in Musashi history. The legacy body is
// normalized only for typed decoding; its original bytes remain attached to
// the block and body for hashes, storage, and re-serving.
func DecodeDijkstraBlock(
	raw []byte,
	config ...common.VerifyConfig,
) (ledger.Block, error) {
	block, err := dijkstra.NewDijkstraBlockFromCbor(raw, config...)
	if err == nil {
		return block, nil
	}
	legacyBlock, legacyErr := decodeLegacyDijkstraBlock(raw, config...)
	if legacyErr == nil {
		return legacyBlock, nil
	}
	return nil, err
}

func decodeLegacyDijkstraBlock(
	raw []byte,
	config ...common.VerifyConfig,
) (*dijkstra.DijkstraBlock, error) {
	var components []cbor.RawMessage
	if _, err := cbor.Decode(raw, &components); err != nil {
		return nil, fmt.Errorf("decode legacy Dijkstra block: %w", err)
	}
	if len(components) != 2 {
		return nil, fmt.Errorf(
			"legacy Dijkstra block has %d components, expected 2",
			len(components),
		)
	}
	var body []cbor.RawMessage
	if _, err := cbor.Decode(components[1], &body); err != nil {
		return nil, fmt.Errorf("decode legacy Dijkstra block body: %w", err)
	}
	if len(body) != 4 {
		return nil, fmt.Errorf(
			"legacy Dijkstra block body has %d components, expected 4",
			len(body),
		)
	}

	invalidIndexes, err := decodeLegacyDijkstraInvalidIndexes(body[0])
	if err != nil {
		return nil, err
	}
	invalid := make(map[uint]struct{}, len(invalidIndexes))
	for _, index := range invalidIndexes {
		invalid[index] = struct{}{}
	}

	var transactions []cbor.RawMessage
	if _, err := cbor.Decode(body[1], &transactions); err != nil {
		return nil, fmt.Errorf("decode legacy Dijkstra transactions: %w", err)
	}
	for index := range invalid {
		if uint64(index) >= uint64(len(transactions)) {
			return nil, fmt.Errorf(
				"legacy Dijkstra invalid transaction index %d outside transaction list length %d",
				index,
				len(transactions),
			)
		}
	}
	blockTransactions := make([]cbor.RawMessage, len(transactions))
	for index, transaction := range transactions {
		var fields []cbor.RawMessage
		if _, err := cbor.Decode(transaction, &fields); err != nil {
			return nil, fmt.Errorf("decode legacy Dijkstra transaction %d: %w", index, err)
		}
		if len(fields) != 3 {
			return nil, fmt.Errorf(
				"legacy Dijkstra transaction %d has %d components, expected 3",
				index,
				len(fields),
			)
		}
		isValid := cbor.RawMessage{0xf5}
		if _, isInvalid := invalid[uint(index)]; isInvalid {
			isValid = cbor.RawMessage{0xf4}
		}
		encoded, err := cbor.Encode([]cbor.RawMessage{
			fields[0], fields[1], fields[2], isValid,
		})
		if err != nil {
			return nil, fmt.Errorf("encode legacy Dijkstra transaction %d: %w", index, err)
		}
		blockTransactions[index] = encoded
	}
	encodedTransactions, err := cbor.Encode(blockTransactions)
	if err != nil {
		return nil, fmt.Errorf("encode legacy Dijkstra transactions: %w", err)
	}
	encodedBody, err := cbor.Encode([]cbor.RawMessage{
		cbor.RawMessage(encodedTransactions), body[2], body[3],
	})
	if err != nil {
		return nil, fmt.Errorf("encode normalized Dijkstra block body: %w", err)
	}
	encodedBlock, err := cbor.Encode([]cbor.RawMessage{
		components[0], cbor.RawMessage(encodedBody),
	})
	if err != nil {
		return nil, fmt.Errorf("encode normalized Dijkstra block: %w", err)
	}

	verifyConfig := common.VerifyConfig{SkipBodyHashValidation: true}
	if len(config) > 0 {
		verifyConfig = config[0]
		verifyConfig.SkipBodyHashValidation = true
	}
	block, err := dijkstra.NewDijkstraBlockFromCbor(encodedBlock, verifyConfig)
	if err != nil {
		return nil, fmt.Errorf("decode normalized legacy Dijkstra block: %w", err)
	}
	block.BlockBody.SetCbor(components[1])
	block.SetCbor(raw)
	if len(config) == 0 || !config[0].SkipBodyHashValidation {
		actual := block.CalculatedBlockBodyHash()
		expected := block.BlockBodyHash()
		if actual != expected {
			return nil, common.NewValidationError(
				common.ValidationErrorTypeBodyHash,
				"Dijkstra block body hash mismatch during parsing",
				map[string]any{
					"era":           dijkstra.EraNameDijkstra,
					"expected_hash": expected.String(),
					"actual_hash":   actual.String(),
				},
				nil,
			)
		}
	}
	return block, nil
}

func decodeLegacyDijkstraInvalidIndexes(raw cbor.RawMessage) ([]uint, error) {
	if len(raw) == 1 && raw[0] == 0xf6 {
		return nil, nil
	}
	var indexes cbor.SetType[uint64]
	if _, err := cbor.Decode(raw, &indexes); err != nil {
		return nil, fmt.Errorf("decode legacy Dijkstra invalid transactions: %w", err)
	}
	if err := indexes.CheckForDuplicatesAlways(); err != nil {
		return nil, fmt.Errorf("decode legacy Dijkstra invalid transactions: %w", err)
	}
	items := indexes.Items()
	ret := make([]uint, len(items))
	for idx, item := range items {
		if uint64(uint(item)) != item {
			return nil, fmt.Errorf(
				"legacy Dijkstra invalid transaction index %d overflows uint",
				item,
			)
		}
		ret[idx] = uint(item)
	}
	return ret, nil
}
