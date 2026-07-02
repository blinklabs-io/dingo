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
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

const (
	vrfKeyBodyIndex          = 4
	praosVrfResultBodyIndex  = 5
	tpraosLeaderVrfBodyIndex = 6
	vrfResultFieldCount      = 2
)

// normalizeHeaderVrfFieldsFromBodyCbor returns a shallow header copy whose
// typed VRF key and result are derived from the original header-body CBOR.
// Chainsync and block decoders keep those original bytes for KES verification;
// using the same source for VRF avoids rejecting canonical headers when a
// decoded VRF field is stale or otherwise inconsistent with the wire bytes.
func normalizeHeaderVrfFieldsFromBodyCbor(
	header ledger.BlockHeader,
) (ledger.BlockHeader, error) {
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(header)
	if err != nil || !ok {
		return header, err
	}
	vrfResult, ok, err := headerVrfResultFromBodyCbor(header)
	if err != nil || !ok {
		return header, err
	}
	return headerWithVrfKeyAndResult(header, vrfKey, vrfResult), nil
}

func headerVrfKeyFromBodyCbor(
	header ledger.BlockHeader,
) ([]byte, bool, error) {
	bodyCbor, ok := headerBodyCbor(header)
	if !ok || len(bodyCbor) == 0 {
		return nil, false, nil
	}
	vrfKey, err := decodeBytesFromHeaderBodyCbor(bodyCbor, vrfKeyBodyIndex)
	if err != nil {
		return nil, false, fmt.Errorf("decode VRF key: %w", err)
	}
	return vrfKey, true, nil
}

func headerVrfResultFromBodyCbor(
	header ledger.BlockHeader,
) (lcommon.VrfResult, bool, error) {
	var (
		bodyCbor []byte
		index    int
	)
	switch h := header.(type) {
	case *shelley.ShelleyBlockHeader:
		bodyCbor = h.Body.Cbor()
		index = tpraosLeaderVrfBodyIndex
	case *allegra.AllegraBlockHeader:
		bodyCbor = h.Body.Cbor()
		index = tpraosLeaderVrfBodyIndex
	case *mary.MaryBlockHeader:
		bodyCbor = h.Body.Cbor()
		index = tpraosLeaderVrfBodyIndex
	case *alonzo.AlonzoBlockHeader:
		bodyCbor = h.Body.Cbor()
		index = tpraosLeaderVrfBodyIndex
	case *babbage.BabbageBlockHeader:
		bodyCbor = h.Body.Cbor()
		index = praosVrfResultBodyIndex
	case *conway.ConwayBlockHeader:
		bodyCbor = h.Body.Cbor()
		index = praosVrfResultBodyIndex
	case *dijkstra.DijkstraBlockHeader:
		bodyCbor = h.Body.Cbor()
		index = praosVrfResultBodyIndex
	default:
		return lcommon.VrfResult{}, false, nil
	}
	if len(bodyCbor) == 0 {
		return lcommon.VrfResult{}, false, nil
	}
	vrfResult, err := decodeVrfResultFromHeaderBodyCbor(bodyCbor, index)
	if err != nil {
		return lcommon.VrfResult{}, false, err
	}
	return vrfResult, true, nil
}

func headerBodyCbor(header ledger.BlockHeader) ([]byte, bool) {
	switch h := header.(type) {
	case *shelley.ShelleyBlockHeader:
		return h.Body.Cbor(), true
	case *allegra.AllegraBlockHeader:
		return h.Body.Cbor(), true
	case *mary.MaryBlockHeader:
		return h.Body.Cbor(), true
	case *alonzo.AlonzoBlockHeader:
		return h.Body.Cbor(), true
	case *babbage.BabbageBlockHeader:
		return h.Body.Cbor(), true
	case *conway.ConwayBlockHeader:
		return h.Body.Cbor(), true
	case *dijkstra.DijkstraBlockHeader:
		return h.Body.Cbor(), true
	default:
		return nil, false
	}
}

func decodeBytesFromHeaderBodyCbor(
	bodyCbor []byte,
	index int,
) ([]byte, error) {
	decoder, err := cbor.NewStreamDecoder(bodyCbor)
	if err != nil {
		return nil, err
	}
	fieldCount, _, _, err := decoder.DecodeArrayHeader()
	if err != nil {
		return nil, err
	}
	if index >= fieldCount {
		return nil, fmt.Errorf(
			"header body has %d fields, cannot read bytes at index %d",
			fieldCount,
			index,
		)
	}
	if _, _, err := decoder.SkipN(index); err != nil {
		return nil, fmt.Errorf(
			"skip header body fields before bytes: %w",
			err,
		)
	}
	var ret []byte
	if _, _, err := decoder.Decode(&ret); err != nil {
		return nil, err
	}
	return cloneBytes(ret), nil
}

func decodeVrfResultFromHeaderBodyCbor(
	bodyCbor []byte,
	index int,
) (lcommon.VrfResult, error) {
	decoder, err := cbor.NewStreamDecoder(bodyCbor)
	if err != nil {
		return lcommon.VrfResult{}, err
	}
	fieldCount, _, _, err := decoder.DecodeArrayHeader()
	if err != nil {
		return lcommon.VrfResult{}, err
	}
	if index >= fieldCount {
		return lcommon.VrfResult{}, fmt.Errorf(
			"header body has %d fields, cannot read VRF result at index %d",
			fieldCount,
			index,
		)
	}
	if _, _, err := decoder.SkipN(index); err != nil {
		return lcommon.VrfResult{}, fmt.Errorf(
			"skip header body fields before VRF result: %w",
			err,
		)
	}
	var fields []cbor.RawMessage
	if _, _, err := decoder.Decode(&fields); err != nil {
		return lcommon.VrfResult{}, fmt.Errorf(
			"decode raw VRF result: %w",
			err,
		)
	}
	if len(fields) != vrfResultFieldCount {
		return lcommon.VrfResult{}, fmt.Errorf(
			"VRF result has %d fields, expected %d",
			len(fields),
			vrfResultFieldCount,
		)
	}
	var output []byte
	if _, err := cbor.Decode(fields[0], &output); err != nil {
		return lcommon.VrfResult{}, fmt.Errorf(
			"decode VRF output bytes: %w",
			err,
		)
	}
	var proof []byte
	if _, err := cbor.Decode(fields[1], &proof); err != nil {
		return lcommon.VrfResult{}, fmt.Errorf(
			"decode VRF proof bytes: %w",
			err,
		)
	}
	return lcommon.VrfResult{
		Output: cloneBytes(output),
		Proof:  cloneBytes(proof),
	}, nil
}

func headerWithVrfKeyAndResult(
	header ledger.BlockHeader,
	vrfKey []byte,
	vrfResult lcommon.VrfResult,
) ledger.BlockHeader {
	switch h := header.(type) {
	case *shelley.ShelleyBlockHeader:
		clone := *h
		clone.Body.VrfKey = cloneBytes(vrfKey)
		clone.Body.LeaderVrf = cloneVrfResult(vrfResult)
		return &clone
	case *allegra.AllegraBlockHeader:
		clone := *h
		clone.Body.VrfKey = cloneBytes(vrfKey)
		clone.Body.LeaderVrf = cloneVrfResult(vrfResult)
		return &clone
	case *mary.MaryBlockHeader:
		clone := *h
		clone.Body.VrfKey = cloneBytes(vrfKey)
		clone.Body.LeaderVrf = cloneVrfResult(vrfResult)
		return &clone
	case *alonzo.AlonzoBlockHeader:
		clone := *h
		clone.Body.VrfKey = cloneBytes(vrfKey)
		clone.Body.LeaderVrf = cloneVrfResult(vrfResult)
		return &clone
	case *babbage.BabbageBlockHeader:
		clone := *h
		clone.Body.VrfKey = cloneBytes(vrfKey)
		clone.Body.VrfResult = cloneVrfResult(vrfResult)
		return &clone
	case *conway.ConwayBlockHeader:
		clone := *h
		clone.Body.VrfKey = cloneBytes(vrfKey)
		clone.Body.VrfResult = cloneVrfResult(vrfResult)
		return &clone
	case *dijkstra.DijkstraBlockHeader:
		clone := *h
		clone.Body.VrfKey = cloneBytes(vrfKey)
		clone.Body.VrfResult = cloneVrfResult(vrfResult)
		return &clone
	default:
		return header
	}
}

func cloneVrfResult(vrfResult lcommon.VrfResult) lcommon.VrfResult {
	return lcommon.VrfResult{
		Output: cloneBytes(vrfResult.Output),
		Proof:  cloneBytes(vrfResult.Proof),
	}
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
