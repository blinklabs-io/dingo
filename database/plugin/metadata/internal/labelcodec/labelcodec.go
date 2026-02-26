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

package labelcodec

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type Entry struct {
	Label     uint64
	CborValue []byte
	JsonValue string
}

// Returns full metadata CBOR and per-label entries
func EncodeAndExtract(
	txMetadata lcommon.TransactionMetadatum,
) ([]byte, []Entry, error) {
	if txMetadata == nil {
		return nil, nil, nil
	}
	metadataCbor, err := metadatumCbor(txMetadata)
	if err != nil {
		return nil, nil, fmt.Errorf("encode metadata: %w", err)
	}
	labels, err := extractFromCbor(metadataCbor)
	if err != nil {
		labels, err = extractFromMetadatum(txMetadata)
		if err != nil {
			return nil, nil, err
		}
	}
	return metadataCbor, labels, nil
}

func extractFromCbor(
	metadataCbor []byte,
) ([]Entry, error) {
	if len(metadataCbor) == 0 {
		return nil, nil
	}
	rawByLabel, err := decodeMetadataLabelMap(metadataCbor)
	if err != nil {
		return nil, fmt.Errorf("decode metadata label map: %w", err)
	}
	labels := make([]uint64, 0, len(rawByLabel))
	for label := range rawByLabel {
		labels = append(labels, label)
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i] < labels[j] })

	ret := make([]Entry, 0, len(labels))
	for _, label := range labels {
		rawValue := rawByLabel[label]
		jsonValue, err := metadatumRawToJSON(rawValue)
		if err != nil {
			return nil, fmt.Errorf("decode metadata label %d JSON: %w", label, err)
		}
		ret = append(ret, Entry{
			Label:     label,
			CborValue: append([]byte(nil), rawValue...),
			JsonValue: jsonValue,
		})
	}
	return ret, nil
}

// Decode CBOR map keys as (uint64/uint/int/int64)
func decodeMetadataLabelMap(
	metadataCbor []byte,
) (map[uint64]cbor.RawMessage, error) {
	var asUint64 map[uint64]cbor.RawMessage
	if _, err := cbor.Decode(metadataCbor, &asUint64); err == nil {
		return asUint64, nil
	}
	var asUint map[uint]cbor.RawMessage
	if _, err := cbor.Decode(metadataCbor, &asUint); err == nil {
		ret := make(map[uint64]cbor.RawMessage, len(asUint))
		for k, v := range asUint {
			ret[uint64(k)] = v
		}
		return ret, nil
	}
	var asInt map[int]cbor.RawMessage
	if _, err := cbor.Decode(metadataCbor, &asInt); err == nil {
		ret := make(map[uint64]cbor.RawMessage, len(asInt))
		for k, v := range asInt {
			if k < 0 {
				return nil, fmt.Errorf("negative metadata label: %d", k)
			}
			ret[uint64(k)] = v
		}
		return ret, nil
	}
	var asInt64 map[int64]cbor.RawMessage
	if _, err := cbor.Decode(metadataCbor, &asInt64); err == nil {
		ret := make(map[uint64]cbor.RawMessage, len(asInt64))
		for k, v := range asInt64 {
			if k < 0 {
				return nil, fmt.Errorf("negative metadata label: %d", k)
			}
			ret[uint64(k)] = v
		}
		return ret, nil
	}
	return nil, fmt.Errorf("metadata is not an integer-keyed map")
}

func metadatumRawToJSON(raw cbor.RawMessage) (string, error) {
	decoded, err := lcommon.DecodeMetadatumRaw(raw)
	if err != nil {
		return "", err
	}
	tmpValue, err := metadatumToJSONValue(decoded)
	if err != nil {
		return "", err
	}
	jsonBytes, err := json.Marshal(tmpValue)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func metadatumCbor(
	txMetadata lcommon.TransactionMetadatum,
) ([]byte, error) {
	if cborProvider, ok := txMetadata.(interface{ Cbor() []byte }); ok {
		if raw := cborProvider.Cbor(); len(raw) > 0 {
			return append([]byte(nil), raw...), nil
		}
	}
	return cbor.Encode(txMetadata)
}

func extractFromMetadatum(
	txMetadata lcommon.TransactionMetadatum,
) ([]Entry, error) {
	tmpMap, ok := txMetadata.(lcommon.MetaMap)
	if !ok {
		return nil, fmt.Errorf("metadata is not an integer-keyed map")
	}
	ret := make([]Entry, 0, len(tmpMap.Pairs))
	for _, pair := range tmpMap.Pairs {
		keyInt, ok := pair.Key.(lcommon.MetaInt)
		if !ok || keyInt.Value == nil {
			continue
		}
		if keyInt.Value.Sign() < 0 || !keyInt.Value.IsUint64() {
			return nil, fmt.Errorf("invalid metadata label: %s", keyInt.Value.String())
		}
		cborValue, err := metadatumCbor(pair.Value)
		if err != nil {
			return nil, fmt.Errorf("encode metadata label %s value: %w", keyInt.Value.String(), err)
		}
		jsonValueAny, err := metadatumToJSONValue(pair.Value)
		if err != nil {
			return nil, fmt.Errorf("decode metadata label %s JSON: %w", keyInt.Value.String(), err)
		}
		jsonBytes, err := json.Marshal(jsonValueAny)
		if err != nil {
			return nil, fmt.Errorf("encode metadata label %s JSON: %w", keyInt.Value.String(), err)
		}
		ret = append(ret, Entry{
			Label:     keyInt.Value.Uint64(),
			CborValue: cborValue,
			JsonValue: string(jsonBytes),
		})
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Label < ret[j].Label })
	return ret, nil
}

// Convert to JSON safe (int/text/bytes/list/map)
func metadatumToJSONValue(md lcommon.TransactionMetadatum) (any, error) {
	switch m := md.(type) {
	case lcommon.MetaInt:
		if m.Value == nil {
			return "0", nil
		}
		if m.Value.IsInt64() {
			return m.Value.Int64(), nil
		}
		return m.Value.String(), nil
	case lcommon.MetaText:
		return m.Value, nil
	case lcommon.MetaBytes:
		return hex.EncodeToString(m.Value), nil
	case lcommon.MetaList:
		ret := make([]any, 0, len(m.Items))
		for _, item := range m.Items {
			tmp, err := metadatumToJSONValue(item)
			if err != nil {
				return nil, err
			}
			ret = append(ret, tmp)
		}
		return ret, nil
	case lcommon.MetaMap:
		ret := make(map[string]any, len(m.Pairs))
		for _, pair := range m.Pairs {
			key, err := metadatumMapKeyToString(pair.Key)
			if err != nil {
				return nil, err
			}
			tmp, err := metadatumToJSONValue(pair.Value)
			if err != nil {
				return nil, err
			}
			ret[key] = tmp
		}
		return ret, nil
	default:
		return nil, fmt.Errorf("unsupported metadatum type: %T", md)
	}
}

func metadatumMapKeyToString(md lcommon.TransactionMetadatum) (string, error) {
	switch m := md.(type) {
	case lcommon.MetaInt:
		if m.Value == nil {
			return "0", nil
		}
		return m.Value.String(), nil
	case lcommon.MetaText:
		return m.Value, nil
	case lcommon.MetaBytes:
		return hex.EncodeToString(m.Value), nil
	default:
		tmp, err := metadatumToJSONValue(md)
		if err != nil {
			return "", err
		}
		jsonBytes, err := json.Marshal(tmp)
		if err != nil {
			return "", err
		}
		return string(jsonBytes), nil
	}
}
