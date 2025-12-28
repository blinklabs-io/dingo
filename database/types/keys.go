// Copyright 2025 Blink Labs Software
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
	"encoding/binary"
	"slices"
)

const (
	BlockBlobKeyPrefix         = "bp"
	BlockBlobIndexKeyPrefix    = "bi"
	BlockBlobMetadataKeySuffix = "_metadata"
	UtxoBlobKeyPrefix          = "u"
)

func BlockBlobKeyUint64ToBytes(input uint64) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, input)
	return ret
}

func BlockBlobKey(slot uint64, hash []byte) []byte {
	key := []byte(BlockBlobKeyPrefix)
	// Convert slot to bytes
	slotBytes := BlockBlobKeyUint64ToBytes(slot)
	key = append(key, slotBytes...)
	key = append(key, hash...)
	return key
}

func BlockBlobIndexKey(blockNumber uint64) []byte {
	key := []byte(BlockBlobIndexKeyPrefix)
	// Convert block number to bytes
	blockNumberBytes := BlockBlobKeyUint64ToBytes(blockNumber)
	key = append(key, blockNumberBytes...)
	return key
}

func BlockBlobMetadataKey(baseKey []byte) []byte {
	return slices.Concat(baseKey, []byte(BlockBlobMetadataKeySuffix))
}

func UtxoBlobKey(txId []byte, outputIdx uint32) []byte {
	key := []byte(UtxoBlobKeyPrefix)
	key = append(key, txId...)
	// Convert index to bytes
	idxBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idxBytes, outputIdx)
	key = append(key, idxBytes...)
	return key
}
