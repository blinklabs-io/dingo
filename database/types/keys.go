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

package types

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// BlockBlobKeySize is the on-disk length of a fully-formed block bp key:
// prefix (2) + slot (8) + hash (32) = 42 bytes. Plugins use this to
// decide whether a key is a base bp key (vs a metadata-suffixed one).
const BlockBlobKeySize = 2 + 8 + 32

// blockHashLen is the fixed hash length encoded into a block bp key.
const blockHashLen = 32

const (
	BlockBlobKeyPrefix         = "bp"
	BlockBlobIndexKeyPrefix    = "bi"
	BlockBlobMetadataKeySuffix = "_metadata"
	UtxoBlobKeyPrefix          = "u"
	TxBlobKeyPrefix            = "t"
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

// ParseBlockBlobKey is the inverse of BlockBlobKey: it pulls (slot,
// hash) back out of a fully-formed bp key. Used by blob plugins that
// need to attach the (slot, hash) of a tombstoned block to a typed
// error surfaced from iteration. Returns an error for any key whose
// shape doesn't match what BlockBlobKey produced.
func ParseBlockBlobKey(key []byte) (uint64, []byte, error) {
	if len(key) != BlockBlobKeySize {
		return 0, nil, fmt.Errorf(
			"block blob key length %d, expected %d",
			len(key), BlockBlobKeySize,
		)
	}
	if string(key[:len(BlockBlobKeyPrefix)]) != BlockBlobKeyPrefix {
		return 0, nil, fmt.Errorf(
			"block blob key has wrong prefix: %q", key[:len(BlockBlobKeyPrefix)],
		)
	}
	slot := binary.BigEndian.Uint64(
		key[len(BlockBlobKeyPrefix) : len(BlockBlobKeyPrefix)+8],
	)
	hash := make([]byte, blockHashLen)
	copy(hash, key[len(BlockBlobKeyPrefix)+8:])
	return slot, hash, nil
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

// BlockHashIndexKeyPrefix is the key prefix for the hash→block-key index.
// Key format: "bh" + hash(32 bytes), value: BlockBlobKey (slot+hash).
const BlockHashIndexKeyPrefix = "bh"

// BlockHashIndexKey builds the key for a hash→slot lookup index entry.
func BlockHashIndexKey(hash []byte) []byte {
	key := make([]byte, 0, len(BlockHashIndexKeyPrefix)+len(hash))
	key = append(key, BlockHashIndexKeyPrefix...)
	key = append(key, hash...)
	return key
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

// TxBlobKey creates a key for storing transaction offset data.
// Key format: 't' + txHash (32 bytes)
func TxBlobKey(txHash []byte) []byte {
	key := []byte(TxBlobKeyPrefix)
	key = append(key, txHash...)
	return key
}

const (
	// LeiosEBManifestKeyPrefix is the key prefix for storing raw Leios
	// endorser-block manifest CBOR (the bytes received over leios-fetch
	// MsgBlock). Key format: "em" + hash(32 bytes). The stored value is
	// slot(8 bytes big-endian) + manifest CBOR, so the slot can be recovered
	// on load without a separate index.
	LeiosEBManifestKeyPrefix = "em"
	// LeiosEBTxsKeyPrefix is the key prefix for storing the raw endorser-block
	// transaction bodies (the CBOR-in-CBOR wrapped tx list from leios-fetch
	// MsgBlockTxs). Key format: "et" + hash(32 bytes). The value is a
	// CBOR-encoded []cbor.RawMessage (each element is a CBOR byte string
	// wrapping a full transaction CBOR, matching the leios-fetch wire format).
	// Only written when the transaction cache is complete (all txCount txs
	// fetched), so a missing "et" key means the txs are not available.
	LeiosEBTxsKeyPrefix = "et"
)

// LeiosEBManifestKey builds the blob key for a Leios endorser-block manifest.
func LeiosEBManifestKey(hash []byte) []byte {
	key := make([]byte, 0, len(LeiosEBManifestKeyPrefix)+len(hash))
	key = append(key, LeiosEBManifestKeyPrefix...)
	key = append(key, hash...)
	return key
}

// LeiosEBTxsKey builds the blob key for the raw transaction bodies of a Leios
// endorser block.
func LeiosEBTxsKey(hash []byte) []byte {
	key := make([]byte, 0, len(LeiosEBTxsKeyPrefix)+len(hash))
	key = append(key, LeiosEBTxsKeyPrefix...)
	key = append(key, hash...)
	return key
}
