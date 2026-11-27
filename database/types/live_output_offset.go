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
	"errors"
)

// LiveOutputOffsetMagic is the prefix of an offset into a live output's own
// CBOR rather than into a block.
var LiveOutputOffsetMagic = [4]byte{'D', 'L', 'O', 'F'} // "DLOF" = Dingo Live Output oFfset

// LiveOutputOffset names bytes by their position inside the CBOR of an output
// that is live in the ledger's UTxO set. It stands in for a block offset when
// the block expired before the offset could be computed and the output's
// position in that block is lost: the payload lies inside the output, and the
// output is active, so the ledger's live set still holds the bytes.
//
// Layout (52 bytes, the size of a CborOffset):
//   - bytes 0-3:   Magic "DLOF"
//   - bytes 4-35:  transaction hash
//   - bytes 36-39: output index (big-endian uint32)
//   - bytes 40-43: byte offset within the output's CBOR (big-endian uint32)
//   - bytes 44-47: byte length (big-endian uint32)
//   - bytes 48-51: zero
type LiveOutputOffset struct {
	TxHash      [32]byte
	OutputIndex uint32
	ByteOffset  uint32
	ByteLength  uint32
}

// Encode serializes the offset.
func (o *LiveOutputOffset) Encode() []byte {
	ret := make([]byte, CborOffsetSize)
	copy(ret[0:4], LiveOutputOffsetMagic[:])
	copy(ret[4:36], o.TxHash[:])
	binary.BigEndian.PutUint32(ret[36:40], o.OutputIndex)
	binary.BigEndian.PutUint32(ret[40:44], o.ByteOffset)
	binary.BigEndian.PutUint32(ret[44:48], o.ByteLength)
	return ret
}

// IsLiveOutputOffset reports whether data is an encoded live-output offset.
func IsLiveOutputOffset(data []byte) bool {
	return len(data) == CborOffsetSize &&
		[4]byte(data[0:4]) == LiveOutputOffsetMagic
}

// DecodeLiveOutputOffset deserializes a live-output offset.
func DecodeLiveOutputOffset(data []byte) (*LiveOutputOffset, error) {
	if !IsLiveOutputOffset(data) {
		return nil, errors.New("invalid live output offset: expected DLOF")
	}
	ret := &LiveOutputOffset{
		OutputIndex: binary.BigEndian.Uint32(data[36:40]),
		ByteOffset:  binary.BigEndian.Uint32(data[40:44]),
		ByteLength:  binary.BigEndian.Uint32(data[44:48]),
	}
	copy(ret.TxHash[:], data[4:36])
	return ret, nil
}
