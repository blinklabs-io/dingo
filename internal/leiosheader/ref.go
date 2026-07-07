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

package leiosheader

import (
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// legacyEndorserBlockReferencer is implemented by older gouroboros Dijkstra
// headers that exposed a direct accessor for the prototype's one-field
// [eb_hash, eb_size] Leios header extension.
type legacyEndorserBlockReferencer interface {
	LeiosEndorserBlockRef() (lcommon.Blake2b256, uint64, bool)
}

// ReferencedEndorserBlock returns the Leios endorser block named by a Dijkstra
// ranking-block header. It supports both the current prototype-2026w27
// [leios_certified, leios_announcement] extension and the earlier one-field
// [eb_hash, eb_size] extension.
func ReferencedEndorserBlock(
	header lcommon.BlockHeader,
) (lcommon.Blake2b256, uint64, bool) {
	if ref, ok := header.(legacyEndorserBlockReferencer); ok {
		if hash, size, ok := ref.LeiosEndorserBlockRef(); ok {
			return hash, size, true
		}
	}
	dijkstraHeader, ok := header.(*dijkstra.DijkstraBlockHeader)
	if !ok || dijkstraHeader == nil {
		return lcommon.Blake2b256{}, 0, false
	}
	if hash, size, ok := dijkstraHeader.LeiosAnnouncement(); ok {
		return hash, size, true
	}
	return legacyExtensionRef(dijkstraHeader.LeiosHeaderExtension)
}

func legacyExtensionRef(
	extension []cbor.RawMessage,
) (lcommon.Blake2b256, uint64, bool) {
	if len(extension) != 1 {
		return lcommon.Blake2b256{}, 0, false
	}
	var pair []cbor.RawMessage
	if _, err := cbor.Decode(extension[0], &pair); err != nil || len(pair) != 2 {
		return lcommon.Blake2b256{}, 0, false
	}
	var hashBytes []byte
	if _, err := cbor.Decode(pair[0], &hashBytes); err != nil ||
		len(hashBytes) != lcommon.Blake2b256Size {
		return lcommon.Blake2b256{}, 0, false
	}
	var size uint64
	if _, err := cbor.Decode(pair[1], &size); err != nil {
		return lcommon.Blake2b256{}, 0, false
	}
	return lcommon.NewBlake2b256(hashBytes), size, true
}
