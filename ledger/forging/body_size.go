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

package forging

import "github.com/blinklabs-io/gouroboros/cbor"

type segmentedBodySize struct {
	payloadSize   uint64
	txCount       int
	metadataCount int
}

func (size segmentedBodySize) withTransaction(
	body, witnesses, metadata cbor.RawMessage,
) segmentedBodySize {
	size.payloadSize += uint64(len(body)) + uint64(len(witnesses))
	if metadata != nil {
		size.payloadSize += uint64(cbor.ArrayHeaderSize(size.txCount)) +
			uint64(len(metadata))
		size.metadataCount++
	}
	size.txCount++
	return size
}

func (size segmentedBodySize) size(era eraKind) uint64 {
	total := size.payloadSize + 2*uint64(cbor.ArrayHeaderSize(size.txCount)) +
		uint64(cbor.ArrayHeaderSize(size.metadataCount))
	if era.hasInvalidTxs() {
		total++
		if era.usesIndefInvalidList() {
			total++
		}
	}
	return total
}
