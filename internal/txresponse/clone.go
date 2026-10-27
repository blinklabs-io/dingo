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

// Package txresponse owns transaction payloads copied into serving responses.
package txresponse

import (
	"math"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// Clone copies transaction bytes into one response-owned buffer. Each item
// has independent capacity, so append cannot overwrite its neighbor. Retaining
// an item retains this response buffer, not the source transaction closure.
func Clone(in []cbor.RawMessage) []cbor.RawMessage {
	if in == nil {
		return nil
	}
	total := 0
	for _, tx := range in {
		if len(tx) > math.MaxInt-total {
			panic("transaction response exceeds addressable memory")
		}
		total += len(tx)
	}
	out := make([]cbor.RawMessage, len(in))
	packed := make([]byte, total)
	offset := 0
	for i, tx := range in {
		if tx == nil {
			continue
		}
		end := offset + copy(packed[offset:], tx)
		out[i] = packed[offset:end:end]
		offset = end
	}
	return out
}
