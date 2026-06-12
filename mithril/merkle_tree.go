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

package mithril

import (
	"errors"

	"golang.org/x/crypto/blake2s"
)

// blake2sPair hashes the concatenation of two MMR nodes with
// Blake2s-256, matching Mithril's merkle tree hasher.
func blake2sPair(left, right []byte) []byte {
	buf := make([]byte, 0, len(left)+len(right))
	buf = append(buf, left...)
	buf = append(buf, right...)
	sum := blake2s.Sum256(buf)
	return sum[:]
}

// computeMMRRoot computes the root of a Mithril merkle tree over the
// given leaves. Mithril's tree is a Merkle Mountain Range (the ckb
// MMR construction) using Blake2s-256: leaves are inserted as-is
// (not pre-hashed), equal-height subtrees merge as
// blake2s256(older || newer), and the final root "bags" the peaks
// right-to-left with reversed merge order:
// acc = blake2s256(acc || nextPeakToTheLeft).
func computeMMRRoot(leaves [][]byte) ([]byte, error) {
	if len(leaves) == 0 {
		return nil, errors.New(
			"cannot compute merkle root over empty leaf set",
		)
	}
	type peak struct {
		height int
		hash   []byte
	}
	peaks := make([]peak, 0, 64)
	for _, leaf := range leaves {
		node := peak{height: 0, hash: leaf}
		for len(peaks) > 0 && peaks[len(peaks)-1].height == node.height {
			prev := peaks[len(peaks)-1]
			peaks = peaks[:len(peaks)-1]
			node = peak{
				height: prev.height + 1,
				hash:   blake2sPair(prev.hash, node.hash),
			}
		}
		peaks = append(peaks, node)
	}
	acc := peaks[len(peaks)-1].hash
	for i := len(peaks) - 2; i >= 0; i-- {
		acc = blake2sPair(acc, peaks[i].hash)
	}
	return acc, nil
}
