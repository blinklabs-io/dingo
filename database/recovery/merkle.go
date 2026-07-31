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

package recovery

import "crypto/sha256"

// Domain separation prefixes, following RFC 6962. Hashing leaves and interior
// nodes under different prefixes stops an attacker, or a coincidence, from
// presenting an interior node's preimage as a leaf and vice versa.
const (
	merkleLeafPrefix     byte = 0x00
	merkleInteriorPrefix byte = 0x01
)

// MerkleRoot computes a binary merkle root over leaves.
//
// The tree is built bottom-up, promoting an odd trailing node unchanged to the
// next level. Order is significant: callers must present leaves in a fixed
// canonical order so the same state always yields the same root.
//
// An empty leaf set hashes nothing at all, which gives the empty tree a defined
// root that no leaf can produce: every leaf hash covers at least the leaf
// prefix byte, so none of them is the hash of the empty string.
func MerkleRoot(leaves [][]byte) []byte {
	if len(leaves) == 0 {
		sum := sha256.Sum256(nil)
		return sum[:]
	}
	level := make([][]byte, 0, len(leaves))
	for _, leaf := range leaves {
		h := sha256.New()
		h.Write([]byte{merkleLeafPrefix})
		h.Write(leaf)
		level = append(level, h.Sum(nil))
	}
	for len(level) > 1 {
		next := make([][]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				// Odd node at this level: promote it rather than
				// duplicating it. Duplication makes an even tree
				// and an odd tree with a repeated tail collide.
				next = append(next, level[i])
				continue
			}
			h := sha256.New()
			h.Write([]byte{merkleInteriorPrefix})
			h.Write(level[i])
			h.Write(level[i+1])
			next = append(next, h.Sum(nil))
		}
		level = next
	}
	return level[0]
}
