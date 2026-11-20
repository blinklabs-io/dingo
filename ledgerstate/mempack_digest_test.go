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

package ledgerstate

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// TestMempackOutputDigestsEqualBlockForm pins the UTxO digest's value
// canonicalisation to encodeMempackTxOut: an output as the snapshot import
// writes it must digest equal to the same output as a block holds it, with
// policies and names in arbitrary order, zero quantities and empty policies
// present, and an empty multiasset next to a bare coin.
func TestMempackOutputDigestsEqualBlockForm(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(6546, 1))
	addr := bytes.Repeat([]byte{0x61}, 29)
	digest := func(out []byte) metadata.UtxoSetDigest {
		d := metadata.NewUtxoDigester()
		d.Add([]byte{1}, 0, "1", []byte{2}, nil, nil, true, out)
		return d.Sum()
	}
	for range 500 {
		coin := rng.Uint64N(1 << uint(1+rng.IntN(63)))
		// block holds every entry, imported only the non-zero ones.
		type entry struct {
			name []byte
			qty  uint64
		}
		type policy struct {
			id      []byte
			entries []entry
		}
		var policies []policy
		var imported []ParsedAsset
		for range rng.IntN(5) {
			p := policy{id: make([]byte, 28)}
			for i := range p.id {
				p.id[i] = byte(rng.IntN(3))
			}
			seen := map[string]bool{string(p.id): false}
			dup := false
			for _, q := range policies {
				dup = dup || bytes.Equal(q.id, p.id)
			}
			if dup {
				continue
			}
			for range 1 + rng.IntN(4) {
				name := make([]byte, rng.IntN(5))
				for i := range name {
					name[i] = byte('a' + rng.IntN(3))
				}
				if seen[string(name)] {
					continue
				}
				seen[string(name)] = true
				qty := uint64(0)
				if rng.IntN(3) > 0 {
					qty = rng.Uint64N(1<<uint(1+rng.IntN(63))) + 1
				}
				p.entries = append(p.entries, entry{name, qty})
				if qty != 0 {
					imported = append(imported, ParsedAsset{
						PolicyId: p.id, Name: name, Amount: qty,
					})
				}
			}
			policies = append(policies, p)
		}
		ledger, err := encodeMempackTxOut(&decodedMempackTxOut{
			Address: addr, Lovelace: coin, Assets: imported,
		})
		require.NoError(t, err)

		block := appendCBORHead(nil, 5, 2)
		block = appendCBORHead(block, 0, 0)
		block = appendCBORHead(block, 2, uint64(len(addr)))
		block = append(block, addr...)
		block = appendCBORHead(block, 0, 1)
		if len(policies) == 0 && rng.IntN(2) == 0 {
			block = appendCBORHead(block, 0, coin)
		} else {
			block = appendCBORHead(block, 4, 2)
			block = appendCBORHead(block, 0, coin)
			block = appendCBORHead(block, 5, uint64(len(policies)))
			for _, p := range policies {
				block = appendCBORHead(block, 2, uint64(len(p.id)))
				block = append(block, p.id...)
				block = appendCBORHead(block, 5, uint64(len(p.entries)))
				for _, e := range p.entries {
					block = appendCBORHead(block, 2, uint64(len(e.name)))
					block = append(block, e.name...)
					block = appendCBORHead(block, 0, e.qty)
				}
			}
		}
		require.Equal(
			t, digest(ledger), digest(block),
			"ledger %x block %x", ledger, block,
		)
	}
}
