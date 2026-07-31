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

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerkleRootIsDeterministic(t *testing.T) {
	t.Parallel()
	leaves := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	assert.Equal(t, MerkleRoot(leaves), MerkleRoot(leaves))
}

func TestMerkleRootDependsOnOrder(t *testing.T) {
	t.Parallel()
	assert.NotEqual(
		t,
		MerkleRoot([][]byte{[]byte("a"), []byte("b")}),
		MerkleRoot([][]byte{[]byte("b"), []byte("a")}),
	)
}

func TestMerkleRootDetectsAnyLeafChange(t *testing.T) {
	t.Parallel()
	base := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	want := MerkleRoot(base)
	for i := range base {
		changed := make([][]byte, len(base))
		copy(changed, base)
		changed[i] = append([]byte{}, base[i]...)
		changed[i][0] ^= 0xff
		assert.NotEqual(
			t,
			want,
			MerkleRoot(changed),
			"changing leaf %d must change the root",
			i,
		)
	}
}

func TestMerkleRootSingleLeafIsNotTheRawLeafHash(t *testing.T) {
	t.Parallel()
	// A leaf is domain-separated, so a single-leaf root must differ from a
	// bare hash of the same bytes; otherwise a leaf preimage could be
	// presented as a tree.
	raw := sha256.Sum256([]byte("only"))
	assert.NotEqual(t, raw[:], MerkleRoot([][]byte{[]byte("only")}))
}

func TestMerkleRootEmptyIsDefinedAndDistinct(t *testing.T) {
	t.Parallel()
	empty := MerkleRoot(nil)
	assert.Len(t, empty, sha256.Size)
	assert.NotEqual(t, empty, MerkleRoot([][]byte{{}}))
}

func TestMerkleRootOddLeafCountsAreDistinct(t *testing.T) {
	t.Parallel()
	// Promoting rather than duplicating the odd trailing node means a tree
	// with a repeated tail does not collide with the shorter tree.
	three := MerkleRoot([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	four := MerkleRoot(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("c")},
	)
	assert.NotEqual(t, three, four)
}

func TestMerkleRootSizes(t *testing.T) {
	t.Parallel()
	for n := 1; n <= 9; n++ {
		t.Run(fmt.Sprintf("leaves=%d", n), func(t *testing.T) {
			t.Parallel()
			leaves := make([][]byte, n)
			for i := range leaves {
				leaves[i] = fmt.Appendf(nil, "leaf-%d", i)
			}
			assert.Len(t, MerkleRoot(leaves), sha256.Size)
		})
	}
}
