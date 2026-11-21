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
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestConvertMultiAssetCanonicalOrder(t *testing.T) {
	t.Parallel()
	data := map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{}
	for i := range 12 {
		var policy lcommon.Blake2b224
		policy[0] = byte((i * 37) % 251)
		data[policy] = map[cbor.ByteString]*big.Int{}
		for _, name := range []string{"b", "a", "ab", "aa"} {
			data[policy][cbor.NewByteString([]byte(name))] = big.NewInt(1)
		}
	}
	ma := lcommon.NewMultiAsset(data)
	for range 10 {
		got := convertMultiAsset(&ma)
		require.Len(t, got, 48)
		require.True(t, slices.IsSortedFunc(got, func(a, b ParsedAsset) int {
			if c := bytes.Compare(a.PolicyId, b.PolicyId); c != 0 {
				return c
			}
			return bytes.Compare(a.Name, b.Name)
		}))
	}
}
