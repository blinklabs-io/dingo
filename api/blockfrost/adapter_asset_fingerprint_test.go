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

package blockfrost

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// cip14Vectors are the published CIP-14 test vectors. The expected strings
// come from the specification, not from the code that computes them.
var cip14Vectors = []struct {
	policy      string
	name        string
	fingerprint string
}{
	{
		"7eae28af2208be856f7a119668ae52a49b73725e326dc16579dcc373",
		"", "asset1rjklcrnsdzqp65wjgrg55sy9723kw09mlgvlc3",
	},
	{
		"7eae28af2208be856f7a119668ae52a49b73725e326dc16579dcc373",
		"504154415445", "asset13n25uv0yaf5kus35fm2k86cqy60z58d9xmde92",
	},
	{
		"1e349c9bdea19fd6c147626a5260bc44b71635f398b67c59881df209",
		"504154415445", "asset1hv4p5tv2a837mzqrst04d0dcptdjmluqvdx9k3",
	},
	{
		"1e349c9bdea19fd6c147626a5260bc44b71635f398b67c59881df209",
		"", "asset1uyuxku60yqe57nusqzjx38aan3f2wq6s93f6ea",
	},
}

// TestNodeAdapterAssetFingerprintIsCIP14 requires the asset detail to carry
// the CIP-14 fingerprint of a held asset, including the empty name.
func TestNodeAdapterAssetFingerprintIsCIP14(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xc3, 0)
	for i, vector := range cip14Vectors {
		policy, err := hex.DecodeString(vector.policy)
		require.NoError(t, err)
		name, err := hex.DecodeString(vector.name)
		require.NoError(t, err)
		seedAddressOutput(t, raw, db, addr, fill32(byte(0x40+i)), 5,
			1_000_000,
			[]models.Asset{{PolicyId: policy, Name: name, Amount: 3}}, nil)

		info, err := adapter.Asset(context.Background(), vector.policy, name)
		require.NoError(t, err)
		require.Equal(t, vector.fingerprint, info.Fingerprint)
		require.Equal(t, "3", info.Quantity)
	}
}
