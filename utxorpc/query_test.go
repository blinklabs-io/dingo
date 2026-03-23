// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"math"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
	utxorpcCardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

func TestCaip2FromNetworkMagic_KnownNetworks(t *testing.T) {
	mainnet, ok := ouroboros.NetworkByName("mainnet")
	require.True(t, ok, "expected mainnet network to exist")
	preprod, ok := ouroboros.NetworkByName("preprod")
	require.True(t, ok, "expected preprod network to exist")
	preview, ok := ouroboros.NetworkByName("preview")
	require.True(t, ok, "expected preview network to exist")

	tests := []struct {
		name   string
		magic  uint32
		caip2  string
	}{
		{
			name:  "mainnet",
			magic: mainnet.NetworkMagic,
			caip2: "cardano:mainnet",
		},
		{
			name:  "preprod",
			magic: preprod.NetworkMagic,
			caip2: "cardano:preprod",
		},
		{
			name:  "preview",
			magic: preview.NetworkMagic,
			caip2: "cardano:preview",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := caip2FromNetworkMagic(tc.magic)
			require.Equal(t, tc.caip2, got)
		})
	}
}

func TestCaip2FromNetworkMagic_UnknownNetwork(t *testing.T) {
	const devnetMagic uint32 = 42
	got := caip2FromNetworkMagic(devnetMagic)
	require.Equal(t, "cardano:devnet", got)
}

func TestCaip2FromNetworkMagic_CustomNetwork(t *testing.T) {
	const customMagic uint32 = math.MaxUint32
	got := caip2FromNetworkMagic(customMagic)
	require.Equal(t, "cardano:4294967295", got)
}

func TestExtractSearchPredicatePatterns_NilPredicate(t *testing.T) {
	addressPattern, assetPattern := extractSearchPredicatePatterns(nil)
	require.Nil(t, addressPattern)
	require.Nil(t, assetPattern)
}

func TestExtractSearchPredicatePatterns_WithCardanoMatch(t *testing.T) {
	address := &utxorpcCardano.AddressPattern{
		ExactAddress: []byte{0x01, 0x02},
	}
	asset := &utxorpcCardano.AssetPattern{
		PolicyId:  []byte{0xaa},
		AssetName: []byte{0xbb},
	}
	predicate := &query.UtxoPredicate{
		Match: &query.AnyUtxoPattern{
			UtxoPattern: &query.AnyUtxoPattern_Cardano{
				Cardano: &utxorpcCardano.TxOutputPattern{
					Address: address,
					Asset:   asset,
				},
			},
		},
	}

	addressPattern, assetPattern := extractSearchPredicatePatterns(predicate)
	require.Equal(t, address, addressPattern)
	require.Equal(t, asset, assetPattern)
}

