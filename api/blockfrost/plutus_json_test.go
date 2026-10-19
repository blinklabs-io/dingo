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
	"math/big"
	"testing"

	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

func TestPlutusDataDetailedJSON(t *testing.T) {
	t.Parallel()
	datum := data.NewConstr(0,
		data.NewInteger(bigInt(42)),
		data.NewByteString([]byte("ab")),
		data.NewList(data.NewInteger(bigInt(-1))),
		data.NewMap([][2]data.PlutusData{{
			data.NewByteString([]byte("k")),
			data.NewConstr(1),
		}}),
	)
	raw, err := data.Encode(datum)
	require.NoError(t, err)

	got, err := plutusDataDetailedJSON(raw)
	require.NoError(t, err)
	require.JSONEq(t, `{"constructor":0,"fields":[
{"int":42},
{"bytes":"6162"},
{"list":[{"int":-1}]},
{"map":[{"k":{"bytes":"6b"},"v":{"constructor":1,"fields":[]}}]}]}`, string(got))

	_, err = plutusDataDetailedJSON([]byte{0xff})
	require.Error(t, err)
}

func bigInt(v int64) *big.Int { return big.NewInt(v) }
