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

	"github.com/stretchr/testify/assert"
)

// TestSignedSumText pins the rendering of the account reserves_sum and
// treasury_sum fields. Both aggregate delta_coin rows, so a net negative total
// has to reach the response with its sign rather than as its magnitude, and an
// account with no MIR history has to render as "0" rather than an empty string.
func TestSignedSumText(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value *big.Int
		want  string
	}{
		{name: "nil renders as zero", value: nil, want: "0"},
		{name: "zero", value: big.NewInt(0), want: "0"},
		{name: "positive", value: big.NewInt(1_200), want: "1200"},
		{name: "negative keeps its sign", value: big.NewInt(-200), want: "-200"},
		{
			name:  "beyond int64",
			value: new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 70)),
			want:  "-1180591620717411303424",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, signedSumText(test.value))
		})
	}
}
