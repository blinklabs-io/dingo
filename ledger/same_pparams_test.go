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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
)

func TestSameProtocolParameters(t *testing.T) {
	mk := func(maxTx uint) *babbage.BabbageProtocolParameters {
		return &babbage.BabbageProtocolParameters{
			MinFeeA:       44,
			MinFeeB:       155381,
			MaxTxSize:     maxTx,
			ProtocolMajor: 8,
		}
	}
	p := mk(16384)
	var nilBabbage *babbage.BabbageProtocolParameters
	var nilIface lcommon.ProtocolParameters
	tests := []struct {
		name string
		a, b lcommon.ProtocolParameters
		want bool
	}{
		{"same pointer", p, p, true},
		{"equal value, new pointer", p, mk(16384), true},
		{"changed value", p, mk(32768), false},
		{"both nil interface", nilIface, nilIface, true},
		{"one nil interface", p, nilIface, false},
		{"typed nil vs value", nilBabbage, p, false},
		{"both typed nil", nilBabbage, nilBabbage, true},
		{"different types", p, &shelley.ShelleyProtocolParameters{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sameProtocolParameters(tc.a, tc.b))
		})
	}
}
