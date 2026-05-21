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

package chainselection_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestComparePraosTipsSlotBattleWithoutSelectViewDoesNotInventHashTieBreak(
	t *testing.T,
) {
	dingoTip := ochainsync.Tip{
		BlockNumber: 11,
		Point: ocommon.Point{
			Slot: 40,
			Hash: []byte{
				0x5c, 0x25, 0x9f, 0x44, 0x42, 0xe1, 0x49, 0x33,
				0x72, 0xe2, 0x07, 0x83, 0xaa, 0xb3, 0x06, 0x39,
				0x13, 0x75, 0x44, 0x42, 0xe5, 0x98, 0xa0, 0x54,
				0xad, 0xe8, 0xb6, 0x26, 0xef, 0xe7, 0x4c, 0xe1,
			},
		},
	}
	cardanoTip := ochainsync.Tip{
		BlockNumber: 11,
		Point: ocommon.Point{
			Slot: 40,
			Hash: []byte{
				0x0c, 0x54, 0x11, 0x82, 0xc9, 0xa9, 0xa6, 0x6d,
				0xa2, 0xf7, 0xc3, 0x1a, 0x1f, 0x9b, 0x9e, 0x4a,
				0xeb, 0xf0, 0xfe, 0x0f, 0xed, 0x41, 0xe8, 0x04,
				0xb7, 0x21, 0xac, 0xf5, 0x78, 0x3a, 0x64, 0x03,
			},
		},
	}

	got := chainselection.ComparePraosTips(
		dingoTip,
		cardanoTip,
		chainselection.PraosTiebreakerViewFromTip(
			dingoTip,
			nil,
			chainselection.PraosTiebreakerConfigConway(),
		),
		chainselection.PraosTiebreakerViewFromTip(
			cardanoTip,
			nil,
			chainselection.PraosTiebreakerConfigConway(),
		),
	)
	require.Equal(t, chainselection.ChainEqual, got)
}
