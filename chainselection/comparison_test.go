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

package chainselection

import (
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
)

func TestComparePraosTipsLongerChainWins(t *testing.T) {
	shorter := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("shorter")},
		BlockNumber: 40,
	}
	longer := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("longer")},
		BlockNumber: 41,
	}

	assert.Equal(
		t,
		ChainBBetter,
		ComparePraosTips(shorter, longer, PraosTiebreakerView{}, PraosTiebreakerView{}),
	)
	assert.Equal(
		t,
		ChainABetter,
		ComparePraosTips(longer, shorter, PraosTiebreakerView{}, PraosTiebreakerView{}),
	)
}

func TestComparePraosTipsEqualLengthDoesNotUseSlotOrHashFallback(t *testing.T) {
	lowerSlotLowerHash := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 90, Hash: []byte("a")},
		BlockNumber: 50,
	}
	higherSlotHigherHash := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("z")},
		BlockNumber: 50,
	}

	assert.Equal(
		t,
		ChainEqual,
		ComparePraosTips(
			lowerSlotLowerHash,
			higherSlotHigherHash,
			PraosTiebreakerView{},
			PraosTiebreakerView{},
		),
	)
}
