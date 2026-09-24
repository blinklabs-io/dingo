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
	"time"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

func TestLedgerViewProtocolParameterUpdateWindow(t *testing.T) {
	t.Parallel()

	const slotsPerEpoch = 432_000
	provider := newMockSlotTimeProvider(time.Unix(0, 0), time.Second, slotsPerEpoch)
	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		},
		slotClock: NewSlotClock(provider, DefaultSlotClockConfig()),
	}
	lv := &LedgerView{ls: ls}

	epoch, noReturn, err := lv.ProtocolParameterUpdateWindow(2*slotsPerEpoch + 123)
	require.NoError(t, err)
	require.Equal(t, uint64(2), epoch)
	require.Equal(t, uint64(3*slotsPerEpoch-25_920), noReturn)

	epoch, nextNoReturn, err := lv.ProtocolParameterUpdateWindow(3 * slotsPerEpoch)
	require.NoError(t, err)
	require.Equal(t, uint64(3), epoch)
	require.Equal(t, noReturn+slotsPerEpoch, nextNoReturn)
}

func TestLedgerViewProtocolParameterUpdateWindowNeedsKnownState(t *testing.T) {
	t.Parallel()

	_, _, err := (&LedgerView{}).ProtocolParameterUpdateWindow(10)
	require.ErrorContains(t, err, "window unavailable")

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		slotClock: NewSlotClock(
			newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 432_000),
			DefaultSlotClockConfig(),
		),
	}
	_, _, err = (&LedgerView{ls: ls}).ProtocolParameterUpdateWindow(10)
	require.ErrorContains(t, err, "stability window")
}
