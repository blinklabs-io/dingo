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

package txpump

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPumpReleasePendingWaitsForAvailableSlot(t *testing.T) {
	wallet := NewWallet()
	pump := NewPump(
		&Config{ConfirmationSlots: 3},
		wallet,
		nil,
		nil,
		time.Time{},
	)
	pump.deferUTxO(UTxO{TxHash: sampleHash, Index: 1, Amount: 2_000_000}, 10)

	pump.releasePending(9)
	require.Equal(t, 0, wallet.Len())
	require.Len(t, pump.pending, 1)

	pump.releasePending(10)
	require.Equal(t, 1, wallet.Len())
	require.Empty(t, pump.pending)
}

func TestPumpConfirmationSlotsDefaultsConservatively(t *testing.T) {
	pump := NewPump(&Config{}, NewWallet(), nil, nil, time.Time{})
	require.Equal(t, uint64(30), pump.confirmationSlots())
}
