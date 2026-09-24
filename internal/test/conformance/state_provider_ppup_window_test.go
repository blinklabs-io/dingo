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

package conformance

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDingoStateProviderProtocolParameterUpdateWindow(t *testing.T) {
	t.Parallel()

	provider := NewDingoStateProvider(nil)
	epoch, noReturn, err := provider.ProtocolParameterUpdateWindow(
		2*conformanceSlotsPerEpoch + 123,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), epoch)
	require.Equal(
		t,
		3*conformanceSlotsPerEpoch-conformanceStabilityWindowSlots,
		noReturn,
	)

	epoch, nextNoReturn, err := provider.ProtocolParameterUpdateWindow(
		3 * conformanceSlotsPerEpoch,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), epoch)
	require.Equal(t, noReturn+conformanceSlotsPerEpoch, nextNoReturn)

	_, _, err = provider.ProtocolParameterUpdateWindow(math.MaxUint64)
	require.ErrorContains(t, err, "overflows")
}
