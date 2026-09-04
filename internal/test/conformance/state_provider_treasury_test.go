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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStateProviderTreasuryValueReportsBackendState proves the conformance
// provider reports the treasury the backend holds, in the same shape
// production's ledger.LedgerView.TreasuryValue reports. It previously
// returned a hardcoded zero regardless of backend state.
func TestStateProviderTreasuryValueReportsBackendState(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	const treasury = uint64(87_920_693_660_807)
	require.NoError(
		t,
		m.db.Metadata().SetNetworkState(treasury, 1_000, 42, nil),
	)

	got, err := m.GetStateProvider().TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, treasury, got)
}

// TestStateProviderTreasuryValueMissingFailsClosed proves a backend with no
// network-state row is reported as unavailable rather than as a treasury of
// zero. The upstream current-treasury-value rule compares for equality once a
// transaction body carries key 21, so a synthetic zero would silently reject
// every vector declaring a non-zero treasury and silently accept one
// declaring zero.
func TestStateProviderTreasuryValueMissingFailsClosed(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	got, err := m.GetStateProvider().TreasuryValue()
	require.ErrorContains(t, err, "treasury network state is unavailable")
	require.Zero(t, got)
}

// TestStateProviderTreasuryValueTracksLatestSlot proves the provider reads
// the newest network-state row rather than the first one written, so a
// vector that advances the treasury is validated against the value the
// backend currently holds.
func TestStateProviderTreasuryValueTracksLatestSlot(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	require.NoError(t, m.db.Metadata().SetNetworkState(100, 900, 10, nil))
	require.NoError(t, m.db.Metadata().SetNetworkState(250, 750, 20, nil))

	got, err := m.GetStateProvider().TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, uint64(250), got)
}
