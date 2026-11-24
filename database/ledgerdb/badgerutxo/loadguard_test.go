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

package badgerutxo_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// Once the set serves the ledger alone, a reload over held rows is refused
// and changes nothing: no metadata store holds the rows a load would read.
func TestRefuseLoadsRefusesALoadOverHeldRows(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loadHistory := func() error {
		return s.LoadPoolHistory(func(put func(k, v []byte) error) error {
			return put(
				[]byte{badgerutxo.PoolHistoryTag, 1, 0x01},
				[]byte{0x80},
			)
		})
	}
	require.NoError(t, loadHistory())
	require.NoError(t, s.UseLegacyPoolHistoryMarkerForTest())
	require.NoError(t, loadHistory(), "loads are accepted until refused")
	loaded, err := s.PoolHistoryLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	require.NoError(t, s.UseLegacyPoolHistoryMarkerForTest())
	s.RefuseLoads()
	require.ErrorIs(t, loadHistory(), badgerutxo.ErrLoadsRefused)
	loaded, err = s.PoolHistoryLoaded()
	require.NoError(t, err)
	require.False(t, loaded, "a refused load must change nothing")
}
