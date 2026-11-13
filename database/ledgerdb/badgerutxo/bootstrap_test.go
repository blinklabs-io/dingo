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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// A bootstrap that fails after it has started replacing the contents must
// not leave them looking current: the store has no tip and reports itself
// stale, so the caller rebuilds it instead of trusting a partial set.
func TestBootstrapFailureLeavesTheStoreStale(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01), out(0x11, 0), out(0x12, 0))
	stale, err := s.Stale()
	require.NoError(t, err)
	require.False(t, stale)

	feedErr := errors.New("feed failed")
	err = s.Bootstrap(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			if err := put(out(0x21, 0)); err != nil {
				return err
			}
			return feedErr
		},
	)
	require.ErrorIs(t, err, feedErr)

	stale, err = s.Stale()
	require.NoError(t, err)
	require.True(t, stale, "a failed bootstrap left the format marker")
	_, ok, err := s.Tip()
	require.NoError(t, err)
	require.False(t, ok, "a failed bootstrap left the old tip")
}
