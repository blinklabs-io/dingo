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

// A catch-up bootstrap replaces the outputs and keeps the families, and marks
// a merge pending until the merge ends.
func TestBootstrapCatchUpKeepsTheFamiliesAndMarksAMerge(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01), out(0x11, 0))
	require.NoError(t, s.LoadAccounts(
		func(put func(badgerutxo.AccountRow) error) error {
			return put(badgerutxo.AccountRow{
				Tag: 0, Key: []byte{0xaa}, Active: true, AddedSlot: 5,
			})
		},
	))

	require.NoError(t, s.BootstrapCatchUp(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			return put(out(0x21, 0))
		},
	))

	_, live, err := s.Get(out(0x11, 0).Ref)
	require.NoError(t, err)
	require.False(t, live, "the old output must be replaced")
	_, live, err = s.Get(out(0x21, 0).Ref)
	require.NoError(t, err)
	require.True(t, live)
	held, err := s.AccountsLoaded()
	require.NoError(t, err)
	require.True(t, held, "the accounts marker must survive")
	_, ok, err := s.Account(0, []byte{0xaa})
	require.NoError(t, err)
	require.True(t, ok, "the account must survive")
	pending, err := s.MergePending()
	require.NoError(t, err)
	require.True(t, pending)

	require.NoError(t, s.BeginMerge())
	require.NoError(t, s.LoadAccounts(
		func(put func(badgerutxo.AccountRow) error) error {
			return put(badgerutxo.AccountRow{
				Tag: 0, Key: []byte{0xbb}, Active: true, AddedSlot: 20,
			})
		},
	))
	require.NoError(t, s.EndMerge(true))
	for _, key := range [][]byte{{0xaa}, {0xbb}} {
		_, ok, err := s.Account(0, key)
		require.NoError(t, err)
		require.True(t, ok, "a merge drops no account")
	}
	pending, err = s.MergePending()
	require.NoError(t, err)
	require.False(t, pending)
	r, ok, err := s.AccountAt(0, []byte{0xbb}, 10)
	require.NoError(t, err)
	require.False(t, ok, "as of the earlier slot the merged account did not exist: %v", r)
}

// A full bootstrap replaces every family, so a merge a catch-up left pending
// is not carried into the rows it loads: the next family load would otherwise
// merge them over rows that no longer exist.
func TestFullBootstrapClearsAPendingMerge(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01), out(0x11, 0))
	require.NoError(t, s.BootstrapCatchUp(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			return put(out(0x21, 0))
		},
	))
	pending, err := s.MergePending()
	require.NoError(t, err)
	require.True(t, pending)

	bootstrap(t, s, point(30, 0x03), out(0x31, 0))

	pending, err = s.MergePending()
	require.NoError(t, err)
	require.False(t, pending, "a full bootstrap left the merge mark")
}
