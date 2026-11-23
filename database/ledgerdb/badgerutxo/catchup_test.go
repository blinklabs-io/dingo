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

// A catch-up import replaces the outputs and keeps the families, and marks
// the set as holding a partial import until the import ends it.
func TestBootstrapImportCatchUpKeepsTheFamiliesAndMarksAnImport(t *testing.T) {
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

	require.NoError(t, s.BootstrapImportCatchUp(point(20, 0x02),
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
	held, err = s.AccountInactivityActivationLoaded()
	require.NoError(t, err)
	require.False(
		t, held,
		"the kept accounts' activation is still to be read from the store",
	)
	_, ok, err := s.Account(0, []byte{0xaa})
	require.NoError(t, err)
	require.True(t, ok, "the account must survive")
	at, pending, err := s.ImportPending()
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, uint64(20), at.Slot)

	require.NoError(t, s.EndImport())
	_, pending, err = s.ImportPending()
	require.NoError(t, err)
	require.False(t, pending)
}

// A full bootstrap replaces every family, so an import a catch-up left
// pending is not carried into the rows it loads.
func TestFullBootstrapClearsAPendingImport(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01), out(0x11, 0))
	require.NoError(t, s.BootstrapImportCatchUp(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			return put(out(0x21, 0))
		},
	))
	_, pending, err := s.ImportPending()
	require.NoError(t, err)
	require.True(t, pending)

	bootstrap(t, s, point(30, 0x03), out(0x31, 0))

	_, pending, err = s.ImportPending()
	require.NoError(t, err)
	require.False(t, pending, "a full bootstrap left the import mark")
}

// An import writes the account rows itself, with their inactivity activation
// provenance, so a set it starts in reports that provenance as held.
func TestBootstrapImportMarksTheAccountActivationProvenanceLoaded(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.BootstrapImport(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			return put(out(0x21, 0))
		},
	))

	held, err := s.AccountsLoaded()
	require.NoError(t, err)
	require.True(t, held)
	held, err = s.AccountInactivityActivationLoaded()
	require.NoError(t, err)
	require.True(t, held)
}
