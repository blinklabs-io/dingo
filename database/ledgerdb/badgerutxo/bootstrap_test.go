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

// A bootstrap that stops after its new contents are written, before the
// final transaction records the tip, must not pair them with the old tip or
// format marker: the store is stale, and after a restart it still has no
// tip, which the live UTxO set rebuilds from.
func TestBootstrapStoppedAfterFlushLeavesNoTip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := open(t, dir)
	bootstrap(t, s, point(10, 0x01), out(0x11, 0), out(0x12, 0))

	crash := errors.New("crash after flush")
	s.SetAfterBootstrapFlushForTest(func() error { return crash })
	err := s.Bootstrap(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			return put(out(0x21, 0))
		},
	)
	require.ErrorIs(t, err, crash)
	requireLive(t, s, true, out(0x21, 0).Ref)
	stale, err := s.Stale()
	require.NoError(t, err)
	require.True(t, stale, "new contents kept the old format marker")
	require.NoError(t, s.Close())

	s = open(t, dir)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, ok, err := s.Tip()
	require.NoError(t, err)
	require.False(t, ok, "new contents kept the old tip across a restart")
}

// A bootstrap replaces the whole set, so the genesis key delegations and MIR
// certificates the old contents held, and the marker that says they are
// complete, must go with them.
func TestBootstrapClearsGenesisDelegationsAndMIRCertificates(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01), out(0x11, 0))
	require.NoError(t, s.LoadGenesisMIR(func(put func(k, v []byte) error) error {
		if err := put([]byte("G0000000000000000"), []byte{1}); err != nil {
			return err
		}
		return put([]byte("M0000000000000000"), []byte{1})
	}))
	loaded, err := s.GenesisMIRLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	_, ok, err := s.Row([]byte("G0000000000000000"))
	require.NoError(t, err)
	require.True(t, ok)

	bootstrap(t, s, point(20, 0x02), out(0x21, 0))

	loaded, err = s.GenesisMIRLoaded()
	require.NoError(t, err)
	require.False(t, loaded, "the marker survived the bootstrap")
	for _, k := range []string{"G0000000000000000", "M0000000000000000"} {
		_, ok, err = s.Row([]byte(k))
		require.NoError(t, err)
		require.False(t, ok, "row %q survived the bootstrap", k)
	}
}

// A set refuses a key that is neither a genesis delegation nor a MIR
// certificate, and is not marked loaded by the failed load.
func TestLoadGenesisMIRRejectsAForeignKey(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	err := s.LoadGenesisMIR(func(put func(k, v []byte) error) error {
		return put([]byte("y0000000000000000"), []byte{1})
	})
	require.Error(t, err)
	loaded, err := s.GenesisMIRLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
}
