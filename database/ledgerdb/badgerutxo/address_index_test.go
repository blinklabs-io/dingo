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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// byFirstByte files an output under its first CBOR byte, which out sets to
// the byte its ref starts with.
func byFirstByte(cbor []byte) [][]byte {
	if len(cbor) == 0 {
		return nil
	}
	return [][]byte{{'t', cbor[0]}}
}

func openIndexed(
	t *testing.T,
	dir string,
	extra ...badgerutxo.Option,
) (*badgerutxo.Store, error) {
	t.Helper()
	opts := append([]badgerutxo.Option{badgerutxo.WithIndexer(byFirstByte)}, extra...)
	return badgerutxo.Open(dir, opts...)
}

func indexed(
	t *testing.T,
	s *badgerutxo.Store,
	b byte,
	after *badgerutxo.Ref,
) []badgerutxo.Ref {
	t.Helper()
	var got []badgerutxo.Ref
	require.NoError(t, s.EachIndexed(
		[][]byte{{'t', b}}, after,
		func(o badgerutxo.Output) (bool, error) {
			got = append(got, o.Ref)
			return false, nil
		},
	))
	return got
}

func TestIndexFollowsApplyAndRollback(t *testing.T) {
	t.Parallel()
	s, err := openIndexed(t, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01), out(0xAA, 0), out(0xAA, 1))

	p2 := point(2, 0x02)
	_, err = s.Apply(p2, []badgerutxo.Output{out(0xAA, 2), out(0xBB, 0)},
		[]badgerutxo.Ref{ref(0xAA, 0)})
	require.NoError(t, err)
	require.Equal(t, []badgerutxo.Ref{ref(0xAA, 1), ref(0xAA, 2)},
		indexed(t, s, 0xAA, nil))
	require.Equal(t, []badgerutxo.Ref{ref(0xBB, 0)}, indexed(t, s, 0xBB, nil))

	_, err = s.Rollback(point(1, 0x01))
	require.NoError(t, err)
	require.Equal(t, []badgerutxo.Ref{ref(0xAA, 0), ref(0xAA, 1)},
		indexed(t, s, 0xAA, nil))
	require.Empty(t, indexed(t, s, 0xBB, nil))
}

func TestIndexResumesAfterACursor(t *testing.T) {
	t.Parallel()
	s, err := openIndexed(t, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01), out(0xAA, 0), out(0xAA, 1), out(0xAA, 2))
	after := ref(0xAA, 0)
	require.Equal(t, []badgerutxo.Ref{ref(0xAA, 1), ref(0xAA, 2)},
		indexed(t, s, 0xAA, &after))
}

func TestBootstrapDropsStaleIndexRows(t *testing.T) {
	t.Parallel()
	s, err := openIndexed(t, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01), out(0xAA, 0))
	bootstrap(t, s, point(2, 0x02), out(0xBB, 0))
	require.Empty(t, indexed(t, s, 0xAA, nil))
	require.Equal(t, []badgerutxo.Ref{ref(0xBB, 0)}, indexed(t, s, 0xBB, nil))
}

// A store written before the index existed gains it at open, and a build that
// stops partway resumes after the last output it indexed.
func TestIndexBuildResumesAfterAStop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	plain := open(t, dir)
	const total = 1300
	outs := make([]badgerutxo.Output, 0, total)
	for i := range total {
		outs = append(outs, out(0xAA, uint32(i))) //nolint:gosec // small test value
	}
	bootstrap(t, plain, point(1, 0x01), outs...)
	require.NoError(t, plain.Close())

	stop := errors.New("stopped")
	_, err := openIndexed(t, dir, badgerutxo.StopIndexBuildAfterForTest(1, stop))
	require.ErrorIs(t, err, stop)

	s, err := openIndexed(t, dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ok, err := s.AddressIndexed()
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, indexed(t, s, 0xAA, nil), total)
}

func TestIndexReadRefusedWithoutAnIndexer(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01), out(0xAA, 0))
	err := s.EachIndexed([][]byte{{'t', 0xAA}}, nil,
		func(badgerutxo.Output) (bool, error) { return false, nil })
	require.ErrorIs(t, err, badgerutxo.ErrAddressIndexUnavailable)
}

// A backup of a store that kept no index, loaded into one that does, leaves
// every output live but unindexed until Restore builds the index.
func TestRestoreBuildsTheIndex(t *testing.T) {
	t.Parallel()
	plain := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, plain.Close()) })
	bootstrap(t, plain, point(1, 0x01), out(0xAA, 0), out(0xBB, 0))
	var buf bytes.Buffer
	require.NoError(t, plain.Backup(context.Background(), &buf))

	s, err := openIndexed(t, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.Reset())
	require.NoError(t, s.Restore(context.Background(), &buf))

	ok, err := s.AddressIndexed()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []badgerutxo.Ref{ref(0xAA, 0)}, indexed(t, s, 0xAA, nil))
	require.Equal(t, []badgerutxo.Ref{ref(0xBB, 0)}, indexed(t, s, 0xBB, nil))
}
