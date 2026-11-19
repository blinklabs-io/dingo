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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestSnapshotUtxoSetReadsTheSetAsOfTheCall requires the view to answer every
// read, UTxOs and tip, as of the call while the set keeps following the
// ledger, and to refuse writes.
func TestSnapshotUtxoSetReadsTheSetAsOfTheCall(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	hash := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }
	add := func(slot uint64, b byte) {
		p := ocommon.NewPoint(slot, hash(b))
		require.NoError(t, set.ApplyUtxoChanges(&p, []ledger.UtxoEvent{{
			Added: []ledger.UtxoOutput{{
				Ref: models.UtxoId{Hash: hash(b)}, Cbor: []byte{0x80, b},
			}},
		}}))
	}
	add(1, 1)

	view, release, err := set.SnapshotUtxoSet()
	require.NoError(t, err)
	t.Cleanup(release)
	add(2, 2)

	reader, ok := view.(ledger.UtxoSetReader)
	require.True(t, ok)
	_, live, err := reader.Get(hash(1), 0)
	require.NoError(t, err)
	require.True(t, live)
	_, live, err = reader.Get(hash(2), 0)
	require.NoError(t, err)
	require.False(t, live, "an output applied after the snapshot")
	_, live, err = set.Get(hash(2), 0)
	require.NoError(t, err)
	require.True(t, live, "the set itself follows the ledger")
	tip, _, err := view.(ledger.UtxoSetTipReader).Tip()
	require.NoError(t, err)
	require.Equal(t, uint64(1), tip.Slot)

	historical, err := view.(ledger.UtxoSetHistory).UtxoSetAt(
		ocommon.NewPoint(1, hash(1)),
	)
	require.NoError(t, err)
	t.Cleanup(historical.Close)
	_, live, err = historical.Get(hash(1), 0)
	require.NoError(t, err)
	require.True(t, live)
	_, live, err = historical.Get(hash(2), 0)
	require.NoError(t, err)
	require.False(t, live)
	historical.Close()
	historical.Close()
	_, live, err = reader.Get(hash(1), 0)
	require.NoError(t, err)
	require.True(t, live, "closing a historical view preserves its parent")

	p := ocommon.NewPoint(3, hash(3))
	require.Error(t, view.ApplyUtxoChanges(&p, nil), "a snapshot is read-only")
}

func TestSnapshotHistorySurvivesLiveRollback(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	first := ocommon.NewPoint(1, bytes.Repeat([]byte{1}, 32))
	second := ocommon.NewPoint(2, bytes.Repeat([]byte{2}, 32))
	require.NoError(t, set.ApplyUtxoChanges(&first, nil))
	require.NoError(t, set.ApplyUtxoChanges(&second, []ledger.UtxoEvent{{
		Added: []ledger.UtxoOutput{{
			Ref: models.UtxoId{Hash: second.Hash}, Cbor: []byte{0x80},
		}},
	}}))
	view, release, err := set.SnapshotUtxoSet()
	require.NoError(t, err)
	t.Cleanup(release)
	require.NoError(t, set.RollbackTo(first))
	_, live, err := set.Get(second.Hash, 0)
	require.NoError(t, err)
	require.False(t, live)
	historical, err := view.(ledger.UtxoSetHistory).UtxoSetAt(second)
	require.NoError(t, err, "pinned history must survive a live rollback")
	t.Cleanup(historical.Close)
	_, live, err = historical.Get(second.Hash, 0)
	require.NoError(t, err)
	require.True(t, live, "pinned history lost an output after live rollback")
}
