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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func snapshotPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
}

// PoolStakeSnapshotsCapturedIn reads only the rows captured inside the
// range: a row outside it that cannot even be decoded is never touched.
func TestPoolStakeSnapshotsCapturedInReadsOnlyTheRange(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	pool := bytes.Repeat([]byte{0x61}, 28)
	save := func(slot uint64, epoch uint64, captured uint64, typ string) {
		tip := snapshotPoint(slot)
		require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
			Snapshots: []ledger.SnapshotChange{{
				Kind: ledger.SnapshotSave,
				Snapshot: &models.PoolStakeSnapshot{
					Epoch: epoch, SnapshotType: typ, PoolKeyHash: pool,
					CapturedSlot: captured,
				},
			}},
		}))
	}
	save(1, 3, 150, "mark")
	save(2, 1, 100, "set")
	save(3, 1, 199, "go")
	// Outside [100, 199], with values no decoder accepts.
	garbage := func(slot, epoch, captured uint64) {
		require.NoError(t, set.PutSnapshotRowForTest(
			snapshotPoint(slot), badgerutxo.SnapshotRow{
				Epoch: epoch, Type: "mark", Pool: pool,
				Value: []byte{0xff, 0xff}, CapturedSlot: captured,
			},
		))
	}
	garbage(4, 0, 99)
	garbage(5, 2, 200)
	garbage(6, 9, 5000)

	require.NoError(t, set.IndexSnapshotSlotsForTest())

	got, err := set.PoolStakeSnapshotsCapturedIn(100, 199)
	require.NoError(t, err, "a row outside the range was read")
	var keys []string
	for _, sn := range got {
		keys = append(keys, sn.SnapshotType)
		require.GreaterOrEqual(t, sn.CapturedSlot, uint64(100))
		require.LessOrEqual(t, sn.CapturedSlot, uint64(199))
	}
	require.Equal(t, []string{"go", "set", "mark"}, keys,
		"in key order: epoch 1 go, epoch 1 set, epoch 3 mark")
}

// A set written before the capture index stored each snapshot row as its
// CBOR alone. The backfill reads the captured slot out of that CBOR, so the
// indexed read finds the rows by the slots they were captured at.
func TestSnapshotSlotBackfillReadsTheSlotFromStoredRows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	set, err := utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	tip := snapshotPoint(1)
	require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{}))
	require.NoError(t, set.Close())

	pool := bytes.Repeat([]byte{0x62}, 28)
	legacy := func(epoch uint64, typ string, captured uint64) ([]byte, []byte) {
		v, err := cbor.Encode(models.PoolStakeSnapshot{
			Epoch: epoch, SnapshotType: typ, PoolKeyHash: pool,
			TotalStake: 7, CapturedSlot: captured,
		})
		require.NoError(t, err)
		k := binary.BigEndian.AppendUint64([]byte{badgerutxo.SnapshotTag}, epoch)
		k = append(k, byte(len(typ)))
		k = append(k, typ...)
		return append(k, pool...), v
	}
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for _, r := range []struct {
			epoch    uint64
			typ      string
			captured uint64
		}{{1, "mark", 120}, {2, "set", 250}, {2, "go", 260}} {
			k, v := legacy(r.epoch, r.typ, r.captured)
			if err := txn.Set(k, v); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	set, err = utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	require.NoError(t, set.IndexSnapshotSlotsForTest())

	got, err := set.PoolStakeSnapshotsCapturedIn(200, 299)
	require.NoError(t, err)
	var keys []string
	for _, sn := range got {
		keys = append(keys, fmt.Sprintf("%d%s@%d", sn.Epoch, sn.SnapshotType, sn.CapturedSlot))
		require.EqualValues(t, 7, sn.TotalStake)
		require.Equal(t, pool, sn.PoolKeyHash)
	}
	require.Equal(t, []string{"2go@260", "2set@250"}, keys)
}
