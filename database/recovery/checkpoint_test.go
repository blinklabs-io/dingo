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

package recovery

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCheckpoint(seq uint64) Checkpoint {
	return Checkpoint{
		Seq:              seq,
		CreatedUnixMilli: 1700000000000 + int64(seq),
		CommitTimestamp:  1699999999000 + int64(seq),
		TipSlot:          1000 + seq,
		TipHash:          bytes.Repeat([]byte{byte(seq)}, 32),
		TipBlockNumber:   900 + seq,
		BlobTipSlot:      1000 + seq,
		BlobTipHash:      bytes.Repeat([]byte{byte(seq)}, 32),
	}
}

func newTestCheckpointStore(t *testing.T, retain int) *CheckpointStore {
	t.Helper()
	store, err := NewCheckpointStore(t.TempDir(), retain, nil)
	require.NoError(t, err)
	return store
}

func TestCheckpointSealAndVerify(t *testing.T) {
	t.Parallel()
	cp := testCheckpoint(1)
	assert.Error(t, cp.Verify(), "an unsealed checkpoint has no root")
	cp.Seal()
	assert.NoError(t, cp.Verify())
}

func TestCheckpointVerifyDetectsFieldTampering(t *testing.T) {
	t.Parallel()
	// Every field is bound by the root, so changing any of them after
	// sealing must be caught.
	mutations := map[string]func(*Checkpoint){
		"seq": func(c *Checkpoint) { c.Seq++ },
		"created_unix_milli": func(c *Checkpoint) {
			c.CreatedUnixMilli++
		},
		"commit_timestamp": func(c *Checkpoint) { c.CommitTimestamp++ },
		"tip_slot":         func(c *Checkpoint) { c.TipSlot++ },
		"tip_block_number": func(c *Checkpoint) { c.TipBlockNumber++ },
		"tip_hash":         func(c *Checkpoint) { c.TipHash[0] ^= 0xff },
		"blob_tip_slot":    func(c *Checkpoint) { c.BlobTipSlot++ },
		"blob_tip_hash":    func(c *Checkpoint) { c.BlobTipHash[0] ^= 0xff },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cp := testCheckpoint(1)
			cp.Seal()
			mutate(&cp)
			assert.Error(t, cp.Verify())
		})
	}
}

func TestCheckpointStoreWriteAndLatest(t *testing.T) {
	t.Parallel()
	store := newTestCheckpointStore(t, 3)
	_, err := store.Latest()
	assert.ErrorIs(t, err, ErrNoCheckpoint)

	require.NoError(t, store.Write(testCheckpoint(1)))
	require.NoError(t, store.Write(testCheckpoint(2)))

	latest, err := store.Latest()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), latest.Seq)
	assert.NoError(t, latest.Verify())
}

func TestCheckpointStoreSkipsCorruptGeneration(t *testing.T) {
	t.Parallel()
	store := newTestCheckpointStore(t, 3)
	require.NoError(t, store.Write(testCheckpoint(1)))
	require.NoError(t, store.Write(testCheckpoint(2)))

	// Damage the newest generation. Retaining several exists precisely so
	// this falls back to the previous good one instead of failing.
	newest := filepath.Join(store.Dir(), "checkpoint-00000000000000000002.bin")
	data, err := os.ReadFile(newest) //nolint:gosec
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(newest, data, 0o600))

	latest, err := store.Latest()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), latest.Seq)
}

func TestCheckpointStoreReportsNoUsableGeneration(t *testing.T) {
	t.Parallel()
	store := newTestCheckpointStore(t, 3)
	require.NoError(t, store.Write(testCheckpoint(1)))
	path := filepath.Join(store.Dir(), "checkpoint-00000000000000000001.bin")
	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))
	_, err := store.Latest()
	assert.ErrorIs(t, err, ErrNoCheckpoint)
}

func TestCheckpointStorePrunesOldGenerations(t *testing.T) {
	t.Parallel()
	store := newTestCheckpointStore(t, 2)
	for seq := uint64(1); seq <= 5; seq++ {
		require.NoError(t, store.Write(testCheckpoint(seq)))
	}
	entries, err := os.ReadDir(store.Dir())
	require.NoError(t, err)
	assert.Len(t, entries, 2)
	latest, err := store.Latest()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), latest.Seq)
}

func TestCheckpointStoreLeavesNoTempFiles(t *testing.T) {
	t.Parallel()
	store := newTestCheckpointStore(t, 3)
	require.NoError(t, store.Write(testCheckpoint(1)))
	entries, err := os.ReadDir(store.Dir())
	require.NoError(t, err)
	for _, entry := range entries {
		assert.NotContains(t, entry.Name(), ".tmp")
	}
}

func TestNewCheckpointStoreRequiresDir(t *testing.T) {
	t.Parallel()
	_, err := NewCheckpointStore("", 3, nil)
	assert.Error(t, err)
}

func TestCheckpointPointAccessors(t *testing.T) {
	t.Parallel()
	cp := testCheckpoint(4)
	assert.Equal(
		t,
		Point{Slot: cp.TipSlot, Hash: cp.TipHash},
		cp.TipPoint(),
	)
	assert.Equal(
		t,
		Point{Slot: cp.BlobTipSlot, Hash: cp.BlobTipHash},
		cp.BlobTipPoint(),
	)
}
