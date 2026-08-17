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

package historyexpiry

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testLedgerWindow struct {
	currentSlot     uint64
	stabilityWindow uint64
	err             error
}

func (w testLedgerWindow) CurrentSlot() (uint64, error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.currentSlot, nil
}

func (w testLedgerWindow) StabilityWindow() uint64 {
	return w.stabilityWindow
}

func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})
	return db
}

func insertTestBlock(
	t *testing.T,
	db *database.Database,
	slot uint64,
	hashByte byte,
) []byte {
	t.Helper()
	hash := bytes.Repeat([]byte{hashByte}, 32)
	err := db.BlockCreate(models.Block{
		Slot:   slot,
		Hash:   hash,
		Cbor:   []byte{0x82, hashByte},
		Type:   1,
		Number: slot,
	}, nil)
	require.NoError(t, err)
	return hash
}

func TestPrunerExpiresBlocksOlderThanStabilityWindow(t *testing.T) {
	db := newTestDB(t)
	oldHash := insertTestBlock(t, db, 9, 0x09)
	boundaryHash := insertTestBlock(t, db, 10, 0x0a)
	recentHash := insertTestBlock(t, db, 11, 0x0b)

	pruner := NewPruner(PrunerConfig{
		LedgerState: testLedgerWindow{
			currentSlot:     15,
			stabilityWindow: 5,
		},
		DB:        db,
		Frequency: time.Hour,
	})
	pruner.prune(context.Background())

	txn := db.BlobTxn(false)
	defer txn.Release()
	_, _, err := db.Blob().GetBlock(txn.Blob(), 9, oldHash)
	assert.ErrorIs(t, err, types.ErrHistoryExpired)
	_, _, err = db.Blob().GetBlock(txn.Blob(), 10, boundaryHash)
	assert.NoError(t, err)
	_, _, err = db.Blob().GetBlock(txn.Blob(), 11, recentHash)
	assert.NoError(t, err)
}

func TestPrunerSkipsWhenCurrentSlotWithinStabilityWindow(t *testing.T) {
	db := newTestDB(t)
	hash := insertTestBlock(t, db, 1, 0x01)

	pruner := NewPruner(PrunerConfig{
		LedgerState: testLedgerWindow{
			currentSlot:     5,
			stabilityWindow: 5,
		},
		DB:        db,
		Frequency: time.Hour,
	})
	pruner.prune(context.Background())

	txn := db.BlobTxn(false)
	defer txn.Release()
	_, _, err := db.Blob().GetBlock(txn.Blob(), 1, hash)
	assert.NoError(t, err)
}

func TestPrunerSkipsAlreadyExpiredBlocks(t *testing.T) {
	db := newTestDB(t)
	firstHash := insertTestBlock(t, db, 1, 0x01)
	secondHash := insertTestBlock(t, db, 2, 0x02)

	_, err := db.PruneBlock(1, firstHash)
	require.NoError(t, err)

	pruner := NewPruner(PrunerConfig{
		LedgerState: testLedgerWindow{
			currentSlot:     10,
			stabilityWindow: 5,
		},
		DB:        db,
		Frequency: time.Hour,
	})
	pruner.prune(context.Background())

	txn := db.BlobTxn(false)
	defer txn.Release()
	_, _, err = db.Blob().GetBlock(txn.Blob(), 2, secondHash)
	assert.ErrorIs(t, err, types.ErrHistoryExpired)
}

func TestPrunerStartValidation(t *testing.T) {
	db := newTestDB(t)
	tests := []struct {
		name string
		cfg  PrunerConfig
	}{
		{
			name: "invalid frequency",
			cfg: PrunerConfig{
				LedgerState: testLedgerWindow{},
				DB:          db,
			},
		},
		{
			name: "missing ledger",
			cfg: PrunerConfig{
				DB:        db,
				Frequency: time.Hour,
			},
		},
		{
			name: "missing database",
			cfg: PrunerConfig{
				LedgerState: testLedgerWindow{},
				Frequency:   time.Hour,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewPruner(tt.cfg).Start(context.Background())
			require.Error(t, err)
		})
	}
}

func TestPrunerStartStop(t *testing.T) {
	db := newTestDB(t)
	pruner := NewPruner(PrunerConfig{
		LedgerState: testLedgerWindow{},
		DB:          db,
		Frequency:   time.Hour,
	})
	require.NoError(t, pruner.Start(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, pruner.Stop(ctx))
}

func TestPrunerHandlesCurrentSlotError(t *testing.T) {
	db := newTestDB(t)
	hash := insertTestBlock(t, db, 1, 0x01)

	pruner := NewPruner(PrunerConfig{
		LedgerState: testLedgerWindow{
			err: errors.New("slot clock unavailable"),
		},
		DB:        db,
		Frequency: time.Hour,
	})
	pruner.prune(context.Background())

	txn := db.BlobTxn(false)
	defer txn.Release()
	_, _, err := db.Blob().GetBlock(txn.Blob(), 1, hash)
	assert.NoError(t, err)
}
