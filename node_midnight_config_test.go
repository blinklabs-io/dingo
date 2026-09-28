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

package dingo

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestMidnightIndexerEnablement(t *testing.T) {
	for _, mode := range []StorageMode{StorageModeAPI, StorageModeCore} {
		for _, enabled := range []bool{false, true} {
			cfg := MidnightConfig{Enabled: enabled}
			want := enabled && mode.IsAPI()
			require.Equal(t, want, midnightIndexerActive(mode, cfg))
			if !want {
				n := &Node{config: Config{storageMode: mode, midnight: cfg}}
				require.NotPanics(t, func() {
					require.NoError(t, n.reinitializeMidnightIndexer())
				}, "inactive indexers must return before accessing ledger or storage")
			}
		}
	}
	// A separately enabled server must not enable indexing.
	require.False(t, midnightIndexerActive(StorageModeAPI, MidnightConfig{
		ServerEnabled: true, Port: 50051,
	}))
}

func newMidnightConfigDatabase(t *testing.T, slot uint64) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(slot, make([]byte, 32)),
	}, nil))
	return db
}

func TestMidnightIndexerConfigUsesCurrentPersistedTip(t *testing.T) {
	n := &Node{db: newMidnightConfigDatabase(t, 123)}
	cfg := n.midnightIndexerConfig()
	require.NotNil(t, cfg.LedgerTipSlot)
	slot, err := cfg.LedgerTipSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(123), slot)
	// The closure must follow a replaced database rather than pinning the old one.
	n.db = newMidnightConfigDatabase(t, 456)
	slot, err = cfg.LedgerTipSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(456), slot)
}

func TestMidnightIndexerFatalCallbackPreservesCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &Node{
		db:  newMidnightConfigDatabase(t, 123),
		ctx: ctx, cancel: cancel,
		config: Config{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}
	callback := n.midnightIndexerConfig().FatalErrorFunc
	want := errors.New("midnight component failure")
	callback(want)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, n.waitForShutdown(), want)
	require.ErrorIs(t, n.resolveRunError(context.Canceled), want)
	callback(errors.New("later midnight failure"))
	require.ErrorIs(t, n.waitForShutdown(), want)
}
