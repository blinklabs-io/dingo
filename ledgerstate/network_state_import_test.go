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

package ledgerstate

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/stretchr/testify/require"
)

func TestImportTipPersistsSnapshotNetworkState(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	const (
		treasury = uint64(87_920_693_660_807)
		reserves = uint64(14_914_270_613_432_674)
	)
	err = importTip(context.Background(), ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			Epoch:    12,
			Treasury: treasury,
			Reserves: reserves,
			EraIndex: 6,
			Tip: &SnapshotTip{
				Slot:      123_456,
				BlockHash: make([]byte, 32),
			},
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	})
	require.NoError(t, err)

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(123_456), state.Slot)
	require.Equal(t, treasury, uint64(state.Treasury))
	require.Equal(t, reserves, uint64(state.Reserves))
}
