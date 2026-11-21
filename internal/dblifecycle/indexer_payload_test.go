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

package dblifecycle_test

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// advancingTipState reports one tip for its first read and another from then
// on, as a writer batch committing while the backup runs.
type advancingTipState struct {
	reads atomic.Int32
}

func (s *advancingTipState) GetSyncState(
	key string, _ *database.Txn,
) (string, error) {
	if key != indexerdb.TipSyncKey {
		return "", nil
	}
	hash := byte(1)
	if s.reads.Add(1) > 1 {
		hash = 2
	}
	return fmt.Sprintf("%d:%d:%x", 10*int(hash), hash, [32]byte{hash}), nil
}

func (s *advancingTipState) SetSyncState(
	string, string, *database.Txn,
) error {
	return nil
}

func (s *advancingTipState) DeleteSyncState(string, *database.Txn) error {
	return nil
}

func (s *advancingTipState) ListSyncStateKeysByPrefix(
	string, *database.Txn,
) ([]string, error) {
	return nil, nil
}

func TestCaptureIndexerPayloadRecordsTheTipOfTheBackup(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	state := &advancingTipState{}
	store, err := indexerdb.NewStore(db, state, 10)
	require.NoError(t, err)

	_, payload, cleanup, err := dblifecycle.CaptureIndexerPayload(
		t.Context(), db, store, t.TempDir(),
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.EqualValues(t, 20, payload.TipSlot,
		"the tip that moved during the first backup is not recorded")
	require.Equal(t, hex.EncodeToString(bytesOf(2)), payload.TipHash)
}

func bytesOf(b byte) []byte {
	out := make([]byte, 32)
	out[0] = b
	return out
}
