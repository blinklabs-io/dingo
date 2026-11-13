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

package utxoset

import (
	"bytes"
	"errors"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestApplyUtxoChangesSucceedsWhenPruningFails requires a journal prune
// failure after a committed batch not to fail the commit: the ledger stops
// following a set whose commit reports an error.
func TestApplyUtxoChangesSucceedsWhenPruningFails(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir(), HistorySlots: 10})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	pruned := 0
	set.pruneJournal = func(uint64) error {
		pruned++
		return errors.New("prune failed")
	}
	for slot := uint64(1); slot <= pruneEvery; slot++ {
		tip := ocommon.NewPoint(slot+10, bytes.Repeat([]byte{1}, 32))
		require.NoError(t, set.ApplyUtxoChanges(&tip, nil))
	}
	require.NotZero(t, pruned, "the prune was never attempted")
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(pruneEvery+10), tip.Slot)
}
