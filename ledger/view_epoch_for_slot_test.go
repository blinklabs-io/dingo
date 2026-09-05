package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLedgerViewSatisfiesEpochState pins that LedgerView provides gouroboros'
// optional EpochState capability.
//
// Rules that are expressed relative to the current epoch degrade to a weaker
// check when the ledger state cannot supply one, and they degrade silently. The
// pool-deposit decision is the case that found this: without an epoch it cannot
// tell a retired pool from a registered one, charges no deposit for a
// registration that needs one, and the transaction then fails value
// conservation by exactly the deposit (issue #3908).
func TestLedgerViewSatisfiesEpochState(t *testing.T) {
	var lv any = &LedgerView{}
	_, ok := lv.(lcommon.EpochState)
	require.True(t, ok,
		"LedgerView must satisfy common.EpochState, or every rule that needs "+
			"the current epoch silently takes its degraded path")
}

// TestLedgerViewEpochForSlot checks the mapping itself against a known cache.
func TestLedgerViewEpochForSlot(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 196, StartSlot: 16_934_400, LengthInSlots: 86_400},
			{EpochId: 197, StartSlot: 17_020_800, LengthInSlots: 86_400},
		},
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}

	for _, tc := range []struct {
		name string
		slot uint64
		want uint64
	}{
		{"first slot of an epoch", 17_020_800, 197},
		// The slot that wedged the replay, ninety slots into epoch 197.
		{"the slot from issue 3908", 17_020_890, 197},
		{"last slot of the previous epoch", 17_020_799, 196},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lv.EpochForSlot(tc.slot)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("a slot outside the cache is an error, not a guess", func(t *testing.T) {
		_, err := lv.EpochForSlot(99_000_000)
		require.Error(t, err)
	})
}
