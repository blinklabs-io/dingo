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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/stretchr/testify/require"
)

// rewardJournalDriver writes claimed reward outputs to the journal the way the
// ledger's writeClaimedRewardCredits does. It is the only part of the
// equivalence test that depends on how the journal is keyed.
type rewardJournalDriver struct {
	store *sqlstore.Store
	// cursors holds, per round, the account the last chunk ended on, as the
	// compaction job does.
	cursors map[uint64]uint
}

func (d rewardJournalDriver) write(t *testing.T, outputs []*models.RewardAccountOutput) {
	t.Helper()
	credits := make([]models.AccountRewardCredit, 0, len(outputs))
	for _, output := range outputs {
		credits = append(credits, models.AccountRewardCredit{
			CredentialTag: output.CredentialTag,
			StakingKey:    output.StakingKey,
			Amount:        uint64(output.Amount),
			Slot:          output.BoundarySlot,
			OutputID:      output.ID,
			AccountID:     output.AccountID,
		})
	}
	if len(credits) == 0 {
		return
	}
	require.NoError(t, d.store.AddAccountRewardsByCredential(credits, nil))
}

func (d rewardJournalDriver) foldChunk(t *testing.T, epoch uint64, limit int) int {
	t.Helper()
	from := uint(0)
	if d.cursors != nil {
		from = d.cursors[epoch]
	}
	outputs, err := d.store.ClaimUnfoldedRewardCredits(epoch, from, limit, nil)
	require.NoError(t, err)
	d.write(t, outputs)
	if d.cursors != nil && len(outputs) > 0 {
		d.cursors[epoch] = outputs[len(outputs)-1].AccountID
	}
	return len(outputs)
}

func (d rewardJournalDriver) foldRound(t *testing.T, epoch uint64) {
	t.Helper()
	for d.foldChunk(t, epoch, 1000) > 0 {
	}
}

func (d rewardJournalDriver) foldCredential(t *testing.T, tag uint8, key []byte) {
	t.Helper()
	outputs, err := d.store.ClaimPendingRewardCreditsForCredential(tag, key, nil)
	require.NoError(t, err)
	d.write(t, outputs)
}
