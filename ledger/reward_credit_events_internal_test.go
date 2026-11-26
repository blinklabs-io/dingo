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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestPostSnapshotCreditEventsListEachSourceByCredential(t *testing.T) {
	t.Parallel()
	a := models.NewStakeCredentialRef(0, []byte{0x02})
	b := models.NewStakeCredentialRef(0, []byte{0x01})
	events, err := postSnapshotCreditEvents([]PostSnapshotCredit{
		{Slot: 9, Ref: a, Source: []byte{0x02}, Amount: 5},
		{Slot: 9, Ref: b, Source: []byte{0x07}, Amount: 3},
		{Slot: 9, Ref: a, Source: []byte{0x01}, Amount: 4},
	}, 9)
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, []byte{0x01}, events[0].StakingKey)
	require.Equal(t, []byte{0x07}, events[0].TxHash)
	require.Equal(t, []byte{0x01}, events[1].TxHash)
	require.Equal(t, []byte{0x02}, events[2].TxHash)
	for _, e := range events {
		require.True(t, e.PostSnapshot)
		require.Equal(t, uint64(9), e.AddedSlot)
	}
}

// A credit loaded from the store is a total with no source hash, so it
// cannot be listed as the events it summed.
func TestPostSnapshotCreditEventsRefuseALoadedTotal(t *testing.T) {
	t.Parallel()
	_, err := postSnapshotCreditEvents([]PostSnapshotCredit{
		{Slot: 9, Ref: models.NewStakeCredentialRef(0, []byte{0x01}), Amount: 5, Loaded: true},
		{Slot: 9, Ref: models.NewStakeCredentialRef(0, []byte{0x02}), Source: []byte{0x01}, Amount: 4},
	}, 9)
	require.ErrorIs(t, err, ErrLedgerStateUnserved)
}
