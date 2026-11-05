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
	"bytes"
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func leiosCommitteeQuery() *olocalstatequery.BlockQuery {
	return shelleyBlockQuery(&olocalstatequery.ShelleyLeiosCommitteeQuery{})
}

// runLeiosCommittee runs the query through ls.Query and decodes the reply the
// way the client does, so the wire form is part of what is checked.
func runLeiosCommittee(
	t *testing.T,
	ls *LedgerState,
) olocalstatequery.LeiosCommitteeResult {
	t.Helper()
	got, err := ls.Query(t.Context(), leiosCommitteeQuery(), QueryPoint{})
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	var result olocalstatequery.LeiosCommitteeResult
	require.NoError(t, result.UnmarshalCBOR(wire))
	return result
}

func TestQueryLeiosCommittee_UnsupportedWithoutPrototypeGate(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	ls.config.LeiosCommittee = nil

	got, err := ls.Query(t.Context(), leiosCommitteeQuery(), QueryPoint{})
	require.EqualError(
		t,
		err,
		"unsupported query type: *localstatequery.ShelleyLeiosCommitteeQuery",
	)
	require.Nil(t, got)
}

// TestQueryLeiosCommittee_CurrentAndNextEpoch answers the committee of the
// epoch at the tip and of the epoch after it.
func TestQueryLeiosCommittee_CurrentAndNextEpoch(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	key := bytes.Repeat([]byte{0xaa}, 96)
	var asked []uint64
	ls.config.LeiosCommittee = func(
		epoch uint64,
	) ([]olocalstatequery.LeiosCommitteeSeat, error) {
		asked = append(asked, epoch)
		return []olocalstatequery.LeiosCommitteeSeat{
			{Weight: big.NewRat(int64(epoch)+1, 4), VKey: key},
			{Weight: big.NewRat(1, 8)},
		}, nil
	}

	result := runLeiosCommittee(t, ls)

	// The tip is in epoch 0.
	require.Equal(t, []uint64{0, 1}, asked)
	require.Len(t, result.Current, 2)
	require.Zero(t, big.NewRat(1, 4).Cmp(result.Current[0].Weight))
	require.Equal(t, key, result.Current[0].VKey)
	require.Nil(t, result.Current[1].VKey)
	require.Len(t, result.Next, 2)
	require.Zero(t, big.NewRat(2, 4).Cmp(result.Next[0].Weight))
}

// TestQueryLeiosCommittee_NextCommitteeUnavailable leaves the next committee
// out of the reply, as the ledger does, when it cannot be built yet.
func TestQueryLeiosCommittee_NextCommitteeUnavailable(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	ls.config.LeiosCommittee = func(
		epoch uint64,
	) ([]olocalstatequery.LeiosCommitteeSeat, error) {
		if epoch != 0 {
			return nil, errors.New("snapshot not available")
		}
		return []olocalstatequery.LeiosCommitteeSeat{
			{Weight: big.NewRat(1, 1)},
		}, nil
	}

	result := runLeiosCommittee(t, ls)

	require.Len(t, result.Current, 1)
	require.Nil(t, result.Next)
}

func TestQueryLeiosCommittee_CurrentCommitteeFailureFailsQuery(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	ls.config.LeiosCommittee = func(
		uint64,
	) ([]olocalstatequery.LeiosCommitteeSeat, error) {
		return nil, errors.New("snapshot not available")
	}

	got, err := ls.Query(t.Context(), leiosCommitteeQuery(), QueryPoint{})
	require.ErrorContains(t, err, "leios committee for epoch 0")
	require.Nil(t, got)
}
