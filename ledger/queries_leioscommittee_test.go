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
	"encoding/hex"
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
	// Leaf handlers answer in the single-element []any{value} form.
	values, ok := got.([]any)
	require.True(t, ok, "expected the []any result wrapper, got %T", got)
	require.Len(t, values, 1)
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
			return nil, ErrLeiosCommitteeNotYetAvailable
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

// TestQueryLeiosCommittee_DirectReplyBytes pins the direct reply to the one
// era result wrapper around [current, StrictMaybe next]: the wrapper is not
// doubled by the leaf handler form.
func TestQueryLeiosCommittee_DirectReplyBytes(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	ls.config.LeiosCommittee = func(
		uint64,
	) ([]olocalstatequery.LeiosCommitteeSeat, error) {
		return []olocalstatequery.LeiosCommitteeSeat{
			{Weight: big.NewRat(1, 2)},
		}, nil
	}

	got, err := ls.Query(t.Context(), leiosCommitteeQuery(), QueryPoint{})
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	// [ [ [seat], [ [seat] ] ] ], weight as tag 30 [1, 2], no key.
	require.Equal(
		t,
		"81"+"82"+"81"+"82d81e82010280"+"81"+"81"+"82d81e82010280",
		hex.EncodeToString(wire),
	)
}

// TestQueryLeiosCommittee_ViaGetCBOR answers the query inside GetCBOR with the
// bare [current, next] as tag-24 content, as the ledger encodes it.
func TestQueryLeiosCommittee_ViaGetCBOR(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	ls.config.LeiosCommittee = func(
		uint64,
	) ([]olocalstatequery.LeiosCommitteeSeat, error) {
		return []olocalstatequery.LeiosCommitteeSeat{
			{Weight: big.NewRat(1, 2)},
		}, nil
	}
	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCborQuery{
				Query: &olocalstatequery.ShelleyLeiosCommitteeQuery{},
			},
		},
	}

	got, err := ls.Query(t.Context(), query, QueryPoint{})
	require.NoError(t, err)
	values, ok := got.([]any)
	require.True(t, ok, "expected the []any result wrapper, got %T", got)
	require.Len(t, values, 1)
	tag, ok := values[0].(cbor.Tag)
	require.True(t, ok, "expected a tag-24 value, got %T", values[0])
	require.EqualValues(t, cbor.CborTagCbor, tag.Number)
	content, ok := tag.Content.([]byte)
	require.True(t, ok, "tag content must be CBOR bytes, got %T", tag.Content)
	require.Equal(
		t,
		"82"+"81"+"82d81e82010280"+"81"+"81"+"82d81e82010280",
		hex.EncodeToString(content),
	)
}

// Only a committee that cannot be built yet is left out of the reply; a
// shutdown, backlog or storage failure on the next epoch fails the query.
func TestQueryLeiosCommittee_NextCommitteeOperationalFailureFailsQuery(
	t *testing.T,
) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	storageErr := errors.New("storage fault")
	ls.config.LeiosCommittee = func(
		epoch uint64,
	) ([]olocalstatequery.LeiosCommitteeSeat, error) {
		if epoch != 0 {
			return nil, storageErr
		}
		return []olocalstatequery.LeiosCommitteeSeat{
			{Weight: big.NewRat(1, 1)},
		}, nil
	}

	got, err := ls.Query(t.Context(), leiosCommitteeQuery(), QueryPoint{})
	require.ErrorIs(t, err, storageErr)
	require.ErrorContains(t, err, "leios committee for epoch 1")
	require.Nil(t, got)
}
