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

package sqlstore

import (
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestGetAccountsByCredentialGroupedByTag verifies the grouped-by-tag,
// single-column-IN rewrite of GetAccountsByCredential: it must return exactly
// the accounts named by the caller's refs, for both credential tags, across
// chunk boundaries, and it must still honor includeInactive. A per-ref
// (credential_tag = ? AND staking_key = ?) OR ... predicate and the grouped
// form agree on results; this pins the result shape independently of which
// one is in use.
func TestGetAccountsByCredentialGroupedByTag(t *testing.T) {
	store := newManagementTestStore(t)

	// Larger than the SQLite dialect's 999-parameter limit, so the grouped
	// IN list for each tag must itself be chunked (twice over per tag).
	const perTag = 1200
	refs := make([]models.StakeCredentialRef, 0, perTag*2)
	for tag := uint8(0); tag < 2; tag++ {
		for i := range perTag {
			key := make([]byte, 28)
			key[0] = tag
			binary.BigEndian.PutUint32(key[24:], uint32(i))
			require.NoError(t, store.ImportAccount(&models.Account{
				StakingKey:    key,
				CredentialTag: tag,
				Active:        true,
			}, nil))
			refs = append(
				refs,
				models.StakeCredentialRef{Tag: tag, Key: key},
			)
		}
	}

	inactiveKey := make([]byte, 28)
	inactiveKey[0] = 0
	binary.BigEndian.PutUint32(inactiveKey[24:], uint32(perTag))
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    inactiveKey,
		CredentialTag: 0,
		Active:        false,
	}, nil))
	inactiveRef := models.StakeCredentialRef{Tag: 0, Key: inactiveKey}

	// Two refs are left unrequested so the result must be exactly the
	// requested set, not "every account of that tag".
	requested := append(
		append([]models.StakeCredentialRef{}, refs[:len(refs)-2]...),
		inactiveRef,
	)

	accounts, err := store.GetAccountsByCredential(requested, false, nil)
	require.NoError(t, err)
	// The inactive account is filtered out by includeInactive=false.
	require.Len(t, accounts, len(requested)-1)
	for _, ref := range refs[:len(refs)-2] {
		account, ok := accounts[ref.MapKey()]
		require.True(t, ok, "missing account for tag %d", ref.Tag)
		require.Equal(t, ref.Tag, account.CredentialTag)
		require.Equal(t, ref.Key, account.StakingKey)
	}
	_, ok := accounts[inactiveRef.MapKey()]
	require.False(
		t,
		ok,
		"inactive account leaked through with includeInactive=false",
	)
	for _, ref := range refs[len(refs)-2:] {
		_, ok := accounts[ref.MapKey()]
		require.False(t, ok, "unrequested account leaked into the result")
	}

	withInactive, err := store.GetAccountsByCredential(
		[]models.StakeCredentialRef{inactiveRef},
		true,
		nil,
	)
	require.NoError(t, err)
	require.Contains(t, withInactive, inactiveRef.MapKey())
}

// TestAccountsByCredentialChunkQueryShape pins the query form
// GetAccountsByCredential emits: a single-column staking_key IN (...)
// predicate, never a per-ref (credential_tag = ? AND staking_key = ?)
// OR ... predicate. This is the regression guard the result-only tests
// above can't provide — reverting the query to the old per-ref OR form is
// still correct (same rows come back, just slower), so no assertion on
// returned data can distinguish the two. An EXPLAIN QUERY PLAN assertion
// isn't durable here either: the plan SQLite picks depends on table size
// and whether ANALYZE has run, not on which predicate form generated it.
// Asserting on the generated SQL text instead pins our intent directly.
func TestAccountsByCredentialChunkQueryShape(t *testing.T) {
	keys := [][]byte{
		{0x01, 0x02},
		{0x03, 0x04},
		{0x05, 0x06},
	}

	query, args := accountsByCredentialChunkQuery(1, keys, false)
	require.Contains(t, query, "staking_key IN (")
	require.NotContains(t, query, " OR ")
	require.Contains(t, query, "credential_tag = ?")
	require.Contains(t, query, "AND active = TRUE")
	require.Equal(t, []any{uint8(1), keys[0], keys[1], keys[2]}, args)

	queryInactive, argsInactive := accountsByCredentialChunkQuery(
		0,
		keys,
		true,
	)
	require.Contains(t, queryInactive, "staking_key IN (")
	require.NotContains(t, queryInactive, " OR ")
	require.NotContains(t, queryInactive, "active = TRUE")
	require.Equal(
		t,
		[]any{uint8(0), keys[0], keys[1], keys[2]},
		argsInactive,
	)
}
