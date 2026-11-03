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
	"fmt"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// activeDelegationRows runs the active_delegation CTE, scoped when scopeSQL
// is non-empty, and returns one "pool tag key" line per row in a fixed order.
func activeDelegationRows(
	t *testing.T,
	store *Store,
	slot uint64,
	scopeSQL string,
	scopeArgs []any,
) []string {
	t.Helper()
	query, args := activeDelegationScopedSQL(slot, slot, scopeSQL, scopeArgs)
	query += `
SELECT pool_key_hash, credential_tag, staking_key FROM active_delegation
ORDER BY pool_key_hash, credential_tag, staking_key`
	rows, err := store.writeDB.QueryContext(t.Context(), query, args...)
	require.NoError(t, err)
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var pool, key []byte
		var tag int64
		require.NoError(t, rows.Scan(&pool, &tag, &key))
		ret = append(ret, fmt.Sprintf("%x %d %x", pool, tag, key))
	}
	require.NoError(t, rows.Err())
	return ret
}

// credentialScopeSQL names an explicit credential set as a scope.
func credentialScopeSQL(credentials ...lcommon.Credential) (string, []any) {
	parts := make([]string, 0, len(credentials))
	args := make([]any, 0, 2*len(credentials))
	for _, credential := range credentials {
		parts = append(parts, "SELECT ? AS credential_tag, ? AS staking_key")
		args = append(args, credential.CredType, credential.Credential.Bytes())
	}
	return strings.Join(parts, " UNION ALL "), args
}

// TestActiveDelegationScopedSQLMatchesUnscoped pins the property the pointer
// overlay relies on: restricting activeDelegationSQL's event sources to a
// credential set yields exactly the unscoped rows for that set, through every
// branch -- a surviving delegation, a de-registered credential, a delegation
// the pool's reap removed, and an account row with no certificate history.
func TestActiveDelegationScopedSQLMatchesUnscoped(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)

	kept := depositHeldPoolKey(0xd1)
	reaped := depositHeldPoolKey(0xd2)
	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(kept), 0)
	writeDepositHeldCert(t, store, 100, 1, depositHeldRegistration(reaped), 0)

	delegate := func(
		slot uint64, credential lcommon.Credential, pool lcommon.PoolKeyHash,
	) {
		writeDepositHeldCert(t, store, slot, 0, &lcommon.StakeRegistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: credential,
		}, 0)
		writeDepositHeldCert(t, store, slot+1, 0, &lcommon.StakeDelegationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeDelegation),
			StakeCredential: &credential,
			PoolKeyHash:     pool,
		}, 0)
	}
	surviving := stakeCredential(0xe1)
	delegate(110, surviving, kept)
	deregistered := stakeCredential(0xe2)
	delegate(120, deregistered, kept)
	writeDepositHeldCert(t, store, 130, 0, &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: deregistered,
	}, 0)
	stale := stakeCredential(0xe3)
	delegate(140, stale, reaped)
	// reaped retires at the boundary into epoch 1 and re-registers after it;
	// stale never re-delegates, so POOLREAP has removed its delegation.
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(reaped, 1), 0)
	writeDepositHeldCert(t, store, 1_500, 0, depositHeldRegistration(reaped), 0)
	// An imported account with no certificate history reaches
	// active_delegation only through the account fallback branches.
	imported := stakeCredential(0xe4)
	_, err := store.writeDB.ExecContext(t.Context(), `
INSERT INTO account (staking_key, credential_tag, pool, added_slot, created_slot, active)
VALUES (?, ?, ?, 150, 150, TRUE)`,
		imported.Credential.Bytes(), imported.CredType, kept.Bytes(),
	)
	require.NoError(t, err)

	const slot = 3_000
	unscoped := activeDelegationRows(t, store, slot, "", nil)
	require.ElementsMatch(t, []string{
		fmt.Sprintf("%x %d %x", kept.Bytes(), surviving.CredType,
			surviving.Credential.Bytes()),
		fmt.Sprintf("%x %d %x", kept.Bytes(), imported.CredType,
			imported.Credential.Bytes()),
	}, unscoped, "fixture: only the surviving and imported credentials "+
		"hold an active delegation at slot")

	for _, tc := range []struct {
		name  string
		scope []lcommon.Credential
		want  []string
	}{
		{
			name:  "surviving and imported",
			scope: []lcommon.Credential{surviving, imported},
			want:  unscoped,
		},
		{
			name:  "imported alone",
			scope: []lcommon.Credential{imported},
			want:  unscoped[1:],
		},
		{
			name:  "removed credentials",
			scope: []lcommon.Credential{deregistered, stale},
			want:  nil,
		},
		{
			name:  "every credential",
			scope: []lcommon.Credential{surviving, deregistered, stale, imported},
			want:  unscoped,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scopeSQL, scopeArgs := credentialScopeSQL(tc.scope...)
			require.Equal(t, tc.want,
				activeDelegationRows(t, store, slot, scopeSQL, scopeArgs))
		})
	}
}

// TestActiveDelegationScopedSQLWithoutScopeIsUnscoped pins that the historical
// path, which passes no scope, gets the query text it always had.
func TestActiveDelegationScopedSQLWithoutScopeIsUnscoped(t *testing.T) {
	t.Parallel()
	query, args := activeDelegationSQL(1_999, 2_000)
	scoped, scopedArgs := activeDelegationScopedSQL(1_999, 2_000, "", nil)
	require.Equal(t, query, scoped)
	require.Equal(t, args, scopedArgs)
	require.NotContains(t, query, "credential_scope")
	require.NotContains(t, query, "CROSS JOIN")
}

// TestPointerStakeInputsSQLReadsHistoryByCredential is the regression for the
// epoch-boundary stall: the pointer overlay used to rank every credential's
// delegation and registration history once per 400-pool chunk to attribute a
// handful of pointer outputs, which on mainnet held block application for
// minutes at every boundary. The query must now reach each event source
// through its credential index from the pointer-derived scope, never by a
// table scan, for both the plain and the CIP-0163-gated form.
func TestPointerStakeInputsSQLReadsHistoryByCredential(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	scanned := func(plan string) []string {
		var hits []string
		for _, step := range strings.Split(plan, "\n") {
			if strings.HasPrefix(step, "SCAN account") ||
				strings.HasPrefix(step, "SCAN event") {
				hits = append(hits, step)
			}
		}
		return hits
	}
	for _, expiryEpoch := range []uint64{0, 5} {
		query, args, err := pointerStakeInputsSQL(3_000, expiryEpoch)
		require.NoError(t, err)
		plan := queryPlan(t, store.writeDB, query, args...)
		require.Empty(t, scanned(plan),
			"expiry %d: the overlay must not scan delegation history",
			expiryEpoch)
		require.Contains(t, plan, "credential_scope")
	}
	// The unscoped query is the shape the overlay must not issue: its
	// history ranks are built from whole-table scans.
	query, args := activeDelegationSQL(3_000, 3_000)
	query += ` SELECT count(*) FROM active_delegation`
	require.NotEmpty(t, scanned(queryPlan(t, store.writeDB, query, args...)),
		"control: the unscoped reconstruction scans its event sources")
}
