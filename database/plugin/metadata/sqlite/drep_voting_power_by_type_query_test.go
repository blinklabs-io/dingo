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
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/drepquery"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// oldVotingPowerByTypeSQL is the pre-fix (blinklabs-io/dingo#4364) shape of
// drepquery.VotingPowerByTypeSQL for sqlite: it scans every live utxo row and
// runs a correlated EXISTS subquery against account per row, instead of
// starting from the small set of drep-delegated accounts and joining outward
// to utxo. Kept here, side by side with the fixed query, so the tests below
// hold both shapes against the same fixture permanently rather than only at
// the moment of the fix. IN (?,?) is inlined for the fixed two-element
// AlwaysAbstain/AlwaysNoConfidence collection this test always passes,
// mirroring what expandDrepCollectionQuery would produce for that count.
const oldVotingPowerByTypeSQL = `
	SELECT a.drep_type AS drep_type,
		   COALESCE(SUM(
			   COALESCE(u.utxo_sum, 0)
			   + COALESCE(CAST(a.reward AS INTEGER), 0)
		   ), 0) AS stake
	FROM account a
	LEFT JOIN (
		SELECT credential_tag, staking_key,
			   COALESCE(SUM(CAST(amount AS INTEGER)), 0) AS utxo_sum
		FROM utxo
		WHERE deleted_slot = 0
		  AND EXISTS (
			  SELECT 1 FROM account ax
			  WHERE ax.credential_tag = utxo.credential_tag
			    AND ax.staking_key = utxo.staking_key
			    AND ax.active = 1 AND ax.drep_type IN (?,?)
		  )
		GROUP BY credential_tag, staking_key
	) u ON u.credential_tag = a.credential_tag
		AND u.staking_key = a.staking_key
	WHERE a.active = 1 AND a.drep_type IN (?,?)
	GROUP BY a.drep_type
`

// newVotingPowerByTypeSQL returns the current (fixed) query, with its
// collection placeholder expanded exactly as
// sqlstore.expandDrepCollectionQuery would for a two-element drep_type list.
// That helper is unexported in package sqlstore, so the expansion is
// reproduced here rather than imported.
func newVotingPowerByTypeSQL(t *testing.T, expiryEpoch uint64) string {
	t.Helper()
	query := drepquery.VotingPowerByTypeSQL("sqlite", expiryEpoch)
	expanded := strings.Replace(query, "IN ?", "IN (?,?)", 2)
	require.NotEqual(
		t,
		query,
		expanded,
		"expected two IN ? placeholders to expand",
	)
	return expanded
}

// runVotingPowerByType executes a two-element AlwaysAbstain/
// AlwaysNoConfidence query built by either oldVotingPowerByTypeSQL or
// newVotingPowerByTypeSQL and returns the resulting drep_type -> stake map.
// Both queries bind [type1, type2, type1, type2] with no expiry epoch, or
// [expiry, type1, type2, expiry, type1, type2] with one.
func runVotingPowerByType(
	t *testing.T,
	db *sql.DB,
	query string,
	expiryEpoch uint64,
	drepType1, drepType2 uint64,
) map[uint64]uint64 {
	t.Helper()
	args := []any{}
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	rows, err := db.QueryContext(context.Background(), query, args...)
	require.NoError(t, err)
	defer rows.Close()
	ret := map[uint64]uint64{}
	for rows.Next() {
		var drepType int64
		var stake int64
		require.NoError(t, rows.Scan(&drepType, &stake))
		ret[uint64(drepType)] = uint64(stake)
	}
	require.NoError(t, rows.Err())
	return ret
}

// seedVotingPowerByTypeFixture creates a mix of accounts and utxos designed
// to exercise every exclusion the account-first rewrite must preserve:
//   - two AlwaysAbstain accounts (one with two live utxos, to prove the
//     inner SUM/GROUP BY still aggregates per credential) and one
//     AlwaysNoConfidence account, all counted;
//   - a deleted utxo on an otherwise-counted account, excluded by
//     deleted_slot;
//   - an inactive AlwaysAbstain account, excluded entirely (reward and
//     utxo);
//   - a script-credential (drep_type=1) account with its own utxo, excluded
//     because it is not one of the requested predefined types -- this is
//     the case that would silently reappear if the rewritten inner query
//     ever dropped its ax.drep_type filter;
//   - an orphan utxo with no matching account row at all, excluded because
//     the account-first join never reaches it;
//   - an AlwaysAbstain account with a nonzero ExpirationEpoch, used by the
//     boundary test below.
func seedVotingPowerByTypeFixture(t *testing.T, store sharedDrepStore) {
	t.Helper()
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("abstain-one-staking-key-28by"),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     true, Reward: 1000,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("abstain-one-live-utxo-tx-32bytes"),
		StakingKey: []byte("abstain-one-staking-key-28by"),
		Amount:     500,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("abstain-one-live-utxo-2-tx-32byt"),
		StakingKey: []byte("abstain-one-staking-key-28by"),
		Amount:     700,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:        []byte("abstain-one-deleted-utxo-tx-32by"),
		StakingKey:  []byte("abstain-one-staking-key-28by"),
		Amount:      9_999_999,
		DeletedSlot: 999,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    []byte("abstain-two-staking-key-28byt"),
		CredentialTag: 1,
		DrepType:      models.DrepTypeAlwaysAbstain,
		Active:        true, Reward: 50,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          []byte("abstain-two-live-utxo-tx-32byte"),
		StakingKey:    []byte("abstain-two-staking-key-28byt"),
		CredentialTag: 1,
		Amount:        300,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("no-confidence-staking-key-28b"),
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		Active:     true, Reward: 20,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("no-confidence-live-utxo-tx-32byt"),
		StakingKey: []byte("no-confidence-staking-key-28b"),
		Amount:     80,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("inactive-abstain-staking-key-2"),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     false, Reward: 999,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("inactive-abstain-utxo-tx-32byte"),
		StakingKey: []byte("inactive-abstain-staking-key-2"),
		Amount:     444,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("script-drep-staking-key-28byt"),
		DrepType:   1, // credential-backed (script hash), not predefined
		Active:     true, Reward: 10,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("script-drep-utxo-tx-32bytes-here"),
		StakingKey: []byte("script-drep-staking-key-28byt"),
		Amount:     20,
	}))

	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("orphan-utxo-no-account-tx-32byte"),
		StakingKey: []byte("orphan-staking-key-no-account"),
		Amount:     777,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:      []byte("expiring-abstain-staking-key1"),
		DrepType:        models.DrepTypeAlwaysAbstain,
		Active:          true,
		Reward:          5,
		ExpirationEpoch: 100,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("expiring-abstain-utxo-tx-32byte"),
		StakingKey: []byte("expiring-abstain-staking-key1"),
		Amount:     15,
	}))
}

// sharedDrepStore is the subset of *sqlstore.Store this file exercises
// directly, without going through the full drepStore interface in
// shared_sqlstore_drep_parity_test.go.
type sharedDrepStore interface {
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetDRepVotingPowerByType(
		[]uint64,
		uint64,
		types.Txn,
	) (map[uint64]uint64, error)
}

func TestDRepVotingPowerByTypeUtxoJoinRewriteMatchesReference(t *testing.T) {
	t.Parallel()
	store, writeDB := newSharedSQLStore(t)
	seedVotingPowerByTypeFixture(t, store)

	// Independently hand-computed from the fixture: two live utxos plus the
	// reward on the first AlwaysAbstain account, one live utxo plus reward
	// on the second, plus the ExpirationEpoch=100 account (included because
	// expiryEpoch=0 applies no filter at all). The deleted utxo, the
	// inactive account, the script-credential account, and the orphan utxo
	// each contribute nothing.
	wantAbstain := uint64(1000+500+700) + uint64(50+300) + uint64(5+15)
	wantNoConfidence := uint64(20 + 80)
	want := map[uint64]uint64{
		models.DrepTypeAlwaysAbstain:      wantAbstain,
		models.DrepTypeAlwaysNoConfidence: wantNoConfidence,
	}

	oldResult := runVotingPowerByType(
		t,
		writeDB,
		oldVotingPowerByTypeSQL,
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.Equal(t, want, oldResult, "pre-fix correlated-EXISTS query")

	newResult := runVotingPowerByType(
		t,
		writeDB,
		newVotingPowerByTypeSQL(t, 0),
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.Equal(t, want, newResult, "fixed account-first join query")

	// The production entry point (GetDRepVotingPowerByType) must agree too.
	prodResult, err := store.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		0,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, want, prodResult)
}

// TestDRepVotingPowerByTypeExpiryBoundary drives the CIP-0163 expiry filter
// through its exact boundary: expiration_epoch = 100 must still count an
// account queried as of epoch 100, and must exclude it as of epoch 101. This
// filter and its boundary carry over unchanged from the pre-fix query, but
// the rewrite touches the clause's surrounding structure, so the boundary is
// exercised directly against the production entry point.
func TestDRepVotingPowerByTypeExpiryBoundary(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	seedVotingPowerByTypeFixture(t, store)

	const stakeWithoutExpiring = uint64(1000+500+700) + uint64(50+300)
	const stakeWithExpiring = stakeWithoutExpiring + uint64(5+15)

	atExpiry, err := store.GetDRepVotingPowerByType(
		[]uint64{models.DrepTypeAlwaysAbstain},
		100,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		stakeWithExpiring,
		atExpiry[models.DrepTypeAlwaysAbstain],
		"expiration_epoch = queried epoch must still count",
	)

	afterExpiry, err := store.GetDRepVotingPowerByType(
		[]uint64{models.DrepTypeAlwaysAbstain},
		101,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		stakeWithoutExpiring,
		afterExpiry[models.DrepTypeAlwaysAbstain],
		"expiration_epoch < queried epoch must be excluded",
	)
}

// TestDRepVotingPowerByTypeQueryPlanDrivesFromAccountNotUtxo is the
// performance regression check for blinklabs-io/dingo#4364. At this test's
// row counts sqlite flattens the pre-fix query's EXISTS into an indexed
// semi-join rather than literally naming it a "CORRELATED SCALAR SUBQUERY"
// in EXPLAIN QUERY PLAN output (that wording is what the issue's comment
// observed at production scale, 2.86M live utxo rows), but the pre-fix
// query's FROM clause is still `utxo`, so its inner subquery's driving loop
// is forced to scan every live utxo row regardless of scale, then probe
// account per row. The fixed query's FROM clause is `account`, so its
// driving loop scans only the requested drep_type's accounts and probes
// utxo per row -- the account-first shape from #4364. That FROM-clause
// difference, not a cardinality estimate, is what fixes the driving table,
// so this assertion holds at any data size. Reverting the production fix
// makes this test fail: drepquery.VotingPowerByTypeSQL would then also plan
// a full live-utxo scan as its driving loop.
func TestDRepVotingPowerByTypeQueryPlanDrivesFromAccountNotUtxo(t *testing.T) {
	t.Parallel()
	store, writeDB := newSharedSQLStore(t)
	seedVotingPowerByTypeFixture(t, store)

	const utxoDrivenScan = "SEARCH utxo USING COVERING INDEX " +
		"idx_utxo_deleted_staking_amount (deleted_slot=?)"
	const accountDrivenScan = "SEARCH ax USING COVERING INDEX " +
		"idx_account_drep_type_active_staking_key (drep_type=? AND active=?)"

	oldPlan := explainQueryPlan(
		t,
		writeDB,
		oldVotingPowerByTypeSQL,
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.Contains(
		t,
		oldPlan,
		utxoDrivenScan,
		"pre-fix query plan should drive its inner subquery off every "+
			"live utxo row: %s",
		oldPlan,
	)

	newPlan := explainQueryPlan(
		t,
		writeDB,
		newVotingPowerByTypeSQL(t, 0),
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.NotContains(
		t,
		newPlan,
		utxoDrivenScan,
		"fixed query plan must not scan every live utxo row: %s",
		newPlan,
	)
	require.Contains(
		t,
		newPlan,
		accountDrivenScan,
		"fixed query plan should drive its inner subquery off the "+
			"drep_type-filtered account set instead: %s",
		newPlan,
	)
}

func explainQueryPlan(
	t *testing.T,
	db *sql.DB,
	query string,
	expiryEpoch uint64,
	drepType1, drepType2 uint64,
) string {
	t.Helper()
	args := []any{}
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	rows, err := db.QueryContext(
		context.Background(),
		"EXPLAIN QUERY PLAN "+query,
		args...,
	)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var plan strings.Builder
	for rows.Next() {
		dest := make([]any, len(cols))
		scanBuf := make([]sql.NullString, len(cols))
		for i := range dest {
			dest[i] = &scanBuf[i]
		}
		require.NoError(t, rows.Scan(dest...))
		for _, c := range scanBuf {
			plan.WriteString(c.String)
			plan.WriteString(" ")
		}
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())
	return plan.String()
}
