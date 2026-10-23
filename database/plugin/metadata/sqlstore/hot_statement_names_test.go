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
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var (
	pascalCaseName = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	sqlcNameLine   = regexp.MustCompile(`(?m)-- name: ([A-Za-z0-9_]+) :`)
)

// sqlcQueryNames returns every name carried by a sqlc-generated query in any
// dialect, read from the checked-in generated sources.
func sqlcQueryNames(t *testing.T) map[string]struct{} {
	t.Helper()
	files, err := filepath.Glob("internal/query/*/*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	names := make(map[string]struct{})
	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, match := range sqlcNameLine.FindAllSubmatch(data, -1) {
			names[string(match[1])] = struct{}{}
		}
	}
	require.NotEmpty(t, names)
	return names
}

func sqlcHotQueries() []string {
	return []string{
		sqlitequery.GetAccountByCredentialQuery,
		sqlitequery.GetActiveAccountByCredentialQuery,
		sqlitequery.SetTipQuery,
		sqlitequery.SetBlockNonceQuery,
	}
}

// TestHotStatementsCarryQueryName fails when a hand-written hot statement
// lands in the "unknown" label, when its name is not PascalCase, or when it
// reuses a sqlc-generated query's name for a different statement.
func TestHotStatementsCarryQueryName(t *testing.T) {
	t.Parallel()
	sqlc := sqlcQueryNames(t)
	for _, query := range hotStatements {
		_, name := classifySQLStatement(query)
		require.NotEqual(
			t, "unknown", name,
			"hot statement has no -- name: comment: %.120s", query,
		)
		require.Regexp(t, pascalCaseName, name)
	}
	// Hand-written statements must not reuse a sqlc name. The sqlc entries
	// in hotStatements are generated, so recognise them by the generated
	// text rather than by name.
	generated := map[string]struct{}{}
	for _, query := range sqlcHotQueries() {
		generated[query] = struct{}{}
	}
	for _, query := range hotStatements {
		if _, ok := generated[query]; ok {
			continue
		}
		_, name := classifySQLStatement(query)
		_, collides := sqlc[name]
		require.False(
			t, collides,
			"hand-written statement %q reuses a sqlc query name", name,
		)
	}
}

// TestHandWrittenStatementNamesDoNotReuseSqlcNames scans every production
// source file in this package, not only hotStatements, so an unprepared
// hand-written statement cannot share a label with a sqlc query either.
func TestHandWrittenStatementNamesDoNotReuseSqlcNames(t *testing.T) {
	t.Parallel()
	sqlc := sqlcQueryNames(t)
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	scanned := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, match := range sqlcNameLine.FindAllSubmatch(data, -1) {
			scanned++
			name := string(match[1])
			_, collides := sqlc[name]
			require.False(
				t, collides,
				"%s: hand-written statement %q reuses a sqlc query name",
				file, name,
			)
		}
	}
	require.NotZero(t, scanned)
}

// TestHotStatementsAreRecordedUnderTheirName applies a transaction that
// spends and creates UTxOs, accrues live stake and inserts a transaction,
// then reads the duration histogram: each statement must appear under its
// own name, not "unknown".
func TestHotStatementsAreRecordedUnderTheirName(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	fx := buildSharedCredentialTx(t, 0x07)
	seedConsumedUtxo(t, store, fx)
	for range 2 {
		require.NoError(t, store.SetTransaction(
			fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
		))
	}
	for _, want := range []struct{ op, name string }{
		{"insert", "InsertTransaction"},
		{"update", "ConsumeUtxoReturningStake"},
		{"select", "GetUtxoSpendState"},
		{"select", "SumCredentialUtxoStake"},
		{"insert", "UpsertRewardLiveStake"},
		{"insert", "InsertUtxoIfAbsent"},
	} {
		require.NotZero(
			t,
			histogramSampleCount(t, reg, want.op, want.name),
			"expected %s/%s to be observed", want.op, want.name,
		)
	}
}

// TestHotStatementNamesSurviveDialectTranslation checks that the leading name
// comment is still the first thing classifySQLStatement sees once PostgreSQL
// or MySQL translation has rewritten the statement.
func TestHotStatementNamesSurviveDialectTranslation(t *testing.T) {
	t.Parallel()
	for _, dialect := range []string{"postgres", "mysql"} {
		for _, query := range hotStatements {
			recorder := &recordingQueryer{}
			_, err := newDialectQueryer(recorder, dialect).ExecContext(
				context.Background(), query,
			)
			require.NoError(t, err)
			require.Len(t, recorder.statements, 1)
			_, want := classifySQLStatement(query)
			_, got := classifySQLStatement(recorder.statements[0])
			require.Equal(t, want, got, dialect)
		}
	}
}

// newlyPreparedHotStatements are the per-transaction statements that used to
// run one-shot through the queryer and are now prepared at Start.
func newlyPreparedHotStatements() map[string]string {
	return map[string]string{
		"UpdateCertificateSpecializedID":     updateCertificateSpecializedIDSQL,
		"GetAccountCertificateState":         getAccountCertificateStateSQL,
		"UpsertAccount":                      upsertAccountSQL,
		"GetAccountDrepDelegation":           getAccountDrepDelegationSQL,
		"InsertAccountWithdrawalWitness":     insertAccountWithdrawalWitnessSQL,
		"GetActiveAccountReward":             getActiveAccountRewardSQL,
		"GetAccountRewardState":              getAccountRewardStateSQL,
		"WithdrawalRewardDeltaExists":        withdrawalRewardDeltaExistsSQL,
		"UpdateAccountReward":                updateAccountRewardSQL,
		"InsertWithdrawalRewardDelta":        insertWithdrawalRewardDeltaSQL,
		"GetSpendScriptHashByOutput":         spendScriptHashQuery,
		"DeleteRedeemerScriptsByTransaction": deleteRedeemerScriptsSQL,
		"UpsertUtxoReferenceScript":          upsertUtxoReferenceScriptQuery,
		"GovernanceVoteCastExists":           governanceVoteCastExistsQuery,
	}
}

// TestNewlyPreparedHotStatementsAreCachedAtStart pins each newly prepared
// statement to hotStatements, the prepared cache after Start, and its name.
func TestNewlyPreparedHotStatementsAreCachedAtStart(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	for name, query := range newlyPreparedHotStatements() {
		require.Contains(t, hotStatements, query, name)
		_, got := classifySQLStatement(query)
		require.Equal(t, name, got)
		_, ok := store.lookupCachedStmt(query)
		require.True(t, ok, "%s not prepared by Start", name)
	}
}

// TestCertificateStatementsRunPreparedAndMeasured applies a transaction with a
// stake registration twice and requires the certificate account statements to
// reach the driver only as reuse of the Start-time statement, and to be
// observed in the duration histogram under their names.
func TestCertificateStatementsRunPreparedAndMeasured(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	mark := len(recorder.snapshot())
	fx := buildSharedCredentialTx(t, 0x09)
	seedConsumedUtxo(t, store, fx)
	for range 2 {
		require.NoError(t, store.SetTransaction(
			fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
		))
	}
	after := recorder.snapshot()[mark:]
	for _, query := range []string{
		updateCertificateSpecializedIDSQL,
		getAccountCertificateStateSQL,
		upsertAccountSQL,
	} {
		for _, executed := range after {
			require.NotEqual(
				t, query, executed,
				"statement prepared or run uncached after Start",
			)
		}
	}

	reg := prometheus.NewRegistry()
	measured := newMigratedSQLiteStoreWithRegistry(t, reg)
	fx = buildSharedCredentialTx(t, 0x0a)
	seedConsumedUtxo(t, measured, fx)
	require.NoError(t, measured.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
	))
	for _, want := range []struct{ op, name string }{
		{"update", "UpdateCertificateSpecializedID"},
		{"select", "GetAccountCertificateState"},
		{"insert", "UpsertAccount"},
	} {
		require.NotZero(
			t,
			histogramSampleCount(t, reg, want.op, want.name),
			"expected %s/%s to be observed", want.op, want.name,
		)
	}
}
