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
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// bareRefPair is an output named by a placeholder pair: the hash would
	// have to be bound where the column now holds the producer's row id.
	bareRefPair = regexp.MustCompile(
		`\b((?:[A-Za-z_]+\.)?)tx_id = \? AND (?:[A-Za-z_]+\.)?output_idx = \?`,
	)
	bareRefIn = regexp.MustCompile(`\b((?:[A-Za-z_]+\.)?)tx_id IN \(\?`)
	// removedUtxoSchema names what the transaction identity migration
	// removed from the utxo table and its edge tables.
	removedUtxoSchema = regexp.MustCompile(
		`\b(producer_tx_id|spent_by_tx_id|referenced_by_tx_id|` +
			`collateral_by_tx_id|idx_utxo_producer_output|` +
			`idx_utxo_imported_ref|idx_utxo_transaction_id|tx_id_output_idx|` +
			`utxo_reference_input|utxo_collateral_input)\b`,
	)
)

// utxoStatements collects the text of every statement of the SQLite utxo
// statement sets.
func utxoStatements() map[string]string {
	statements := map[string]string{
		"consumeUtxoSQLiteReturningSQL": consumeUtxoSQLiteReturningSQL,
	}
	v := reflect.ValueOf(*sqliteUtxoSQL)
	for i := range v.NumField() {
		if v.Field(i).Kind() == reflect.String && v.Field(i).String() != "" {
			statements["utxoSQL."+v.Type().Field(i).Name] = v.Field(i).String()
		}
	}
	v = reflect.ValueOf(*sqliteRefs)
	for i := range v.NumField() {
		name := v.Type().Field(i).Name
		switch field := v.Field(i); field.Kind() {
		case reflect.Map:
			for _, key := range field.MapKeys() {
				value := field.MapIndex(key)
				if value.Kind() == reflect.String {
					statements[fmt.Sprintf("%s[%v]", name, key)] = value.String()
					continue
				}
				for _, inner := range value.MapKeys() {
					statements[fmt.Sprintf("%s[%v][%v]", name, key, inner)] = value.MapIndex(inner).String()
				}
			}
		case reflect.Array:
			for n := range field.Len() {
				for _, key := range field.Index(n).MapKeys() {
					statements[fmt.Sprintf("%s[%d][%v]", name, n, key)] = field.Index(n).MapIndex(key).String()
				}
			}
		}
	}
	return statements
}

// TestUtxoStatementsResolveHashesThroughTheTransactionTable requires every
// statement of the utxo statement sets that names an output by hash to
// resolve the hash to the producer's row id first, and none to leave the
// hash for a bare tx_id comparison, which would compare a blob with an id.
func TestUtxoStatementsResolveHashesThroughTheTransactionTable(t *testing.T) {
	t.Parallel()
	statements := utxoStatements()
	require.Greater(t, len(statements), 100)
	for name, text := range statements {
		require.NotRegexp(t, bareRefPair, text, "%s: %s", name, text)
		require.NotRegexp(t, bareRefIn, text, "%s: %s", name, text)
		require.NotRegexp(t, removedUtxoSchema, text, "%s: %s", name, text)
	}
}

// TestNoSourceReferencesRemovedUtxoSchema scans the non-test sources of the
// tree for columns, indexes and tables the transaction identity migration
// removed.
func TestNoSourceReferencesRemovedUtxoSchema(t *testing.T) {
	t.Parallel()
	scanned := 0
	for _, path := range productionSources(t) {
		if strings.Contains(filepath.ToSlash(path), "/migrations/") {
			continue
		}
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		scanned++
		for i, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") ||
				strings.HasPrefix(trimmed, "--") {
				continue
			}
			if m := removedUtxoSchema.FindString(line); m != "" {
				t.Errorf("%s:%d: refers to removed schema %q: %s",
					path, i+1, m, trimmed)
			}
		}
	}
	require.Greater(t, scanned, 500, "the scan must cover the tree")
}

var (
	// transactionAlias captures the alias a statement gives the transaction
	// identity table; an unaliased reference is its own name.
	transactionAlias = regexp.MustCompile(
		`(?:FROM|JOIN)\s+(?:"transaction"|` + "`transaction`" +
			`)(?:\s+AS)?\s+([A-Za-z_][A-Za-z_0-9]*)\b`,
	)
	sqlKeywords = map[string]bool{
		"ON": true, "WHERE": true, "LEFT": true, "JOIN": true, "CROSS": true,
		"INNER": true, "ORDER": true, "GROUP": true, "UNION": true,
		"LIMIT": true, "SET": true, "VALUES": true, "AS": true, "AND": true,
		"OR": true, "USING": true, "WINDOW": true, "FOR": true,
	}
	appliedColumns = `(?:slot|block_index|block_hash|fee|collateral_fee|` +
		`valid|metadata|ttl|type)`
)

// TestNoSourceReadsAppliedColumnsFromTransaction fails when a statement
// reads a column of applied_transaction through an alias of the transaction
// identity table. A transaction known only as the producer of an imported
// output has an identity row and no application row, so such a read would
// either fail to prepare or, through a LEFT JOIN, surface an unapplied
// transaction. Readers that mean an applied transaction join
// applied_transaction.
func TestNoSourceReadsAppliedColumnsFromTransaction(t *testing.T) {
	t.Parallel()
	scanned := 0
	for _, path := range productionSources(t) {
		if strings.Contains(filepath.ToSlash(path), "/migrations/") {
			continue
		}
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		scanned++
		lines := strings.Split(string(raw), "\n")
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") ||
				strings.HasPrefix(trimmed, "--") {
				continue
			}
			aliases := map[string]bool{`"transaction"`: false}
			if m := transactionAlias.FindStringSubmatch(line); m != nil &&
				!sqlKeywords[strings.ToUpper(m[1])] {
				aliases[m[1]] = true
			}
			for alias, aliased := range aliases {
				if !aliased && !strings.Contains(line, `"transaction".`) {
					continue
				}
				pattern := regexp.MustCompile(
					`\b` + regexp.QuoteMeta(alias) + `\.` + appliedColumns + `\b`,
				)
				// The alias may be used a few lines below its join, in the
				// same statement.
				for j := i; j < min(i+40, len(lines)); j++ {
					if j > i && strings.Contains(lines[j], "`)") {
						break
					}
					if pattern.MatchString(lines[j]) {
						t.Errorf(
							"%s:%d: reads %s through the transaction "+
								"identity table; join applied_transaction: %s",
							path, j+1, alias, strings.TrimSpace(lines[j]),
						)
					}
				}
			}
		}
	}
	require.Greater(t, scanned, 500, "the scan must cover the tree")
}

// unaliasedTransaction matches a read of the identity table that takes no
// alias; the columns that statement names are then unqualified.
var (
	unaliasedTransaction = regexp.MustCompile(
		`FROM\s+(?:"transaction"|` + "`transaction`" +
			`)\s*(?:$|\)|WHERE\b|ORDER\b|LIMIT\b|GROUP\b)`,
	)
	unqualifiedApplied = regexp.MustCompile(
		`(?:^|[^.\w])(?:slot|block_index|block_hash|fee|valid)\b`,
	)
)

// TestNoSourceReadsUnqualifiedAppliedColumnsFromTransaction covers the
// statements that read the identity table without an alias, where the
// applied columns carry no qualifier for the alias scan to match.
func TestNoSourceReadsUnqualifiedAppliedColumnsFromTransaction(t *testing.T) {
	t.Parallel()
	for _, path := range productionSources(t) {
		if strings.Contains(filepath.ToSlash(path), "/migrations/") {
			continue
		}
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		lines := strings.Split(string(raw), "\n")
		for i, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "//") ||
				!unaliasedTransaction.MatchString(line) {
				continue
			}
			// The statement is the raw string around the read: back to the
			// line that opens it and forward to the line that closes it.
			first := i
			for first > 0 && !strings.Contains(lines[first], "`") {
				first--
			}
			last := i
			for last < len(lines)-1 && (last == i || !strings.Contains(lines[last], "`")) &&
				!strings.HasSuffix(strings.TrimSpace(lines[last]), "`") {
				last++
			}
			if strings.Contains(lines[i], "SELECT") {
				first, last = i, i
			}
			for j := first; j <= last; j++ {
				if unqualifiedApplied.MatchString(lines[j]) {
					t.Errorf(
						"%s:%d: names an applied column in a statement that "+
							"reads the transaction identity table: %s",
						path, j+1, strings.TrimSpace(lines[j]),
					)
				}
			}
		}
	}
}

// productionSources lists the non-test Go and SQL sources of api, database
// and internal.
func productionSources(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..", "..", "..")
	var paths []string
	for _, dir := range []string{"api", "database", "internal"} {
		err := filepath.WalkDir(
			filepath.Join(root, dir),
			func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				name := d.Name()
				if d.IsDir() || strings.HasSuffix(name, "_test.go") ||
					(!strings.HasSuffix(name, ".go") &&
						!strings.HasSuffix(name, ".sql")) {
					return nil
				}
				if strings.Contains(filepath.ToSlash(path), "/internal/query/") {
					return nil
				}
				paths = append(paths, path)
				return nil
			},
		)
		require.NoError(t, err)
	}
	return paths
}

// TestHashJoinStatementsKeepCrossJoinToSQLite requires the statements that
// name outputs by producer hash to fix the loop order with CROSS JOIN on
// SQLite only: PostgreSQL rejects ON after CROSS JOIN.
func TestHashJoinStatementsKeepCrossJoinToSQLite(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 4} {
		for name, query := range map[string][2]string{
			"stake_consumed_delta": {
				utxoStakeConsumedDeltaQuery(n, true),
				utxoStakeConsumedDeltaQuery(n, false),
			},
			"stake_refs": {
				utxoStakeRefsByTxIDQuery(n, true),
				utxoStakeRefsByTxIDQuery(n, false),
			},
			"deletion_rows": {
				utxoDeletionRowsQuery(n, false, true),
				utxoDeletionRowsQuery(n, true, false),
			},
		} {
			require.Contains(t, query[0], "CROSS JOIN", name)
			require.NotContains(t, query[1], "CROSS JOIN", name)
			require.Contains(t, query[1], `JOIN utxo AS u ON u.tx_id = t.id`, name)
		}
	}
}
