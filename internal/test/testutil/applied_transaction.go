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

package testutil

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// Execer is the part of *sql.DB, *sql.Tx and *sql.Conn the transaction
// fixtures write through.
type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// Queryer is an Execer that can also read a row.
type Queryer interface {
	Execer
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// AppliedTransaction is the row pair of a transaction applied to the chain: an
// identity row in "transaction" and its application row in
// applied_transaction. A zero ID takes the next row id. Fee and CollateralFee
// default to "0" and Valid to true when left empty.
type AppliedTransaction struct {
	ID            int64
	Hash          []byte
	BlockHash     []byte
	Metadata      []byte
	Slot          int64
	Type          int64
	Fee           string
	CollateralFee string
	TTL           string
	BlockIndex    int64
	Invalid       bool
}

// InsertAppliedTransaction writes an identity row and its application row and
// returns the transaction's row id.
func InsertAppliedTransaction(
	tb testing.TB,
	db Execer,
	tx AppliedTransaction,
) int64 {
	tb.Helper()
	id := InsertTransactionIdentity(tb, db, tx.ID, tx.Hash)
	fee, collateralFee, ttl := tx.Fee, tx.CollateralFee, tx.TTL
	if fee == "" {
		fee = "0"
	}
	if collateralFee == "" {
		collateralFee = "0"
	}
	if ttl == "" {
		ttl = "0"
	}
	_, err := db.ExecContext(
		context.Background(),
		`INSERT INTO applied_transaction (
    tx_id, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, tx.BlockHash, tx.Metadata, tx.Slot, tx.Type, fee, collateralFee,
		ttl, tx.BlockIndex, !tx.Invalid,
	)
	if err != nil {
		tb.Fatalf("insert applied transaction: %v", err)
	}
	return id
}

// InsertTransactionIdentity writes the identity row of a transaction known
// only by hash and returns its row id. A zero id takes the next row id.
func InsertTransactionIdentity(
	tb testing.TB,
	db Execer,
	id int64,
	hash []byte,
) int64 {
	tb.Helper()
	var (
		result sql.Result
		err    error
	)
	if id == 0 {
		result, err = db.ExecContext(
			context.Background(),
			`INSERT INTO "transaction" (hash) VALUES (?)`, hash,
		)
	} else {
		result, err = db.ExecContext(
			context.Background(),
			`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`, id, hash,
		)
	}
	if err != nil {
		tb.Fatalf("insert transaction identity: %v", err)
	}
	if id != 0 {
		return id
	}
	id, err = result.LastInsertId()
	if err != nil {
		tb.Fatalf("read transaction identity id: %v", err)
	}
	return id
}

// TransactionID returns the row id of the identity row of hash, inserting the
// row when the hash is not known yet. It is how a fixture names the producer
// or spender of an output it inserts directly.
func TransactionID(tb testing.TB, db Queryer, hash []byte) int64 {
	tb.Helper()
	var id int64
	err := db.QueryRowContext(
		context.Background(),
		`SELECT id FROM "transaction" WHERE hash = ?`, hash,
	).Scan(&id)
	if err == nil {
		return id
	}
	if err != sql.ErrNoRows {
		tb.Fatalf("look up transaction identity: %v", err)
	}
	return InsertTransactionIdentity(tb, db, 0, hash)
}

// TransactionIDs resolves transaction hashes to row ids for a fixture that
// inserts many outputs directly, creating each identity row on first use and
// remembering it.
type TransactionIDs struct {
	tb  testing.TB
	db  Queryer
	ids map[string]int64
}

// NewTransactionIDs returns a resolver that reads and writes through db.
func NewTransactionIDs(tb testing.TB, db Queryer) *TransactionIDs {
	tb.Helper()
	return &TransactionIDs{tb: tb, db: db, ids: make(map[string]int64)}
}

// ID returns the row id of hash.
func (r *TransactionIDs) ID(hash []byte) int64 {
	r.tb.Helper()
	if id, ok := r.ids[string(hash)]; ok {
		return id
	}
	id := TransactionID(r.tb, r.db, hash)
	r.ids[string(hash)] = id
	return id
}

// ExecTransactionInsert runs a fixture statement written against the layout
// the transaction table had before it became an identity table: an
// INSERT INTO "transaction" (columns) VALUES (values) whose columns mix hash
// and id with the per-application ones. It splits the statement into the
// identity insert and the application insert and returns the result of the
// identity insert, so its LastInsertId is the transaction's row id. A
// statement with no hash column gets a random one. Values are either ?
// placeholders or literals without top-level commas.
func ExecTransactionInsert(
	db Execer,
	query string,
	args ...any,
) (sql.Result, error) {
	onConflict := ""
	if loc := onConflictPattern.FindStringIndex(query); loc != nil {
		query, onConflict = query[:loc[0]], " ON CONFLICT DO NOTHING"
	}
	columns, values, err := parseTransactionInsert(query)
	if err != nil {
		return nil, err
	}
	next := 0
	take := func(value string) (string, []any) {
		if value == "?" {
			arg := args[next]
			next++
			return "?", []any{arg}
		}
		return value, nil
	}
	var identityCols, identityVals []string
	var identityArgs []any
	var appliedCols, appliedVals []string
	var appliedArgs []any
	const unsetHash = "randomblob(32)"
	hashToken := unsetHash
	var hashArgs []any
	for i, column := range columns {
		token, tokenArgs := take(values[i])
		switch column {
		case "id":
			identityCols = append(identityCols, "id")
			identityVals = append(identityVals, token)
			identityArgs = append(identityArgs, tokenArgs...)
		case "hash":
			hashToken, hashArgs = token, tokenArgs
		default:
			appliedCols = append(appliedCols, column)
			appliedVals = append(appliedVals, token)
			appliedArgs = append(appliedArgs, tokenArgs...)
		}
	}
	if hashToken == unsetHash {
		// A fixture without a hash needs one that stays stable between the
		// two statements.
		hashToken = "?"
		hashArgs = []any{randomHash()}
	}
	identityCols = append(identityCols, "hash")
	identityVals = append(identityVals, hashToken)
	identityArgs = append(identityArgs, hashArgs...)
	result, err := db.ExecContext(
		context.Background(),
		`INSERT INTO "transaction" (`+strings.Join(identityCols, ", ")+
			`) VALUES (`+strings.Join(identityVals, ", ")+`)`+onConflict,
		identityArgs...,
	)
	if err != nil {
		return nil, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	if onConflict != "" {
		if affected, _ := result.RowsAffected(); affected == 0 {
			queryer, ok := db.(Queryer)
			if !ok {
				return nil, errors.New("transaction insert conflict needs a Queryer")
			}
			if err := queryer.QueryRowContext(
				context.Background(),
				`SELECT id FROM "transaction" WHERE hash = ?`, hashArgs...,
			).Scan(&id); err != nil {
				return nil, err
			}
		}
	}
	appliedCols = append([]string{"tx_id"}, appliedCols...)
	appliedVals = append([]string{"?"}, appliedVals...)
	appliedArgs = append([]any{id}, appliedArgs...)
	_, err = db.ExecContext(
		context.Background(),
		`INSERT INTO applied_transaction (`+strings.Join(appliedCols, ", ")+
			`) VALUES (`+strings.Join(appliedVals, ", ")+`)`+onConflict,
		appliedArgs...,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

var onConflictPattern = regexp.MustCompile(`(?i)\sON\s+CONFLICT`)

func randomHash() []byte {
	hash := make([]byte, 32)
	if _, err := rand.Read(hash); err != nil {
		panic(err)
	}
	return hash
}

func parseTransactionInsert(query string) ([]string, []string, error) {
	open := strings.Index(query, "(")
	closeCols := strings.Index(query, ")")
	if open < 0 || closeCols < open {
		return nil, nil, fmt.Errorf("unsupported transaction insert: %s", query)
	}
	var columns []string
	for _, column := range strings.Split(query[open+1:closeCols], ",") {
		columns = append(columns, strings.Trim(strings.TrimSpace(column), "`\""))
	}
	rest := query[closeCols+1:]
	vopen := strings.Index(rest, "(")
	vclose := strings.LastIndex(rest, ")")
	if vopen < 0 || vclose < vopen {
		return nil, nil, fmt.Errorf("unsupported transaction insert: %s", query)
	}
	var values []string
	depth, start := 0, vopen+1
	inQuote := false
	for i := vopen + 1; i <= vclose; i++ {
		switch rest[i] {
		case '\'':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote && depth > 0 {
				depth--
				continue
			}
			if !inQuote && i == vclose {
				values = append(values, strings.TrimSpace(rest[start:i]))
			}
		case ',':
			if !inQuote && depth == 0 {
				values = append(values, strings.TrimSpace(rest[start:i]))
				start = i + 1
			}
		}
	}
	if len(values) != len(columns) {
		return nil, nil, fmt.Errorf(
			"transaction insert has %d columns and %d values: %s",
			len(columns), len(values), query,
		)
	}
	return columns, values, nil
}
