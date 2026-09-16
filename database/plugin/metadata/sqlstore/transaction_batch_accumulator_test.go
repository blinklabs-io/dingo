package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakePrepareOnlyQueryer implements queryer with a PrepareContext that
// always fails, so insertTransaction returns before touching the *sql.Stmt
// it would otherwise store -- this test only needs to observe the dialect
// flag insertTransaction sets before calling PrepareContext, not to execute
// a real statement.
type fakePrepareOnlyQueryer struct {
	prepareErr error
}

func (fakePrepareOnlyQueryer) ExecContext(
	context.Context,
	string,
	...any,
) (sql.Result, error) {
	return nil, errors.New("fakePrepareOnlyQueryer: ExecContext not implemented")
}

func (fakePrepareOnlyQueryer) QueryContext(
	context.Context,
	string,
	...any,
) (*sql.Rows, error) {
	return nil, errors.New("fakePrepareOnlyQueryer: QueryContext not implemented")
}

func (fakePrepareOnlyQueryer) QueryRowContext(
	context.Context,
	string,
	...any,
) *sql.Row {
	return nil
}

func (f fakePrepareOnlyQueryer) PrepareContext(
	context.Context,
	string,
) (*sql.Stmt, error) {
	return nil, f.prepareErr
}

// TestInsertTransactionDetectsMySQLThroughCountingQueryer is the regression
// test for the type assertion insertTransaction uses to detect a MySQL
// dialect: db.(dialectQueryer) alone misses a dialectQueryer wrapped in
// countingQueryer, which is exactly what every real caller passes once
// Config.PromRegistry is set (see Store.instrumentedQueryer). Without
// unwrapDialectQueryer, a.mysql stays false on a metrics-enabled MySQL
// store, and insertTransaction takes the RETURNING-id QueryRowContext path
// MySQL cannot serve instead of the ExecContext/LastInsertId path this test
// proves gets selected.
func TestInsertTransactionDetectsMySQLThroughCountingQueryer(t *testing.T) {
	t.Parallel()
	prepareErr := errors.New("prepare not needed for this assertion")
	inner := dialectQueryer{
		queryer: fakePrepareOnlyQueryer{prepareErr: prepareErr},
		dialect: "mysql",
	}
	wrapped := countingQueryer{queryer: inner, counter: nil}

	acc := &transactionBatchAccumulator{}
	_, err := acc.insertTransaction(context.Background(), wrapped)
	require.ErrorIs(t, err, prepareErr)
	require.True(
		t,
		acc.mysql,
		"expected insertTransaction to detect the mysql dialect through countingQueryer",
	)
}

func TestTransactionBatchAccumulatorResetClosesStatement(t *testing.T) {
	store := newMigratedSQLiteStore(t)
	txn := store.Transaction(context.Background())
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)

	acc := &transactionBatchAccumulator{}
	oldStmtArgs := []any{
		[]byte{0x01}, []byte{0x02}, nil, 1, 0,
		"0", "0", "0", 0, true,
	}
	_, err = acc.insertTransaction(ctx, db, oldStmtArgs...)
	require.NoError(t, err)
	stmt := acc.transactionInsert
	require.NotNil(t, stmt)

	acc.Reset()
	require.Nil(t, acc.transactionInsert)
	_, err = stmt.ExecContext(ctx, oldStmtArgs...)
	require.Error(t, err, "reset must close statements bound to the batch transaction")

	require.NoError(t, txn.Rollback())
	var count int
	require.NoError(t, store.readDB.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		[]byte{0x01},
	).Scan(&count))
	require.Zero(t, count, "rollback must discard writes made through the accumulator")
}
