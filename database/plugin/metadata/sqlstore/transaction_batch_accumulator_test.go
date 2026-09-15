package sqlstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
