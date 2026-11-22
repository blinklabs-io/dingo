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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWriteTransactionStatementContextNotCancellable requires a write
// transaction's statements to carry a context without a Done channel, so a
// driver that watches cancellable contexts does not start a watcher per
// statement, while a store call after cancellation still fails.
func TestWriteTransactionStatementContextNotCancellable(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	txn := store.Transaction(ctx)
	t.Cleanup(func() { _ = txn.Rollback() })

	_, stmtCtx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	require.Nil(t, stmtCtx.Done(), "write statements must not be cancellable")

	cancel()
	_, _, err = store.dbFromTxn(txn)
	require.ErrorIs(t, err, context.Canceled)
}

// TestReadTransactionStatementContextKeepsCancellation requires read-only
// transactions to keep interrupting statements on cancellation.
func TestReadTransactionStatementContextKeepsCancellation(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	txn := store.ReadTransaction(ctx)
	t.Cleanup(func() { _ = txn.Rollback() })

	_, stmtCtx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	require.NotNil(t, stmtCtx.Done())
	cancel()
	require.ErrorIs(t, stmtCtx.Err(), context.Canceled)
}
