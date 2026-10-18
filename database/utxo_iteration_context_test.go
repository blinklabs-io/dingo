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

package database

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestIterateUtxoRefsAsOfUsesCallContextWithSuppliedTransaction(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	txn := db.Transaction(context.Background(), false)
	defer txn.Release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := db.IterateUtxoRefsAsOf(ctx, 0, txn, func(*models.Utxo) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
