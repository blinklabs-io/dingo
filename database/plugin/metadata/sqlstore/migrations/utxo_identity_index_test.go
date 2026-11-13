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

package migrations

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/stretchr/testify/require"
)

// The identity rebuild recreates indexes the deferred manifest also builds.
// A deferred rebuild skips an index that already exists, so a shape the
// rebuild gets wrong is never corrected afterwards.
func TestUtxoIdentityIndexesMatchDeferredShape(t *testing.T) {
	t.Parallel()
	byName := make(map[string]deferred.Index)
	for _, index := range append(
		append([]deferred.Index(nil), deferred.Manifest...),
		deferred.Retained...,
	) {
		byName[index.Name] = index
	}
	matched := 0
	for _, index := range utxoIdentityIndexes {
		want, ok := byName[index.name]
		if !ok {
			continue
		}
		matched++
		require.Equal(t, want.Table, index.table, index.name)
		require.Equal(t, want.Columns, index.columns, index.name)
		require.Equal(t, want.Where, index.where, index.name)
		require.Equal(t, want.MySQLColumns, index.mysqlColumns, index.name)
	}
	require.NotZero(t, matched)
}

func TestUtxoIdentitySpentStakingIndexSQL(t *testing.T) {
	t.Parallel()
	var index utxoIdentityIndex
	for _, candidate := range utxoIdentityIndexes {
		if candidate.name == "idx_utxo_deleted_staking_amount" {
			index = candidate
		}
	}
	require.Equal(t,
		`CREATE INDEX IF NOT EXISTS "idx_utxo_deleted_staking_amount" ON "utxo" `+
			`("deleted_slot", "credential_tag", "staking_key", "amount") `+
			`WHERE deleted_slot > 0`,
		index.createSQL("sqlite"))
	require.Equal(t,
		"CREATE INDEX `idx_utxo_deleted_staking_amount` ON `utxo` (`deleted_slot`)",
		index.createSQL("mysql"))
}
