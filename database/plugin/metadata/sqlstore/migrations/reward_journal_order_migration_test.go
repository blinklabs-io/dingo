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

package migrations_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// The legacy journal's row order is its application order. A migrated event
// credit written after a withdrawal of the same account and slot is marked
// after_withdrawal; one written before it, or at another slot, is not. Batches
// of one legacy row put the withdrawal and the later credit in different
// transactions.
func TestRewardJournalMigrationKeepsSameSlotOrder(t *testing.T) {
	t.Parallel()
	for _, batchSize := range []int{0, 1} {
		t.Run(fmt.Sprintf("batch size %d", batchSize), func(t *testing.T) {
			t.Parallel()
			db, runTo := migratedPrefixDB(t, rewardJournalVersion-1)
			key := bytes.Repeat([]byte{0x5a}, 28)
			other := bytes.Repeat([]byte{0x5b}, 28)
			for _, k := range [][]byte{key, other} {
				execAll(t, db, fmt.Sprintf(`INSERT INTO account
(staking_key, credential_tag, added_slot, created_slot, active, reward)
VALUES (%s, 0, 1, 1, TRUE, '0')`, hexBlob(k)))
			}
			withdrawal := bytes.Repeat([]byte{0xf1}, 32)
			execTransactionInsert(t, db, fmt.Sprintf(
				`INSERT INTO "transaction" (hash, slot, block_index) VALUES (%s, 900, 0)`,
				hexBlob(withdrawal),
			))
			row := func(who, hash []byte, amount string, withdrawn bool) {
				previous := "NULL"
				if withdrawn {
					previous = "'24'"
				}
				execAll(t, db, fmt.Sprintf(`INSERT INTO account_reward_delta
(staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot,
 withdrawal, post_snapshot) VALUES (%s, 0, %s, '%s', %s, 900, %v, FALSE)`,
					hexBlob(who), hexBlob(hash), amount, previous, withdrawn))
			}
			row(key, bytes.Repeat([]byte{0xf2}, 32), "20", false)
			row(other, bytes.Repeat([]byte{0xf3}, 32), "5", false)
			row(key, withdrawal, "24", true)
			row(key, withdrawal, "30", false)
			row(other, bytes.Repeat([]byte{0xf4}, 32), "6", false)

			registry, err := migrations.SQLiteRegistry()
			require.NoError(t, err)
			if batchSize > 0 {
				registry = append([]migrations.Migration(nil), registry...)
				registry[rewardJournalVersion-1].BatchSize = batchSize
			}
			runTo(registry)

			require.Equal(t, 4, count(t, db,
				`SELECT COUNT(*) FROM reward_event_credit`))
			require.Equal(t, 1, count(t, db, fmt.Sprintf(
				`SELECT COUNT(*) FROM reward_event_credit e
JOIN account a ON a.id = e.account_id
WHERE e.after_withdrawal AND a.staking_key = %s AND e.amount = '30'`,
				hexBlob(key),
			)))
			require.Equal(t, 1, count(t, db,
				`SELECT COUNT(*) FROM reward_event_credit WHERE after_withdrawal`))
		})
	}
}

// A direct deposit's source is its transaction's hash, 32 bytes like a
// stake-reward source. At an epoch's first slot it stays an event credit, so
// its order against the transaction's withdrawal survives the migration.
func TestRewardJournalMigrationKeepsDepositAtEpochStart(t *testing.T) {
	t.Parallel()
	db, runTo := migratedPrefixDB(t, rewardJournalVersion-1)
	key := bytes.Repeat([]byte{0x5c}, 28)
	execAll(t, db,
		fmt.Sprintf(`INSERT INTO account
(staking_key, credential_tag, added_slot, created_slot, active, reward)
VALUES (%s, 0, 1, 1, TRUE, '0')`, hexBlob(key)),
		`INSERT INTO epoch (epoch_id, start_slot) VALUES (9, 900)`,
	)
	withdrawal := bytes.Repeat([]byte{0xf5}, 32)
	execTransactionInsert(t, db, fmt.Sprintf(
		`INSERT INTO "transaction" (hash, slot, block_index) VALUES (%s, 900, 0)`,
		hexBlob(withdrawal),
	))
	execAll(t, db,
		fmt.Sprintf(`INSERT INTO account_reward_delta
(staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot,
 withdrawal, post_snapshot)
VALUES (%s, 0, %s, '11', NULL, 900, FALSE, FALSE),
       (%s, 0, %s, '24', '35', 900, TRUE, FALSE),
       (%s, 0, %s, '30', NULL, 900, FALSE, FALSE)`,
			hexBlob(key), hexBlob(bytes.Repeat([]byte{0xf6}, 32)),
			hexBlob(key), hexBlob(withdrawal),
			hexBlob(key), hexBlob(withdrawal)),
	)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)

	require.Equal(t, 1, count(t, db, `SELECT COUNT(*) FROM reward_credit`),
		"the derived-source credit is a stake-reward credit")
	require.Equal(t, 1, count(t, db, `SELECT COUNT(*) FROM reward_event_credit
WHERE amount = '30' AND after_withdrawal`))
}
