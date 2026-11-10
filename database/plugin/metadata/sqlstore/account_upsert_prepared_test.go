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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCertificateAccountRowUpdatesRunPrepared updates an existing account
// once for every combination of changed pool, drep, drep type and active, and
// requires none of the account upserts to reach the driver after Start: each
// changed-column variant must be served by the statement Start prepared, not
// prepared and finalized again per call.
func TestCertificateAccountRowUpdatesRunPrepared(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	ctx := context.Background()
	stored := certificateAccountStored{
		active: true,
		pool:   []byte("pool-a"),
		drep:   []byte("drep-a"),
	}
	for changed := range 16 {
		key := []byte(fmt.Sprintf("account-%d", changed))
		_, err := store.writeDB.ExecContext(ctx, `
INSERT INTO account (staking_key, credential_tag, pool, drep, added_slot,
  created_slot, reward, drep_type, active, expiration_epoch)
VALUES (?, 0, ?, ?, 10, 10, '0', 0, ?, 0)`,
			key, stored.pool, stored.drep, stored.active,
		)
		require.NoError(t, err)
		next := stored
		if changed&1 != 0 {
			next.pool = []byte("pool-b")
		}
		if changed&2 != 0 {
			next.drep = []byte("drep-b")
		}
		if changed&4 != 0 {
			next.active = false
		}
		if changed&8 != 0 {
			next.drepType = 1
		}
		mark := len(recorder.snapshot())
		require.NoError(t, store.updateCertificateAccountRow(
			ctx, store.writeDB, 0, key, 20, stored, next,
		))
		for _, executed := range recorder.snapshot()[mark:] {
			require.NotContains(
				t, executed, "-- name: UpsertAccount ",
				"changed=%04b: account upsert prepared or run uncached "+
					"after Start:\n%s", changed,
				strings.TrimSpace(executed),
			)
		}
	}
}
