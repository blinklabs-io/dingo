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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCertificateAccountRowWritesOnlyChangedDrepIndex changes either drep or
// drep_type of an existing account and requires the WAL to carry no frame of
// the index over the column that kept its value: SQLite rewrites the entry of
// every index over a column named in the SET list, changed or not.
func TestCertificateAccountRowWritesOnlyChangedDrepIndex(t *testing.T) {
	t.Parallel()
	const (
		drepIndex     = "idx_account_drep_active_staking_key"
		drepTypeIndex = "idx_account_drep_type_active_staking_key"
	)
	tests := []struct {
		name      string
		next      func(certificateAccountStored) certificateAccountStored
		written   string
		untouched string
	}{
		{
			name: "drep only",
			next: func(s certificateAccountStored) certificateAccountStored {
				s.drep = []byte("drep-b")
				return s
			},
			written:   drepIndex,
			untouched: drepTypeIndex,
		},
		{
			name: "drep type only",
			next: func(s certificateAccountStored) certificateAccountStored {
				s.drepType = 1
				return s
			},
			written:   drepTypeIndex,
			untouched: drepIndex,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, path := newWALSQLiteStore(t)
			ctx := context.Background()
			stored := certificateAccountStored{
				active: true,
				pool:   []byte("pool-a"),
				drep:   []byte("drep-a"),
			}
			key := []byte("account")
			_, err := store.writeDB.ExecContext(ctx, `
INSERT INTO account (staking_key, credential_tag, pool, drep, added_slot,
  created_slot, reward, drep_type, active, expiration_epoch)
VALUES (?, 0, ?, ?, 10, 10, '0', ?, ?, 0)`,
				key, stored.pool, stored.drep, stored.drepType, stored.active,
			)
			require.NoError(t, err)
			start := walFrameCount(t, path)
			require.NoError(t, store.updateCertificateAccountRow(
				ctx, store.writeDB, 0, key, 20, stored, tc.next(stored),
			))
			frames := stakeWALProfile(
				t, store.writeDB, path, start, walFrameCount(t, path),
			)
			profile := formatFrames(tc.name, frames, 0)
			require.Positive(t, frames[tc.written], profile)
			require.Zero(t, frames[tc.untouched], fmt.Sprintf(
				"%s rewritten although its column kept its value\n%s",
				tc.untouched, profile,
			))
		})
	}
}
