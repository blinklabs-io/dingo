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
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestSignedDecimalRejectsMissingDelta pins that a missing delta is reported
// rather than written as zero, so a certificate that cannot be represented
// fails at the boundary that cannot represent it.
func TestSignedDecimalRejectsMissingDelta(t *testing.T) {
	t.Parallel()
	_, err := signedDecimal("MIR reward delta", nil)
	require.ErrorContains(t, err, "MIR reward delta")
}

func seedMIRRewardRow(
	t *testing.T,
	store *Store,
	pot uint,
	credential []byte,
	amount string,
) {
	t.Helper()
	var mirID int64
	require.NoError(t, store.writeDB.QueryRow(`
INSERT INTO move_instantaneous_rewards (pot, certificate_id, added_slot, other_pot)
VALUES (?, 0, 100, '0')
RETURNING id`,
		pot,
	).Scan(&mirID))
	_, err := store.writeDB.Exec(`
INSERT INTO move_instantaneous_rewards_reward (
    credential, credential_tag, amount, mir_id
) VALUES (?, 0, ?, ?)`,
		credential,
		amount,
		mirID,
	)
	require.NoError(t, err)
}
