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
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestMIRRewardDeltaColumnRoundTripsSigned proves the reward amount column and
// its encoder and decoder carry a sign. A MIR reward is delta_coin, so a
// negative delta has to read back as the value that was written rather than
// being refused by the coin encoder or rejected by the unsigned parser.
//
// The certificate type gouroboros currently exposes cannot hold a negative
// delta, so this exercises the persistence encoding directly; the end-to-end
// certificate path is covered by
// TestApplyMIRCertificatePersistsProjectedDeltas.
func TestMIRRewardDeltaColumnRoundTripsSigned(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	credential := mirTestCredential(0x25).Credential[:]
	for _, delta := range []*big.Int{
		big.NewInt(-450),
		big.NewInt(1_200),
		new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 70)),
	} {
		encoded, err := signedDecimal("MIR reward delta", delta)
		require.NoError(t, err)
		seedMIRRewardRow(t, store, 0, credential, encoded)
	}

	effects, err := store.GetMIRCertsInSlotRange(0, 1_000, nil)
	require.NoError(t, err)
	require.Len(t, effects, 3)
	got := []string{}
	for _, effect := range effects {
		require.Len(t, effect.Rewards, 1)
		require.NotNil(t, effect.Rewards[0].Amount)
		got = append(got, effect.Rewards[0].Amount.String())
	}
	assert.Equal(
		t,
		[]string{"-450", "1200", "-1180591620717411303424"},
		got,
	)
}

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
