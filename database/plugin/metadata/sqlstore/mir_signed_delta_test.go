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
	"database/sql"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var migratedStoreSequence atomic.Uint64

// newMigratedTestStore opens an in-memory SQLite store carrying the checked-in
// schema, so a test exercises the real column types rather than a hand-written
// approximation of them.
func newMigratedTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_mir_%d?mode=memory&cache=shared",
			migratedStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func mirTestCredential(seed byte) *lcommon.Credential {
	var hash lcommon.Blake2b224
	for i := range hash {
		hash[i] = seed
	}
	return &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
}

// TestApplyMIRCertificatePersistsProjectedDeltas proves the persistence path
// writes exactly what the certificate's reward projection returns, for every
// credential, rather than a value it re-derives from the underlying field.
// RewardsAmount is *big.Int on every gouroboros release, so this is the path a
// signed delta travels once the underlying field is widened to delta_coin.
func TestApplyMIRCertificatePersistsProjectedDeltas(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	first := mirTestCredential(0x21)
	second := mirTestCredential(0x22)
	cert := decodeMIRDistributionCertificate(
		t,
		uint(lcommon.MirSourceReserves),
		map[*lcommon.Credential]uint64{
			first:  1_200,
			second: 450,
		},
	)
	_, err := applyMIRCertificate(
		context.Background(),
		newDialectQueryer(store.writeDB, store.dialect.Name()),
		cert,
		0,
		400,
	)
	require.NoError(t, err)

	want := map[string]string{}
	for credential, amount := range cert.Reward.RewardsAmount() {
		want[string(credential.Credential[:])] = amount.String()
	}
	require.Len(t, want, 2)

	effects, err := store.GetMIRCertsInSlotRange(0, 1_000, nil)
	require.NoError(t, err)
	require.Len(t, effects, 1)
	got := map[string]string{}
	for _, reward := range effects[0].Rewards {
		require.NotNil(t, reward.Amount)
		got[string(reward.Credential)] = reward.Amount.String()
	}
	assert.Equal(t, want, got)
}

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

// decodeMIRDistributionCertificate builds a distribution MIR certificate
// through the CBOR decoder, so the test does not depend on the Go type of the
// reward map.
func decodeMIRDistributionCertificate(
	t *testing.T,
	source uint,
	rewards map[*lcommon.Credential]uint64,
) *lcommon.MoveInstantaneousRewardsCertificate {
	t.Helper()
	encoded, err := cbor.Encode(struct {
		cbor.StructAsArray
		Source  uint
		Rewards map[*lcommon.Credential]uint64
	}{
		Source:  source,
		Rewards: rewards,
	})
	require.NoError(t, err)
	cert := &lcommon.MoveInstantaneousRewardsCertificate{
		CertType: uint(lcommon.CertificateTypeMoveInstantaneousRewards),
	}
	require.NoError(t, cert.Reward.UnmarshalCBOR(encoded))
	return cert
}

// TestGetAccountSumsByCredentialSumsSignedMIRDeltas proves the reserves and
// treasury aggregate reads sum signed values. Summing them through the coin
// helper would reject the negative row outright; ignoring the sign would report
// a total larger than the account ever received.
func TestGetAccountSumsByCredentialSumsSignedMIRDeltas(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	credential := mirTestCredential(0x23).Credential[:]
	seedMIRRewardRow(t, store, 0, credential, "1000")
	seedMIRRewardRow(t, store, 0, credential, "-250")
	seedMIRRewardRow(t, store, 1, credential, "700")
	seedMIRRewardRow(t, store, 1, credential, "-900")

	sums, err := store.GetAccountSumsByCredential(0, credential, nil)
	require.NoError(t, err)
	require.NotNil(t, sums.ReservesSum)
	require.NotNil(t, sums.TreasurySum)
	assert.Equal(t, "750", sums.ReservesSum.String())
	assert.Equal(t, "-200", sums.TreasurySum.String())
}

// TestGetAccountSumsByCredentialWithoutMIRHistory pins the zero value the
// aggregate reads return when there is nothing to sum, so the signed totals are
// never handed to a caller as nil. The empty-credential case never reaches a
// query, so it is the one that depends on the returned value being initialized.
func TestGetAccountSumsByCredentialWithoutMIRHistory(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	for _, test := range []struct {
		name       string
		credential []byte
	}{
		{
			name:       "known credential with no MIR rows",
			credential: mirTestCredential(0x24).Credential[:],
		},
		{
			name:       "empty credential short-circuits the query",
			credential: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sums, err := store.GetAccountSumsByCredential(
				0,
				test.credential,
				nil,
			)
			require.NoError(t, err)
			require.NotNil(t, sums.ReservesSum)
			require.NotNil(t, sums.TreasurySum)
			assert.Equal(t, "0", sums.ReservesSum.String())
			assert.Equal(t, "0", sums.TreasurySum.String())
		})
	}
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
