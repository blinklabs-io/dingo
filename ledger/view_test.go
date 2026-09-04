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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"bytes"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLedgerViewUnimplementedMethodsReturnSentinelError(t *testing.T) {
	lv := &LedgerView{}

	rewards, err := lv.CalculateRewards(
		lcommon.AdaPots{},
		lcommon.RewardSnapshot{},
		lcommon.RewardParameters{},
	)
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Nil(t, rewards)

	snapshot, err := lv.GetRewardSnapshot(0)
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Equal(t, lcommon.RewardSnapshot{}, snapshot)

	adaPots, err := lv.GetAdaPotsWithError()
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Equal(t, lcommon.AdaPots{}, adaPots)
	require.PanicsWithValue(t, ErrNotImplemented, func() {
		_ = lv.GetAdaPots()
	})

	err = lv.UpdateAdaPots(lcommon.AdaPots{})
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestLedgerViewRewardAccountBalance(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	key := bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize)
	for _, tag := range []uint8{0, 1} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey:    key,
			CredentialTag: tag,
			Reward:        types.Uint64(100 + uint64(tag)),
			Active:        true,
		}))
	}
	inactive := bytes.Repeat([]byte{0xa2}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: inactive,
		Reward:     55,
		Active:     false,
	}))
	lv := &LedgerView{
		ls: &LedgerState{
			db: db,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		},
	}
	credential := func(tag uint, value []byte) lcommon.Credential {
		return lcommon.Credential{
			CredType:   tag,
			Credential: lcommon.NewBlake2b224(value),
		}
	}
	for _, tc := range []struct {
		name string
		cred lcommon.Credential
		want *uint64
	}{
		{name: "key credential", cred: credential(0, key), want: new(uint64(100))},
		{name: "script credential", cred: credential(1, key), want: new(uint64(101))},
		{name: "missing credential", cred: credential(0, bytes.Repeat([]byte{0xa3}, 28))},
		{name: "inactive credential", cred: credential(0, inactive)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lv.RewardAccountBalance(tc.cred)
			require.NoError(t, err)
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, *tc.want, *got)
		})
	}
	zero := bytes.Repeat([]byte{0xa4}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: zero,
		Reward:     0,
		Active:     true,
	}))
	got, err := lv.RewardAccountBalance(credential(0, zero))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Zero(t, *got)

	_, err = lv.RewardAccountBalance(lcommon.Credential{CredType: 2})
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported stake credential tag")

	require.NoError(t, dbtest.CloseDatabase(db))
	_, err = lv.RewardAccountBalance(credential(0, key))
	require.Error(t, err)
}

func TestLedgerViewStakeCredentialDeposit(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	sharedHash := bytes.Repeat([]byte{0xb1}, lcommon.AddressHashSize)
	credential := func(tag uint, value []byte) lcommon.Credential {
		return lcommon.Credential{
			CredType:   tag,
			Credential: lcommon.NewBlake2b224(value),
		}
	}
	keyCredential := credential(lcommon.CredentialTypeAddrKeyHash, sharedHash)
	scriptCredential := credential(lcommon.CredentialTypeScriptHash, sharedHash)
	zeroCredential := credential(
		lcommon.CredentialTypeAddrKeyHash,
		bytes.Repeat([]byte{0xb6}, lcommon.AddressHashSize),
	)
	importedKeyCredential := credential(
		lcommon.CredentialTypeAddrKeyHash,
		bytes.Repeat([]byte{0xb8}, lcommon.AddressHashSize),
	)
	importedScriptCredential := credential(
		lcommon.CredentialTypeScriptHash,
		bytes.Repeat([]byte{0xb9}, lcommon.AddressHashSize),
	)
	importedThenRegisteredCredential := credential(
		lcommon.CredentialTypeAddrKeyHash,
		bytes.Repeat([]byte{0xbc}, lcommon.AddressHashSize),
	)
	persistViewStakeRegistration(t, db, keyCredential, 2_000_000, 100, 0xb2)
	persistViewStakeRegistration(t, db, scriptCredential, 3_000_000, 101, 0xb3)
	persistViewStakeRegistration(t, db, zeroCredential, 0, 102, 0xb7)
	persistViewStakeRegistration(
		t,
		db,
		importedKeyCredential,
		1_000_000,
		90,
		0xba,
	)
	persistViewImportedStakeAccount(
		t,
		db,
		importedKeyCredential,
		4_000_000,
		103,
	)
	persistViewImportedStakeAccount(
		t,
		db,
		importedScriptCredential,
		5_000_000,
		104,
	)
	persistViewImportedStakeAccount(
		t,
		db,
		importedThenRegisteredCredential,
		4_500_000,
		105,
	)
	persistViewStakeRegistration(
		t,
		db,
		importedThenRegisteredCredential,
		6_000_000,
		106,
		0xbd,
	)

	inactiveHash := bytes.Repeat([]byte{0xb4}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    inactiveHash,
		CredentialTag: 0,
		Active:        false,
	}))
	lv := &LedgerView{
		ls: &LedgerState{
			db: db,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		},
	}

	for _, tc := range []struct {
		name string
		cred lcommon.Credential
		want *uint64
	}{
		{name: "key credential", cred: keyCredential, want: new(uint64(2_000_000))},
		{name: "script credential", cred: scriptCredential, want: new(uint64(3_000_000))},
		{name: "zero deposit", cred: zeroCredential, want: new(uint64(0))},
		{
			name: "imported key credential",
			cred: importedKeyCredential,
			want: new(uint64(4_000_000)),
		},
		{
			name: "imported script credential",
			cred: importedScriptCredential,
			want: new(uint64(5_000_000)),
		},
		{
			name: "newer registration supersedes import",
			cred: importedThenRegisteredCredential,
			want: new(uint64(6_000_000)),
		},
		{
			name: "missing credential",
			cred: credential(
				lcommon.CredentialTypeAddrKeyHash,
				bytes.Repeat([]byte{0xb5}, lcommon.AddressHashSize),
			),
		},
		{
			name: "inactive credential",
			cred: credential(lcommon.CredentialTypeAddrKeyHash, inactiveHash),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lv.StakeCredentialDeposit(tc.cred)
			require.NoError(t, err)
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, *tc.want, *got)
		})
	}

	importHistory, err := db.GetAccountRegistrationHistoryByCredential(
		1,
		importedScriptCredential.Credential[:],
		10,
		0,
		"desc",
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, importHistory)

	require.NoError(t, db.DeleteCertificatesAfterSlot(105, nil))
	require.NoError(t, db.RestoreAccountStateAtSlot(105, nil))
	depositAfterRollback, err := lv.StakeCredentialDeposit(
		importedThenRegisteredCredential,
	)
	require.NoError(t, err)
	require.NotNil(t, depositAfterRollback)
	require.Equal(t, uint64(4_500_000), *depositAfterRollback)

	_, err = lv.StakeCredentialDeposit(lcommon.Credential{CredType: 2})
	require.ErrorContains(t, err, "unsupported stake credential tag")

	require.NoError(t, dbtest.CloseDatabase(db))
	_, err = lv.StakeCredentialDeposit(keyCredential)
	require.Error(t, err)
}

func persistViewImportedStakeAccount(
	t *testing.T,
	db *database.Database,
	credential lcommon.Credential,
	deposit uint64,
	slot uint64,
) {
	t.Helper()
	credentialTag, err := models.CredentialTagFromUint(credential.CredType)
	require.NoError(t, err)
	importDeposit := types.Uint64(deposit)
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    credential.Credential[:],
		CredentialTag: credentialTag,
		AddedSlot:     slot,
		Active:        true,
		ImportDeposit: &importDeposit,
	}, nil))
}

func persistViewStakeRegistration(
	t *testing.T,
	db *database.Database,
	credential lcommon.Credential,
	deposit uint64,
	slot uint64,
	seed byte,
) {
	t.Helper()
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(bytes.Repeat([]byte{seed}, 32))
	builder.WithValid(true)
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{seed + 1}, 32),
		0,
	)
	require.NoError(t, err)
	builder.WithInputs(input)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(
			"addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd",
		).
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	builder.WithOutputs(output)
	builder.WithCertificates(&lcommon.StakeRegistrationCertificate{
		StakeCredential: credential,
	})
	tx, err := builder.Build()
	require.NoError(t, err)
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		ocommon.NewPoint(slot, bytes.Repeat([]byte{seed + 2}, 32)),
		0,
		map[int]uint64{0: deposit},
		nil,
	))
}

// TestLedgerViewPoolCurrentStatePendingRetirement proves PoolCurrentState's
// pending-retirement epoch tracks the pool's latest retirement certificate
// by insertion order (AddedSlot), not the maximum epoch value across every
// retirement row on the pool: a later retirement certificate replaces the
// prior schedule even when it targets an earlier epoch, and a later pool
// registration cancels a pending retirement entirely -- mirroring
// poolIsActive's ordering rule in
// internal/test/conformance/state_provider.go, which this adapter's own
// review caught duplicating this same defect from.
func TestLedgerViewPoolCurrentStatePendingRetirement(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	lv := &LedgerView{
		ls: &LedgerState{
			db: db,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		},
	}

	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xb1}, 28)),
	)

	applyCert := func(slot uint64, txIDSeed byte, cert lcommon.Certificate) {
		input, err := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
			0,
		)
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		txBuilder := mockledger.NewTransactionBuilder()
		txBuilder.WithId(bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size))
		txBuilder.WithType(gledger.TxTypeDijkstra)
		txBuilder.WithValid(true)
		txBuilder.WithInputs(input)
		txBuilder.WithOutputs(output)
		txBuilder.WithCertificates(cert)
		tx, err := txBuilder.Build()
		require.NoError(t, err)
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		}
		require.NoError(
			t,
			db.SetTransactionMetadataOnly(
				tx, point, 0, map[int]uint64{0: 500_000_000}, nil,
			),
		)
	}
	registrationCert := func(seed byte) *lcommon.PoolRegistrationCertificate {
		return &lcommon.PoolRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypePoolRegistration),
			Operator: poolKeyHash,
			VrfKeyHash: lcommon.VrfKeyHash(
				lcommon.NewBlake2b256([]byte{seed, 0x02}),
			),
			Pledge: 1_000_000,
			Cost:   340_000_000,
			Margin: cbor.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount: lcommon.AddrKeyHash(
				lcommon.NewBlake2b224([]byte{seed, 0x03}),
			),
		}
	}
	retirementCert := func(epoch uint64) *lcommon.PoolRetirementCertificate {
		return &lcommon.PoolRetirementCertificate{
			CertType:    uint(lcommon.CertificateTypePoolRetirement),
			PoolKeyHash: poolKeyHash,
			Epoch:       epoch,
		}
	}

	applyCert(1, 0x01, registrationCert(0x01))

	// A retirement targeting epoch 10, then a later retirement targeting an
	// EARLIER epoch (5): the later certificate must win regardless of its
	// epoch value being smaller than the one it replaces.
	applyCert(2, 0x02, retirementCert(10))
	applyCert(3, 0x03, retirementCert(5))

	_, pendingEpoch, err := lv.PoolCurrentState(poolKeyHash)
	require.NoError(t, err)
	require.NotNil(t, pendingEpoch)
	require.Equal(
		t,
		uint64(5),
		*pendingEpoch,
		"a later retirement certificate must replace the prior schedule even when it moves the target epoch earlier",
	)

	// A later re-registration cancels the pending retirement entirely.
	applyCert(4, 0x04, registrationCert(0x04))

	_, pendingEpoch, err = lv.PoolCurrentState(poolKeyHash)
	require.NoError(t, err)
	require.Nil(
		t,
		pendingEpoch,
		"a later pool registration must cancel a pending retirement",
	)

	require.NoError(t, dbtest.CloseDatabase(db))
}

func TestLedgerViewSkipPhase2Validation(t *testing.T) {
	lv := &LedgerView{}
	require.False(t, lv.SkipPhase2Validation())

	lv.skipPhase2Validation = true
	require.True(t, lv.SkipPhase2Validation())
}

// TestLedgerViewMinPoolMargin guards the CIP-23 bridge: MinPoolMargin() must
// forward from the LedgerState embedded via the named ls field, since Go does
// not promote methods across a named (non-embedded) field. Without this
// forwarding method, ls.(eras.MinPoolMarginProvider) in ledger/eras always fails
// for the *LedgerView actually passed to ValidateTx*, silently disabling the
// pool-margin-floor certificate rule.
func TestLedgerViewMinPoolMargin(t *testing.T) {
	ls := &LedgerState{}
	lv := &LedgerView{ls: ls}
	require.Nil(t, lv.MinPoolMargin())

	ls.config.MinPoolMargin = 150
	require.Zero(t, big.NewRat(150, 10_000).Cmp(lv.MinPoolMargin()))
}

func TestExtractCostModelsFromPParams_Nil(t *testing.T) {
	result := extractCostModelsFromPParams(nil)
	require.Empty(t, result)
}

func TestExtractCostModelsFromPParams_Alonzo(t *testing.T) {
	pp := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 1)
	_, ok := result[lcommon.PlutusLanguage(1)]
	assert.True(t, ok, "expected PlutusV1 cost model")
}

func TestExtractCostModelsFromPParams_Babbage(t *testing.T) {
	pp := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
			1: {400, 500, 600},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 2)
	_, hasV1 := result[lcommon.PlutusLanguage(1)]
	_, hasV2 := result[lcommon.PlutusLanguage(2)]
	assert.True(t, hasV1, "expected PlutusV1 cost model")
	assert.True(t, hasV2, "expected PlutusV2 cost model")
}

func TestExtractCostModelsFromPParams_Conway(t *testing.T) {
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
			1: {400, 500, 600},
			2: {700, 800, 900},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 3)
	_, hasV1 := result[lcommon.PlutusLanguage(1)]
	_, hasV2 := result[lcommon.PlutusLanguage(2)]
	_, hasV3 := result[lcommon.PlutusLanguage(3)]
	assert.True(t, hasV1, "expected PlutusV1 cost model")
	assert.True(t, hasV2, "expected PlutusV2 cost model")
	assert.True(t, hasV3, "expected PlutusV3 cost model")
}

func TestExtractCostModelsFromPParams_NilCostModels(t *testing.T) {
	pp := &babbage.BabbageProtocolParameters{
		CostModels: nil,
	}
	result := extractCostModelsFromPParams(pp)
	require.Empty(t, result)
}

func TestExtractCostModelsFromPParams_SkipsUnknownVersions(
	t *testing.T,
) {
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100},
			1: {200},
			2: {300},
			3: {400}, // unknown version, should be skipped
			9: {500}, // unknown version, should be skipped
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 3,
		"should only include versions 0-2")
}

func TestCostModels_WithCurrentPParams(t *testing.T) {
	ls := &LedgerState{
		currentPParams: &conway.ConwayProtocolParameters{
			CostModels: map[uint][]int64{
				0: {1, 2, 3},
				1: {4, 5, 6},
				2: {7, 8, 9},
			},
		},
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}
	result := lv.CostModels()
	require.Len(t, result, 3)
}

func TestCostModels_NilPParams(t *testing.T) {
	ls := &LedgerState{
		currentPParams: nil,
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}
	result := lv.CostModels()
	require.NotNil(t, result,
		"should return empty map, not nil")
	require.Empty(t, result)
}

func TestIsCommitteeThresholdMet(t *testing.T) {
	tests := []struct {
		name                 string
		yesVotes             int
		totalActiveMembers   int
		thresholdNumerator   uint64
		thresholdDenominator uint64
		expected             bool
	}{
		{
			name:                 "no committee - threshold trivially met",
			yesVotes:             0,
			totalActiveMembers:   0,
			thresholdNumerator:   2,
			thresholdDenominator: 3,
			expected:             true,
		},
		{
			name:                 "zero threshold - always met",
			yesVotes:             0,
			totalActiveMembers:   5,
			thresholdNumerator:   0,
			thresholdDenominator: 1,
			expected:             true,
		},
		{
			name:                 "zero denominator - not met",
			yesVotes:             5,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 0,
			expected:             false,
		},
		{
			name:                 "2/3 threshold met exactly",
			yesVotes:             4,
			totalActiveMembers:   6,
			thresholdNumerator:   2,
			thresholdDenominator: 3,
			expected:             true,
		},
		{
			name:                 "2/3 threshold not met",
			yesVotes:             3,
			totalActiveMembers:   6,
			thresholdNumerator:   2,
			thresholdDenominator: 3,
			expected:             false,
		},
		{
			name:                 "simple majority met",
			yesVotes:             3,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 2,
			expected:             true,
		},
		{
			name:                 "simple majority not met",
			yesVotes:             2,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 2,
			expected:             false,
		},
		{
			name:                 "unanimous met",
			yesVotes:             5,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 1,
			expected:             true,
		},
		{
			name:                 "unanimous not met",
			yesVotes:             4,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 1,
			expected:             false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsCommitteeThresholdMet(
				tc.yesVotes,
				tc.totalActiveMembers,
				tc.thresholdNumerator,
				tc.thresholdDenominator,
			)
			assert.Equal(t, tc.expected, result)
		})
	}
}
