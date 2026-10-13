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
	"fmt"
	"testing"

	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// stakeCredential returns a distinct 28-byte key-hash stake credential for
// use in a StakeRegistrationCertificate/StakeDelegationCertificate.
func stakeCredential(seed byte) lcommon.Credential {
	key := make([]byte, 28)
	key[0] = seed
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(key),
	}
}

// TestGetStakeByPoolsAtSlotExcludesDelegationStaleAfterPoolReap pins the fix
// for a bug caught live via node-parity on Preview (epoch 647): a pool that
// retired and later re-registered had a one-time delegator's pre-reap
// delegation certificate wrongly resurrected by activeDelegationSQL's
// certificate-only reconstruction, because it had no equivalent to
// poolReapedAfterDelegation's guard on the rollback path (account.go). The
// credential never re-delegated after the reap, so real cardano-ledger's
// POOLREAP transition (ClearDelegationsToRetiredPool) permanently removed
// the delegation at the reap boundary and it must not count toward the
// pool's stake or delegator count at any later slot.
func TestGetStakeByPoolsAtSlotExcludesDelegationStaleAfterPoolReap(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)

	pool := depositHeldPoolKey(0xc1)
	credential := stakeCredential(0xc2)

	// Register the pool, register a stake credential, and delegate it to the
	// pool -- all in epoch 0.
	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 0)
	writeDepositHeldCert(t, store, 110, 0, &lcommon.StakeRegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: credential,
	}, 0)
	writeDepositHeldCert(t, store, 120, 0, &lcommon.StakeDelegationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDelegation),
		StakeCredential: &credential,
		PoolKeyHash:     pool,
	}, 0)

	// The pool retires at the boundary into epoch 1 (slot 1_000, per
	// depositHeldEpochLength), then re-registers inside epoch 1, after the
	// reap. The credential never re-delegates.
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(pool, 1), 0)
	writeDepositHeldCert(t, store, 1_500, 0, depositHeldRegistration(pool), 0)

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool.Bytes()}, 3_000, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(0),
		delegators[string(pool.Bytes())],
		"a delegation certificate predating the pool's reap must not be "+
			"revived by the pool's later re-registration",
	)
	require.Equal(t, uint64(0), stakes[string(pool.Bytes())])
}

// TestGetStakeByPoolsAtSlotCountsDelegationAfterExplicitRedelegation is the
// positive control for the reap guard: a credential that re-delegates to the
// pool after it reaps must count normally, proving the guard excludes only
// the stale pre-reap certificate and not the pool itself.
func TestGetStakeByPoolsAtSlotCountsDelegationAfterExplicitRedelegation(
	t *testing.T,
) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)

	pool := depositHeldPoolKey(0xc3)
	credential := stakeCredential(0xc4)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 0)
	writeDepositHeldCert(t, store, 110, 0, &lcommon.StakeRegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: credential,
	}, 0)
	writeDepositHeldCert(t, store, 120, 0, &lcommon.StakeDelegationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDelegation),
		StakeCredential: &credential,
		PoolKeyHash:     pool,
	}, 0)
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(pool, 1), 0)
	writeDepositHeldCert(t, store, 1_500, 0, depositHeldRegistration(pool), 0)
	// Explicit re-delegation after the reap and re-registration.
	writeDepositHeldCert(t, store, 1_600, 0, &lcommon.StakeDelegationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDelegation),
		StakeCredential: &credential,
		PoolKeyHash:     pool,
	}, 0)

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool.Bytes()}, 3_000, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), delegators[string(pool.Bytes())])
	_ = stakes
}

func TestEpochBoundaryActivePoolsUseEraSpecificReap(t *testing.T) {
	t.Parallel()
	for _, era := range []uint{6, 7} {
		t.Run(fmt.Sprintf("era_%d", era), func(t *testing.T) {
			store := newDepositHeldStore(t)
			depositHeldEpochs(t, store, 2)
			require.NoError(t, store.SetEpoch(1_000, 1, nil, nil, nil, nil,
				era, 1, depositHeldEpochLength, nil))
			pool := depositHeldPoolKey(0xd1)
			writeDepositHeldCert(t, store, 100, 0,
				depositHeldRegistration(pool), 0)
			writeDepositHeldCert(t, store, 200, 0,
				depositHeldRetirement(pool, 1), 0)
			ordinary, err := store.GetActivePoolKeyHashesAtSlot(999, nil)
			require.NoError(t, err)
			require.Equal(t, [][]byte{pool.Bytes()}, ordinary)
			boundary, err := store.GetEpochBoundaryActivePoolKeyHashes(
				999, 1_000, nil)
			require.NoError(t, err)
			if era == 7 {
				require.Empty(t, boundary)
			} else {
				require.Equal(t, ordinary, boundary)
			}
		})
	}
}

func TestBoundarySnapshotUsesEnactedDijkstraProtocolVersion(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 2)
	params := mockledger.NewMockConwayProtocolParams()
	params.ProtocolVersion.Major = 12
	raw, err := gcbor.Encode(&params)
	require.NoError(t, err)
	require.NoError(t, store.SetPParams(raw, 1_000, 1, 6, nil))
	after, err := boundarySnapshotAfterEnactment(
		t.Context(), store.writeDB, 1_000)
	require.NoError(t, err)
	require.True(t, after, "SNAP precedes incoming era translation")
}

func TestHistoricalPoolEventsOrderClosureBeforeCertifier(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 2)
	pool := depositHeldPoolKey(0xd2)
	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 0)
	writeDepositHeldCert(t, store, 400, 1, depositHeldRetirement(pool, 0), 0)
	_, err := store.writeDB.Exec(`INSERT INTO leios_transaction_context
		(transaction_id, slot) SELECT id, 200 FROM "transaction"
		WHERE slot = 400 AND block_index = 1`)
	require.NoError(t, err)
	writeDepositHeldCert(t, store, 400, 0, depositHeldRegistration(pool), 0)
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(400, make([]byte, 32)),
	}, nil))
	for _, ordered := range []bool{false, true} {
		var pools [][]byte
		if ordered {
			pools, err = store.GetActivePoolKeyHashesOrdered(nil)
		} else {
			pools, err = store.GetActivePoolKeyHashesAtSlot(400, nil)
		}
		require.NoError(t, err)
		require.Equal(t, [][]byte{pool.Bytes()}, pools,
			"ranking registration cancels earlier closure retirement")
	}
}

func TestTransactionLedgerContextCanBeReplayed(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 1)
	writeDepositHeldCert(t, store, 100, 0,
		depositHeldRegistration(depositHeldPoolKey(0xd3)), 0)
	var id int64
	require.NoError(t, store.writeDB.QueryRow(`SELECT id FROM "transaction"`).Scan(&id))
	require.NoError(t, recordTransactionLedgerContext(t.Context(), store.writeDB, id, 99))
	require.NoError(t, recordTransactionLedgerContext(t.Context(), store.writeDB, id, 99))
}

func TestHistoricalExpirationUsesClosureExecutionSlot(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 3)
	pool := depositHeldPoolKey(0xd4)
	credential := stakeCredential(0xd5)
	writeDepositHeldCert(t, store, 50, 0, depositHeldRegistration(pool), 0)
	writeDepositHeldCert(t, store, 100, 0,
		&lcommon.StakeRegistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: credential,
		}, 0)
	writeDepositHeldCert(t, store, 2_000, 0,
		&lcommon.StakeDelegationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeDelegation),
			StakeCredential: &credential, PoolKeyHash: pool,
		}, 0)
	_, err := store.writeDB.Exec(`INSERT INTO leios_transaction_context
		(transaction_id, slot) SELECT id, 1999 FROM "transaction"
		WHERE slot = 2000`)
	require.NoError(t, err)
	query, args := activeDelegationSQL(1_999, 1_999)
	expiration, expirationArgs, err := historicalExpirationSQL(
		t.Context(), store.writeDB, 1_999, 2, 1)
	require.NoError(t, err)
	query += expiration + ` SELECT expiration_epoch FROM historical_expiration`
	args = append(args, expirationArgs...)
	var expiry uint64
	require.NoError(t, store.writeDB.QueryRow(query, args...).Scan(&expiry))
	require.Equal(t, uint64(2), expiry,
		"the epoch-1 closure refreshes activity before epoch 2")
}
