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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
