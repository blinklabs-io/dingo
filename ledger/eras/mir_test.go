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

package eras

import (
	"errors"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mirTx implements just enough of lcommon.Transaction for
// validateMIRAccumulatedRewards: Certificates. It is deliberately not run
// through the full ValidateTxAlonzo/ValidateTxBabbage (that requires a fully
// valid transaction to clear the unrelated UTXO validation rules first,
// following the same direct-unit-test convention as
// validateDelegationConwayBootstrapAware in
// conway_bootstrap_vote_delegation_test.go); the wiring itself is a plain
// unconditional call from both, visible directly in the diff.
type mirTx struct {
	lcommon.Transaction
	certs []lcommon.Certificate
}

func (t *mirTx) Certificates() []lcommon.Certificate {
	return t.certs
}

// mirCredential returns a distinct stake key credential for the given seed
// byte, matching the 28-byte Blake2b224 width real credentials use.
func mirCredential(seed byte) lcommon.Credential {
	raw := make([]byte, 28)
	for i := range raw {
		raw[i] = seed
	}
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(raw),
	}
}

// mirCert builds a reserves-sourced distribution MIR certificate with one
// credential->delta entry. delta may be negative (delta_coin is signed).
func mirCert(cred lcommon.Credential, delta int64) *lcommon.MoveInstantaneousRewardsCertificate {
	return mirCertFromPot(cred, 0, delta)
}

// mirCertFromPot builds a distribution MIR certificate sourced from the given
// pot (0 = reserves, 1 = treasury) with one credential->delta entry.
func mirCertFromPot(
	cred lcommon.Credential,
	pot uint,
	delta int64,
) *lcommon.MoveInstantaneousRewardsCertificate {
	credCopy := cred
	return &lcommon.MoveInstantaneousRewardsCertificate{
		Reward: lcommon.MoveInstantaneousRewardsCertificateReward{
			Source: pot,
			Rewards: map[*lcommon.Credential]*big.Int{
				&credCopy: big.NewInt(delta),
			},
		},
	}
}

func mirKey(cred lcommon.Credential) MIRCredentialKey {
	return mirKeyFromPot(cred, 0)
}

func mirKeyFromPot(cred lcommon.Credential, pot uint) MIRCredentialKey {
	//nolint:gosec // test-only, CredType is always 0 or 1
	return MIRCredentialKey{
		Tag:        uint8(cred.CredType),
		Credential: cred.Credential,
		Pot:        pot,
	}
}

// TestValidateMIRAccumulatedRewards_SingleTxSequential covers +10 then -11 and
// +10 then -10 within one transaction, at Alonzo (PV6) and Babbage (PV8).
func TestValidateMIRAccumulatedRewards_SingleTxSequential(t *testing.T) {
	t.Parallel()

	for _, era := range []struct {
		name  string
		major uint
	}{
		{"Alonzo", lcommon.ProtocolVersionAlonzo},
		{"Babbage", lcommon.ProtocolVersionBabbage},
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()

			cred := mirCredential(0x01)

			t.Run("+10 then -11 rejects", func(t *testing.T) {
				t.Parallel()
				tx := &mirTx{certs: []lcommon.Certificate{
					mirCert(cred, 10),
					mirCert(cred, -11),
				}}
				ls := newMockLedgerState()
				err := validateMIRAccumulatedRewards(tx, 100, ls, era.major)
				require.Error(t, err)
				var negErr MIRProducesNegativeUpdateError
				require.True(t, errors.As(err, &negErr), "expected MIRProducesNegativeUpdateError, got %T: %v", err, err)
				assert.Equal(t, big.NewInt(10), negErr.Existing)
				assert.Equal(t, big.NewInt(-11), negErr.Delta)
			})

			t.Run("+10 then -10 stays valid", func(t *testing.T) {
				t.Parallel()
				tx := &mirTx{certs: []lcommon.Certificate{
					mirCert(cred, 10),
					mirCert(cred, -10),
				}}
				ls := newMockLedgerState()
				err := validateMIRAccumulatedRewards(tx, 100, ls, era.major)
				require.NoError(t, err)
			})

			t.Run("negative delta valid when result stays non-negative", func(t *testing.T) {
				t.Parallel()
				tx := &mirTx{certs: []lcommon.Certificate{
					mirCert(cred, 20),
					mirCert(cred, -5),
				}}
				ls := newMockLedgerState()
				err := validateMIRAccumulatedRewards(tx, 100, ls, era.major)
				require.NoError(t, err)
			})
		})
	}
}

// TestValidateMIRAccumulatedRewards_CrossTxSameEpoch covers +10 then -11 and
// +10 then -10 across separate transactions in the same epoch, simulating an
// earlier transaction's certificate already committed to the database by
// pre-populating pendingMIR (what *ledger.LedgerView.PendingMIRRewardDeltas
// would report, whether the earlier transaction was in the same block or an
// earlier block of the same epoch -- both look identical from this query).
func TestValidateMIRAccumulatedRewards_CrossTxSameEpoch(t *testing.T) {
	t.Parallel()

	for _, era := range []struct {
		name  string
		major uint
	}{
		{"Alonzo", lcommon.ProtocolVersionAlonzo},
		{"Babbage", lcommon.ProtocolVersionBabbage},
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()

			cred := mirCredential(0x02)

			t.Run("+10 then -11 rejects", func(t *testing.T) {
				t.Parallel()
				ls := newMockLedgerState()
				ls.pendingMIR = map[MIRCredentialKey]*big.Int{
					mirKey(cred): big.NewInt(10),
				}
				tx := &mirTx{certs: []lcommon.Certificate{mirCert(cred, -11)}}
				err := validateMIRAccumulatedRewards(tx, 200, ls, era.major)
				require.Error(t, err)
				var negErr MIRProducesNegativeUpdateError
				require.True(t, errors.As(err, &negErr), "expected MIRProducesNegativeUpdateError, got %T: %v", err, err)
			})

			t.Run("+10 then -10 stays valid", func(t *testing.T) {
				t.Parallel()
				ls := newMockLedgerState()
				ls.pendingMIR = map[MIRCredentialKey]*big.Int{
					mirKey(cred): big.NewInt(10),
				}
				tx := &mirTx{certs: []lcommon.Certificate{mirCert(cred, -10)}}
				err := validateMIRAccumulatedRewards(tx, 200, ls, era.major)
				require.NoError(t, err)
			})
		})
	}
}

// TestValidateMIRAccumulatedRewards_CrossPotIsolation proves a reserves
// surplus cannot offset a treasury deficit for the same credential: the
// reference tracks iRReserves and iRTreasury as two entirely separate maps,
// so a certificate drawing from one pot must never be able to fund a negative
// delta drawn from the other.
func TestValidateMIRAccumulatedRewards_CrossPotIsolation(t *testing.T) {
	t.Parallel()

	for _, era := range []struct {
		name  string
		major uint
	}{
		{"Alonzo", lcommon.ProtocolVersionAlonzo},
		{"Babbage", lcommon.ProtocolVersionBabbage},
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()

			cred := mirCredential(0x06)

			t.Run("reserves +100 then treasury -50 in one tx rejects", func(t *testing.T) {
				t.Parallel()
				tx := &mirTx{certs: []lcommon.Certificate{
					mirCertFromPot(cred, 0, 100),
					mirCertFromPot(cred, 1, -50),
				}}
				ls := newMockLedgerState()
				err := validateMIRAccumulatedRewards(tx, 100, ls, era.major)
				require.Error(t, err,
					"a reserves surplus must not offset a treasury deficit")
				var negErr MIRProducesNegativeUpdateError
				require.True(t, errors.As(err, &negErr), "expected MIRProducesNegativeUpdateError, got %T: %v", err, err)
				assert.Equal(t, uint(1), negErr.Pot)
				assert.Equal(t, big.NewInt(0), negErr.Existing)
				assert.Equal(t, big.NewInt(-50), negErr.Delta)
			})

			t.Run("reserves +100 pending, treasury -50 across txs rejects", func(t *testing.T) {
				t.Parallel()
				ls := newMockLedgerState()
				ls.pendingMIR = map[MIRCredentialKey]*big.Int{
					mirKeyFromPot(cred, 0): big.NewInt(100),
				}
				tx := &mirTx{certs: []lcommon.Certificate{
					mirCertFromPot(cred, 1, -50),
				}}
				err := validateMIRAccumulatedRewards(tx, 200, ls, era.major)
				require.Error(t, err,
					"pending reserves state must not be visible to a treasury key")
			})

			t.Run("same-pot accumulation still offsets correctly", func(t *testing.T) {
				t.Parallel()
				tx := &mirTx{certs: []lcommon.Certificate{
					mirCertFromPot(cred, 1, 100),
					mirCertFromPot(cred, 1, -50),
				}}
				ls := newMockLedgerState()
				err := validateMIRAccumulatedRewards(tx, 100, ls, era.major)
				require.NoError(t, err)
			})
		})
	}
}

// TestValidateMIRAccumulatedRewards_PreAlonzoNoOp confirms this check does not
// fire below protocol version 5: a bare negative delta there is
// MIRNegativesNotCurrentlyAllowedError's job (enforced upstream by
// gouroboros's shelley.validateMirDeltaSigns via the Shelley/Allegra/Mary
// UtxoValidateDelegation rule already wired into alonzoUtxoValidationRules'
// predecessors), not this function's.
func TestValidateMIRAccumulatedRewards_PreAlonzoNoOp(t *testing.T) {
	t.Parallel()

	cred := mirCredential(0x03)
	tx := &mirTx{certs: []lcommon.Certificate{
		mirCert(cred, 10),
		mirCert(cred, -11),
	}}
	ls := newMockLedgerState()

	for _, major := range []uint{
		lcommon.ProtocolVersionShelley,
		lcommon.ProtocolVersionAllegra,
		lcommon.ProtocolVersionMary,
	} {
		err := validateMIRAccumulatedRewards(tx, 100, ls, major)
		require.NoError(t, err, "protocol version %d must not trigger MIRProducesNegativeUpdate", major)
	}
}

// TestValidateMIRAccumulatedRewards_NoMIRCerts confirms a transaction with no
// MIR certificates is untouched.
func TestValidateMIRAccumulatedRewards_NoMIRCerts(t *testing.T) {
	t.Parallel()
	tx := &mirTx{}
	ls := newMockLedgerState()
	err := validateMIRAccumulatedRewards(tx, 100, ls, lcommon.ProtocolVersionAlonzo)
	require.NoError(t, err)
}

// noMIRProviderLedgerState implements lcommon.LedgerState (via the embedded
// nil interface) but not MIRPendingRewardsProvider, so it is never called: the
// type assertion in validateMIRAccumulatedRewards must fail closed (no-op)
// rather than panic when the ledger state doesn't expose the capability.
type noMIRProviderLedgerState struct {
	lcommon.LedgerState
}

// TestValidateMIRAccumulatedRewards_NoProvider confirms the check is a no-op,
// not a panic, when the ledger state does not implement
// MIRPendingRewardsProvider (every production *ledger.LedgerView does; this
// only guards the type assertion itself).
func TestValidateMIRAccumulatedRewards_NoProvider(t *testing.T) {
	t.Parallel()
	cred := mirCredential(0x05)
	tx := &mirTx{certs: []lcommon.Certificate{
		mirCert(cred, 10),
		mirCert(cred, -11),
	}}
	err := validateMIRAccumulatedRewards(
		tx, 100, noMIRProviderLedgerState{}, lcommon.ProtocolVersionAlonzo,
	)
	require.NoError(t, err)
}

