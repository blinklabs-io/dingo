// Copyright 2025 Blink Labs Software
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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLedgerState_CalculateCertificateDeposits(t *testing.T) {
	// Setup ledger state with Conway era and protocol parameters
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentPParams: &conway.ConwayProtocolParameters{
			KeyDeposit:  2000000,   // 2 ADA
			PoolDeposit: 500000000, // 500 ADA
			DRepDeposit: 500000000, // 500 ADA
		},
	}

	tests := []struct {
		name        string
		certs       []common.Certificate
		expected    map[int]uint64
		expectError bool
	}{
		{
			name:     "empty certificate list",
			certs:    []common.Certificate{},
			expected: map[int]uint64{},
		},
		{
			name: "single stake registration certificate",
			certs: []common.Certificate{
				&common.StakeRegistrationCertificate{},
			},
			expected: map[int]uint64{
				0: 2000000, // KeyDeposit
			},
		},
		{
			name: "single pool registration certificate",
			certs: []common.Certificate{
				&common.PoolRegistrationCertificate{},
			},
			expected: map[int]uint64{
				0: 500000000, // PoolDeposit
			},
		},
		{
			name: "single DRep registration certificate",
			certs: []common.Certificate{
				&common.RegistrationDrepCertificate{},
			},
			expected: map[int]uint64{
				0: 500000000, // DRepDeposit
			},
		},
		{
			name: "stake delegation certificate (zero deposit)",
			certs: []common.Certificate{
				&common.StakeDelegationCertificate{},
			},
			expected: map[int]uint64{
				0: 0,
			},
		},
		{
			name: "pool retirement certificate (zero deposit)",
			certs: []common.Certificate{
				&common.PoolRetirementCertificate{},
			},
			expected: map[int]uint64{
				0: 0,
			},
		},
		{
			name: "multiple certificates with mixed deposits",
			certs: []common.Certificate{
				&common.StakeRegistrationCertificate{},   // KeyDeposit
				&common.StakeDelegationCertificate{},     // Zero deposit
				&common.PoolRegistrationCertificate{},    // PoolDeposit
				&common.StakeDeregistrationCertificate{}, // Zero deposit
			},
			expected: map[int]uint64{
				0: 2000000,   // Stake registration
				1: 0,         // Stake delegation
				2: 500000000, // Pool registration
				3: 0,         // Stake deregistration
			},
		},
		{
			name: "stake registration delegation certificate",
			certs: []common.Certificate{
				&common.StakeRegistrationDelegationCertificate{},
			},
			expected: map[int]uint64{
				0: 2000000, // KeyDeposit
			},
		},
		{
			name: "vote registration delegation certificate",
			certs: []common.Certificate{
				&common.VoteRegistrationDelegationCertificate{},
			},
			expected: map[int]uint64{
				0: 2000000, // KeyDeposit
			},
		},
		{
			name: "stake vote registration delegation certificate",
			certs: []common.Certificate{
				&common.StakeVoteRegistrationDelegationCertificate{},
			},
			expected: map[int]uint64{
				0: 2000000, // KeyDeposit
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deposits, err := ls.CalculateCertificateDeposits(tt.certs)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, deposits)

			// Additional validation: ensure all certificate indices are covered
			assert.Len(
				t,
				deposits,
				len(tt.certs),
				"deposit map should have entry for each certificate",
			)

			// Validate that deposit amounts are reasonable (non-negative)
			for idx, deposit := range deposits {
				assert.GreaterOrEqual(
					t,
					deposit,
					uint64(0),
					"deposit for certificate %d should be non-negative",
					idx,
				)
			}
		})
	}
}

// TestCalculateCertificateDeposits_IntegrationStyle provides integration-style testing
// that validates the deposit calculation against expected protocol behavior
func TestCalculateCertificateDeposits_IntegrationStyle(t *testing.T) {
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentPParams: &conway.ConwayProtocolParameters{
			KeyDeposit:  2000000,   // 2 ADA in lovelace
			PoolDeposit: 500000000, // 500 ADA in lovelace
			DRepDeposit: 500000000, // 500 ADA in lovelace
		},
	}

	t.Run("comprehensive certificate deposit validation", func(t *testing.T) {
		// Create a comprehensive set of certificates representing real-world scenarios
		certs := []common.Certificate{
			// Registration certificates (require deposits)
			&common.StakeRegistrationCertificate{},
			&common.PoolRegistrationCertificate{},
			&common.RegistrationDrepCertificate{},

			// Delegation certificates (no deposits)
			&common.StakeDelegationCertificate{},
			&common.StakeRegistrationDelegationCertificate{},
			&common.VoteRegistrationDelegationCertificate{},
			&common.StakeVoteRegistrationDelegationCertificate{},

			// Deregistration certificates (no deposits)
			&common.StakeDeregistrationCertificate{},
			&common.PoolRetirementCertificate{},
			&common.DeregistrationDrepCertificate{},

			// Other certificates (no deposits)
			&common.AuthCommitteeHotCertificate{},
			&common.ResignCommitteeColdCertificate{},
			&common.GenesisKeyDelegationCertificate{},
		}

		deposits, err := ls.CalculateCertificateDeposits(certs)
		require.NoError(t, err)
		require.Len(t, deposits, len(certs))

		// Validate deposit requirements by certificate type
		expectedDeposits := map[int]uint64{
			0:  2000000,   // StakeRegistrationCertificate - KeyDeposit
			1:  500000000, // PoolRegistrationCertificate - PoolDeposit
			2:  500000000, // RegistrationDrepCertificate - DRepDeposit
			3:  0,         // StakeDelegationCertificate - no deposit
			4:  2000000,   // StakeRegistrationDelegationCertificate - KeyDeposit
			5:  2000000,   // VoteRegistrationDelegationCertificate - KeyDeposit
			6:  2000000,   // StakeVoteRegistrationDelegationCertificate - KeyDeposit
			7:  0,         // StakeDeregistrationCertificate - no deposit
			8:  0,         // PoolRetirementCertificate - no deposit
			9:  0,         // DeregistrationDrepCertificate - no deposit
			10: 0,         // AuthCommitteeHotCertificate - no deposit
			11: 0,         // ResignCommitteeColdCertificate - no deposit
			12: 0,         // GenesisKeyDelegationCertificate - no deposit
		}

		assert.Equal(t, expectedDeposits, deposits)

		// Validate that registration certificates have non-zero deposits
		registrationIndices := []int{
			0,
			1,
			2,
			4,
			5,
			6,
		} // Indices of registration-type certs
		for _, idx := range registrationIndices {
			assert.Greater(
				t,
				deposits[idx],
				uint64(0),
				"registration certificate at index %d should require a deposit",
				idx,
			)
		}

		// Validate that non-registration certificates have zero deposits
		nonRegistrationIndices := []int{
			3,
			7,
			8,
			9,
			10,
			11,
			12,
		} // Indices of non-registration certs
		for _, idx := range nonRegistrationIndices {
			assert.Equal(
				t,
				uint64(0),
				deposits[idx],
				"non-registration certificate at index %d should not require a deposit",
				idx,
			)
		}
	})

	t.Run(
		"deposit calculation with zero protocol parameters",
		func(t *testing.T) {
			// Test edge case where protocol parameters are zero
			lsZeroDeposits := &LedgerState{
				currentEra: eras.ConwayEraDesc,
				currentPParams: &conway.ConwayProtocolParameters{
					KeyDeposit:  0,
					PoolDeposit: 0,
					DRepDeposit: 0,
				},
			}

			certs := []common.Certificate{
				&common.StakeRegistrationCertificate{},
				&common.PoolRegistrationCertificate{},
				&common.RegistrationDrepCertificate{},
			}

			deposits, err := lsZeroDeposits.CalculateCertificateDeposits(certs)
			require.NoError(t, err)

			// All deposits should be zero when protocol parameters are zero
			expected := map[int]uint64{0: 0, 1: 0, 2: 0}
			assert.Equal(t, expected, deposits)
		},
	)
}
