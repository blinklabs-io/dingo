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
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// CalculateCertificateDeposits calculates deposit amounts for certificates that require deposits.
// Returns a map of certificate index to deposit amount (zero for non-deposit types, required entries for all certificates).
//
// Certificate types that require deposits (Conway era):
//   - PoolRegistrationCertificate: PoolDeposit
//   - StakeRegistrationCertificate: KeyDeposit
//   - RegistrationCertificate: KeyDeposit
//   - RegistrationDrepCertificate: DRepDeposit
//   - StakeRegistrationDelegationCertificate: KeyDeposit
//   - VoteRegistrationDelegationCertificate: KeyDeposit
//   - StakeVoteRegistrationDelegationCertificate: KeyDeposit
//
// All other certificate types (delegations, deregistrations, updates, etc.) have zero deposits.
// This should be called by the ledger layer before persisting transactions with certificates.
func (ls *LedgerState) CalculateCertificateDeposits(
	certs []lcommon.Certificate,
) (map[int]uint64, error) {
	deposits := make(map[int]uint64, len(certs))

	for idx, cert := range certs {
		// Calculate proper deposit using protocol parameters
		certDeposit, err := ls.currentEra.CertDepositFunc(
			cert,
			ls.currentPParams,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"calculate deposit for certificate at index %d: %w",
				idx,
				err,
			)
		}
		// Store the deposit amount (including zero values)
		deposits[idx] = certDeposit
	}

	return deposits, nil
}
