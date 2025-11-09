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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (ls *LedgerState) processTransactionCertificates(
	txn *database.Txn,
	blockPoint pcommon.Point,
	certs []ledger.Certificate,
	blockEraId uint,
) error {
	var tmpCert lcommon.Certificate
	for _, tmpCert = range certs {
		// Calculate certificate deposits using the block's era functions
		// This ensures we use the correct era-specific logic instead of current era
		certDeposit, err := ls.calculateCertificateDeposit(tmpCert, blockEraId)
		if err != nil {
			return fmt.Errorf("get certificate deposit: %w", err)
		}
		switch cert := tmpCert.(type) {
		case *lcommon.DeregistrationCertificate:
			err := ls.db.SetDeregistration(
				cert,
				blockPoint.Slot,
				txn,
			)
			if err != nil {
				return fmt.Errorf("failed to process dereg cert: %w", err)
			}
		case *lcommon.DeregistrationDrepCertificate:
			err := ls.db.SetDeregistrationDrep(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.UpdateDrepCertificate:
			err := ls.db.SetUpdateDrep(
				cert,
				blockPoint.Slot,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.PoolRegistrationCertificate:
			err := ls.db.SetPoolRegistration(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return fmt.Errorf("failed to process pool regis cert: %w", err)
			}
		case *lcommon.PoolRetirementCertificate:
			err := ls.db.SetPoolRetirement(
				cert,
				blockPoint.Slot,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.RegistrationCertificate:
			err := ls.db.SetRegistration(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.RegistrationDrepCertificate:
			err := ls.db.SetRegistrationDrep(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.StakeDelegationCertificate:
			err := ls.db.SetStakeDelegation(
				cert,
				blockPoint.Slot,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.StakeDeregistrationCertificate:
			err := ls.db.SetStakeDeregistration(
				cert,
				blockPoint.Slot,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.StakeRegistrationCertificate:
			err := ls.db.SetStakeRegistration(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.StakeRegistrationDelegationCertificate:
			err := ls.db.SetStakeRegistrationDelegation(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.StakeVoteDelegationCertificate:
			err := ls.db.SetStakeVoteDelegation(
				cert,
				blockPoint.Slot,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.StakeVoteRegistrationDelegationCertificate:
			err := ls.db.SetStakeVoteRegistrationDelegation(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.VoteDelegationCertificate:
			err := ls.db.SetVoteDelegation(
				cert,
				blockPoint.Slot,
				txn,
			)
			if err != nil {
				return err
			}
		case *lcommon.VoteRegistrationDelegationCertificate:
			err := ls.db.SetVoteRegistrationDelegation(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
			}
		default:
			ls.config.Logger.Warn(
				fmt.Sprintf("ignoring unsupported certificate type %T", cert),
			)
		}
	}
	return nil
}

// calculateCertificateDeposit calculates the certificate deposit using the appropriate era's
// certificate deposit function. This ensures we use the correct era-specific logic instead
// of always using the current era, which may not match the block's era for historical data.
func (ls *LedgerState) calculateCertificateDeposit(
	cert lcommon.Certificate,
	blockEraId uint,
) (uint64, error) {
	// Get the era descriptor for this block
	blockEra := eras.GetEraById(blockEraId)
	if blockEra == nil {
		return 0, fmt.Errorf("unknown era ID %d", blockEraId)
	}

	// If this era doesn't support certificates (like Byron), return 0
	if blockEra.CertDepositFunc == nil {
		return 0, nil
	}

	// Use the block era's certificate deposit function with current protocol parameters
	certDeposit, err := blockEra.CertDepositFunc(cert, ls.currentPParams)
	if err != nil {
		// Handle era type mismatch - this can happen when processing historical blocks
		// with newer protocol parameters, or when the certificate type didn't exist
		// in that era
		if errors.Is(err, eras.ErrIncompatibleProtocolParams) {
			return 0, nil
		}
		return 0, err
	}

	return certDeposit, nil
}
