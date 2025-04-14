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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (ls *LedgerState) processTransactionCertificates(
	txn *database.Txn,
	blockPoint pcommon.Point,
	certs []ledger.Certificate,
) error {
	var tmpCert lcommon.Certificate
	for _, tmpCert = range certs {
		certDeposit, err := ls.currentEra.CertDepositFunc(
			tmpCert,
			ls.currentPParams,
		)
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
				return err
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
		case *lcommon.PoolRegistrationCertificate:
			err := ls.db.SetPoolRegistration(
				cert,
				blockPoint.Slot,
				certDeposit,
				txn,
			)
			if err != nil {
				return err
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
