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

	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// calculateCertificateDeposit calculates the certificate deposit using the appropriate era's
// certificate deposit function. This ensures we use the correct era-specific logic instead
// of always using the current era, which may not match the block's era for historical data.
//
// A nil return reports that the deposit is *unknown*, which is not the same
// answer as a deposit of zero. The recorded value is what a later legacy stake
// deregistration is refunded at: gouroboros'
// UtxoValidateValueNotConservedUtxo reads it through
// lcommon.StakeCredentialDepositState and treats any non-nil value as
// authoritative, falling back to the current KeyDeposit only when the state
// reports absence. Reporting zero for an uncomputable deposit therefore
// refunds zero and fails value conservation on an otherwise valid
// transaction, whereas reporting absence falls back to KeyDeposit.
//
// A genuinely computed zero stays zero and must not be folded into the
// unknown case: config/cardano/devnet/shelley-genesis.json sets
// "keyDeposit": 0, so every stake registration on dingo's own devnet records
// an authoritative zero deposit.
func (ls *LedgerState) calculateCertificateDeposit(
	cert lcommon.Certificate,
	blockEraId uint,
	pparams lcommon.ProtocolParameters,
) (*uint64, error) {
	// Get the era descriptor for this block
	blockEra := eras.GetEraById(blockEraId)
	if blockEra == nil {
		return nil, fmt.Errorf("unknown era ID %d", blockEraId)
	}

	// If this era doesn't support certificates (like Byron), no deposit can be
	// computed. Report absence rather than zero.
	if blockEra.CertDepositFunc == nil {
		return nil, nil
	}

	certDeposit, err := blockEra.CertDepositFunc(cert, pparams)
	if err != nil {
		// Handle era type mismatch - this can happen when processing historical blocks
		// with newer protocol parameters, or when the certificate type didn't exist
		// in that era. The deposit is unknown, not zero.
		if errors.Is(err, eras.ErrIncompatibleProtocolParams) {
			return nil, nil
		}
		return nil, err
	}

	return &certDeposit, nil
}
