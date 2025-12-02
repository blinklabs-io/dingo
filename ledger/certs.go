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
