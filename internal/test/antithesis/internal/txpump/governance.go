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

package txpump

import (
	"errors"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// drepDeposit is the devnet dRepDeposit (Conway genesis) paid when a DRep
// registers.
const drepDeposit uint64 = 500_000_000

// drepRegCert is the CBOR representation of a DRep registration certificate.
// Conway CDDL: [16, drep_credential, coin, anchor / null]
type drepRegCert struct {
	cbor.StructAsArray
	CertType   uint32
	Credential stakeCredential
	Deposit    uint64
	Anchor     any // null for our test transactions
}

// drepUpdateCert is the CBOR representation of a DRep update certificate.
// Conway CDDL: [18, drep_credential, anchor / null]
type drepUpdateCert struct {
	cbor.StructAsArray
	CertType   uint32
	Credential stakeCredential
	Anchor     any // null for our test transactions
}

// BuildDRepRegistrationTx constructs a signed Conway transaction containing a
// DRep registration certificate: [16, [0, drepKeyHash], deposit, null].
// drepKey witnesses the credential and may be nil for keyless harness
// wallets.
func BuildDRepRegistrationTx(
	inputs []UTxO,
	drepKeyHash []byte,
	deposit uint64,
	fee uint64,
	changeAddr []byte,
	drepKey *UTxOKey,
) ([]byte, error) {
	if len(drepKeyHash) == 0 {
		return nil, errors.New("drep_reg: DRep key hash must not be empty")
	}
	cert := drepRegCert{
		CertType:   16,
		Credential: stakeCredential{Type: 0, Hash: drepKeyHash},
		Deposit:    deposit,
		Anchor:     nil,
	}
	return buildCertTx(
		"drep_reg", inputs, cert, deposit, fee, changeAddr, drepKey,
	)
}

// BuildDRepUpdateTx constructs a signed Conway transaction containing a DRep
// update certificate for an already registered DRep:
// [18, [0, drepKeyHash], null].
func BuildDRepUpdateTx(
	inputs []UTxO,
	drepKeyHash []byte,
	fee uint64,
	changeAddr []byte,
	drepKey *UTxOKey,
) ([]byte, error) {
	if len(drepKeyHash) == 0 {
		return nil, errors.New("drep_update: DRep key hash must not be empty")
	}
	cert := drepUpdateCert{
		CertType:   18,
		Credential: stakeCredential{Type: 0, Hash: drepKeyHash},
		Anchor:     nil,
	}
	return buildCertTx(
		"drep_update", inputs, cert, 0, fee, changeAddr, drepKey,
	)
}
