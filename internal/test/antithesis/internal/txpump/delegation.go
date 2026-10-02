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
	"encoding/hex"
	"fmt"
	"math"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

// stakeKeyDeposit is the devnet keyDeposit (Shelley genesis protocolParams)
// paid when a stake credential is registered.
const stakeKeyDeposit uint64 = 2_000_000

// stakeCredential is a [credType, hash] pair used inside certificates.
// credType 0 = key hash.
type stakeCredential struct {
	cbor.StructAsArray
	Type uint32
	Hash []byte
}

// delegCert is the CBOR representation of a stake-delegation certificate.
// Conway CDDL: [2, stake_credential, pool_keyhash]
type delegCert struct {
	cbor.StructAsArray
	CertType   uint32
	Credential stakeCredential
	PoolHash   []byte
}

// stakeRegDelegCert registers a stake credential and delegates it in one
// certificate. Conway CDDL: [11, stake_credential, pool_keyhash, coin]
type stakeRegDelegCert struct {
	cbor.StructAsArray
	CertType   uint32
	Credential stakeCredential
	PoolHash   []byte
	Deposit    uint64
}

// txBodyWithCerts is a Conway transaction body carrying certificates.
//
// Key 0 = inputs
// Key 1 = outputs
// Key 2 = fee
// Key 4 = certificates
type txBodyWithCerts struct {
	Inputs  cbor.Set       `cbor:"0,keyasint"`
	Outputs []txBodyOutput `cbor:"1,keyasint"`
	Fee     uint64         `cbor:"2,keyasint"`
	Certs   []any          `cbor:"4,keyasint"`
}

// conwayTxWithCerts is the top-level Conway transaction carrying certificates.
type conwayTxWithCerts struct {
	cbor.StructAsArray
	Body    txBodyWithCerts
	Witness map[any]any
	IsValid bool
	AuxData any
}

// credentialFor returns the key-hash credential txpump uses for certificates
// funded by inputs, and the key that witnesses it. The first input's payment
// key doubles as the stake and DRep credential, so txpump can sign every
// certificate it submits. Keyless harness inputs fall back to a hash derived
// from the input transaction and an unsigned certificate.
func credentialFor(inputs []UTxO) ([]byte, *UTxOKey) {
	if len(inputs) == 0 {
		return nil, nil
	}
	if key := inputs[0].SigningKey; key.canSign() {
		return common.Blake2b224Hash(key.VKey).Bytes(), key
	}
	raw, _ := hex.DecodeString(inputs[0].TxHash)
	hash := make([]byte, 28)
	copy(hash, raw)
	return hash, nil
}

// buildCertTx builds a Conway transaction that spends inputs, pays fee and
// deposit, returns the rest to changeAddr and carries cert. The body is signed
// by every input key and by credKey.
func buildCertTx(
	label string,
	inputs []UTxO,
	cert any,
	deposit uint64,
	fee uint64,
	changeAddr []byte,
	credKey *UTxOKey,
) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("%s: at least one input required", label)
	}

	bodyInputs := make(cbor.Set, 0, len(inputs))
	witnessKeys := make([]*UTxOKey, 0, len(inputs)+1)
	var total uint64
	for _, u := range inputs {
		hashBytes, err := hex.DecodeString(u.TxHash)
		if err != nil {
			return nil, fmt.Errorf(
				"%s: invalid tx hash %q: %w", label, u.TxHash, err,
			)
		}
		if len(hashBytes) != 32 {
			return nil, fmt.Errorf(
				"%s: tx hash %q has unexpected length %d",
				label, u.TxHash, len(hashBytes),
			)
		}
		bodyInputs = append(bodyInputs, txBodyInput{Hash: hashBytes, Idx: u.Index})
		if u.Amount > math.MaxUint64-total {
			return nil, fmt.Errorf("%s: total input overflow", label)
		}
		total += u.Amount
		witnessKeys = append(witnessKeys, u.SigningKey)
	}
	witnessKeys = append(witnessKeys, credKey)

	if deposit > math.MaxUint64-fee || total < fee+deposit {
		return nil, fmt.Errorf(
			"%s: total input %d cannot cover fee %d + deposit %d",
			label, total, fee, deposit,
		)
	}
	change := total - fee - deposit

	var outputs []txBodyOutput
	if change > 0 {
		if len(changeAddr) == 0 {
			return nil, fmt.Errorf(
				"%s: non-zero change requires a change address", label,
			)
		}
		if change < minSendAmount {
			return nil, fmt.Errorf(
				"%s: change %d is below the minimum output %d",
				label, change, minSendAmount,
			)
		}
		outputs = append(
			outputs,
			txBodyOutput{Address: changeAddr, Amount: change},
		)
	}

	body := txBodyWithCerts{
		Inputs:  bodyInputs,
		Outputs: outputs,
		Fee:     fee,
		Certs:   []any{cert},
	}
	bodyBytes, err := cbor.Encode(body)
	if err != nil {
		return nil, fmt.Errorf("%s: body encoding failed: %w", label, err)
	}
	tx := conwayTxWithCerts{
		Body:    body,
		Witness: BuildWitnessMap(bodyBytes, witnessKeys...),
		IsValid: true,
		AuxData: nil,
	}
	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("%s: CBOR encoding failed: %w", label, err)
	}
	return txBytes, nil
}

// BuildDelegationTx constructs a signed Conway transaction that delegates the
// stake credential to poolKeyHash. A non-zero deposit registers the
// credential in the same certificate ([11, cred, pool, deposit]); otherwise
// the credential must already be registered ([2, cred, pool]). stakeKey
// witnesses the credential and may be nil for keyless harness wallets.
func BuildDelegationTx(
	inputs []UTxO,
	stakeKeyHash []byte,
	poolKeyHash []byte,
	deposit uint64,
	fee uint64,
	changeAddr []byte,
	stakeKey *UTxOKey,
) ([]byte, error) {
	if len(stakeKeyHash) != 28 {
		return nil, fmt.Errorf(
			"delegation: stake key hash must be exactly 28 bytes, got %d",
			len(stakeKeyHash),
		)
	}
	if len(poolKeyHash) != 28 {
		return nil, fmt.Errorf(
			"delegation: pool key hash must be exactly 28 bytes, got %d",
			len(poolKeyHash),
		)
	}
	credential := stakeCredential{Type: 0, Hash: stakeKeyHash}
	var cert any = delegCert{
		CertType:   2,
		Credential: credential,
		PoolHash:   poolKeyHash,
	}
	if deposit > 0 {
		cert = stakeRegDelegCert{
			CertType:   11,
			Credential: credential,
			PoolHash:   poolKeyHash,
			Deposit:    deposit,
		}
	}
	return buildCertTx(
		"delegation", inputs, cert, deposit, fee, changeAddr, stakeKey,
	)
}
