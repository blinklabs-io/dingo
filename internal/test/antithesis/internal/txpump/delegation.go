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
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// stakeCredential is a [credType, hash] pair used inside certificates.
// credType 0 = key hash.
type stakeCredential struct {
	_    cbor.StructAsArray
	Type uint32
	Hash []byte
}

// delegCert is the CBOR representation of a stake-delegation certificate.
// Conway CDDL: [2, stake_credential, pool_keyhash]
// Type 2 = delegate_stake.
type delegCert struct {
	_          cbor.StructAsArray
	CertType   uint32
	Credential stakeCredential
	PoolHash   []byte
}

// txBodyWithCerts extends the basic txBody with a certificates field.
//
// Key 0 = inputs
// Key 1 = outputs
// Key 2 = fee
// Key 4 = certificates
type txBodyWithCerts struct {
	Inputs  []txBodyInput  `cbor:"0,keyasint"`
	Outputs []txBodyOutput `cbor:"1,keyasint"`
	Fee     uint64         `cbor:"2,keyasint"`
	Certs   []delegCert    `cbor:"4,keyasint"`
}

// conwayTxWithCerts is the top-level Conway transaction carrying certificates.
type conwayTxWithCerts struct {
	_       cbor.StructAsArray
	Body    txBodyWithCerts
	Witness map[any]any
	IsValid bool
	AuxData any
}

// BuildDelegationTx constructs a minimal CBOR-encoded Conway transaction that
// includes a stake-delegation certificate (type 2).  The transaction has no
// witnesses so it will be rejected by a live node, but it exercises the full
// submission path for Antithesis testing.
//
// The certificate encodes as: [2, [0, stakeKeyHash], poolKeyHash]
func BuildDelegationTx(
	inputs []UTxO,
	stakeKeyHash []byte,
	poolKeyHash []byte,
	fee uint64,
	changeAddr []byte,
) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, errors.New("delegation: at least one input required")
	}
	if len(stakeKeyHash) == 0 {
		return nil, errors.New("delegation: stake key hash must not be empty")
	}
	if len(poolKeyHash) == 0 {
		return nil, errors.New("delegation: pool key hash must not be empty")
	}

	bodyInputs := make([]txBodyInput, 0, len(inputs))
	for _, u := range inputs {
		hashBytes, err := hex.DecodeString(u.TxHash)
		if err != nil {
			return nil, fmt.Errorf(
				"delegation: invalid tx hash %q: %w", u.TxHash, err,
			)
		}
		bodyInputs = append(bodyInputs, txBodyInput{Hash: hashBytes, Idx: u.Index})
	}

	// Calculate change: sum of inputs minus fee.
	var total uint64
	for _, u := range inputs {
		total += u.Amount
	}
	var change uint64
	if total > fee {
		change = total - fee
	}

	var outputs []txBodyOutput
	if change > 0 && len(changeAddr) > 0 {
		outputs = append(outputs, txBodyOutput{Address: changeAddr, Amount: change})
	}

	cert := delegCert{
		CertType: 2,
		Credential: stakeCredential{
			Type: 0,
			Hash: stakeKeyHash,
		},
		PoolHash: poolKeyHash,
	}

	body := txBodyWithCerts{
		Inputs:  bodyInputs,
		Outputs: outputs,
		Fee:     fee,
		Certs:   []delegCert{cert},
	}

	tx := conwayTxWithCerts{
		Body:    body,
		Witness: map[any]any{},
		IsValid: true,
		AuxData: nil,
	}

	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("delegation: CBOR encoding failed: %w", err)
	}
	return txBytes, nil
}
