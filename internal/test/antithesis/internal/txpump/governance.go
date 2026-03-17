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
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// drepRegCert is the CBOR representation of a DRep registration certificate.
// Conway CDDL: [16, drep_credential, coin, anchor / null]
// Type 16 = reg_drep_cert.
type drepRegCert struct {
	_          cbor.StructAsArray
	CertType   uint32
	Credential stakeCredential
	Deposit    uint64
	Anchor     any // null for our test transactions
}

// txBodyWithDRepCerts carries a DRep registration certificate.
//
// Key 0 = inputs
// Key 1 = outputs
// Key 2 = fee
// Key 3 = ttl
// Key 4 = certificates
type txBodyWithDRepCerts struct {
	Inputs  []txBodyInput  `cbor:"0,keyasint"`
	Outputs []txBodyOutput `cbor:"1,keyasint"`
	Fee     uint64         `cbor:"2,keyasint"`
	TTL     uint64         `cbor:"3,keyasint,omitempty"`
	Certs   []drepRegCert  `cbor:"4,keyasint"`
}

// conwayTxWithDRepCerts is the top-level Conway tx carrying DRep certs.
type conwayTxWithDRepCerts struct {
	_       cbor.StructAsArray
	Body    txBodyWithDRepCerts
	Witness map[any]any
	IsValid bool
	AuxData any
}

// BuildDRepRegistrationTx constructs a minimal CBOR-encoded Conway transaction
// containing a DRep registration certificate (type 16).  The transaction has
// no witnesses so it will be rejected by a live node, but exercises the full
// submission path for Antithesis testing.
//
// The certificate encodes as: [16, [0, drepKeyHash], deposit, null]
func BuildDRepRegistrationTx(
	inputs []UTxO,
	drepKeyHash []byte,
	deposit uint64,
	fee uint64,
	changeAddr []byte,
) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, errors.New("drep_reg: at least one input required")
	}
	if len(drepKeyHash) == 0 {
		return nil, errors.New("drep_reg: DRep key hash must not be empty")
	}

	bodyInputs, err := decodeInputs("drep_reg", inputs)
	if err != nil {
		return nil, err
	}

	total := sumInputs(inputs)
	spent := fee + deposit
	if total < spent {
		return nil, fmt.Errorf(
			"drep_reg: total input %d cannot cover fee (%d) + deposit (%d) = %d",
			total, fee, deposit, spent,
		)
	}
	change := total - spent
	if change > 0 && len(changeAddr) == 0 {
		return nil, errors.New("drep_reg: change requires a change address")
	}

	var outputs []txBodyOutput
	if change > 0 {
		outputs = append(outputs, txBodyOutput{Address: changeAddr, Amount: change})
	}

	tx := conwayTxWithDRepCerts{
		Body: txBodyWithDRepCerts{
			Inputs:  bodyInputs,
			Outputs: outputs,
			Fee:     fee,
			TTL:     maxTTL,
			Certs: []drepRegCert{{
				CertType:   16,
				Credential: stakeCredential{Type: 0, Hash: drepKeyHash},
				Deposit:    deposit,
			}},
		},
		Witness: map[any]any{},
		IsValid: true,
	}

	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("drep_reg: CBOR encoding failed: %w", err)
	}
	return txBytes, nil
}

// govActionID identifies a governance action by the transaction that
// introduced it and the index within that transaction.
type govActionID struct {
	_     cbor.StructAsArray
	TxID  []byte
	Index uint32
}

// voter identifies the entity casting a vote.
// voterType 2 = DRep key hash.
type voter struct {
	_         cbor.StructAsArray
	VoterType uint32
	KeyHash   []byte
}

// votingProcedure is a single vote entry: [vote, anchor / null].
// vote: 0 = No, 1 = Yes, 2 = Abstain.
type votingProcedure struct {
	_      cbor.StructAsArray
	Vote   uint32
	Anchor any // null for test transactions
}

// txBodyWithVotes carries voting procedures in key 19.
//
// Key 0 = inputs
// Key 1 = outputs
// Key 2 = fee
// Key 3 = ttl
// Key 19 = voting_procedures (map of voter -> map of gov_action_id -> procedure)
//
// Because the CBOR map keys are not simple integers, we encode the voting
// procedures as a raw cbor.RawMessage so we can hand-craft the nested map
// structure required by the Conway CDDL.
type txBodyWithVotes struct {
	Inputs  []txBodyInput   `cbor:"0,keyasint"`
	Outputs []txBodyOutput  `cbor:"1,keyasint"`
	Fee     uint64          `cbor:"2,keyasint"`
	TTL     uint64          `cbor:"3,keyasint,omitempty"`
	Votes   cbor.RawMessage `cbor:"19,keyasint"`
}

// conwayTxWithVotes is the top-level Conway tx carrying voting procedures.
type conwayTxWithVotes struct {
	_       cbor.StructAsArray
	Body    txBodyWithVotes
	Witness map[any]any
	IsValid bool
	AuxData any
}

// BuildVoteTx constructs a minimal CBOR-encoded Conway transaction that
// contains a single voting procedure (key 19 in the tx body).  The
// transaction has no witnesses so it will be rejected by a live node, but
// it exercises the full submission path for Antithesis testing.
//
// The vote encodes as: voter -> {govActionID -> [1, null]}
// where vote value 1 = Yes and voter type 2 = DRep key hash.
func BuildVoteTx(
	inputs []UTxO,
	voterKeyHash []byte,
	govActionTxHash []byte,
	govActionIndex uint32,
	fee uint64,
	changeAddr []byte,
) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, errors.New("vote: at least one input required")
	}
	if len(voterKeyHash) == 0 {
		return nil, errors.New("vote: voter key hash must not be empty")
	}
	if len(govActionTxHash) == 0 {
		return nil, errors.New("vote: governance action tx hash must not be empty")
	}

	bodyInputs, err := decodeInputs("vote", inputs)
	if err != nil {
		return nil, err
	}

	total := sumInputs(inputs)
	if total < fee {
		return nil, fmt.Errorf("vote: inputs (%d) insufficient for fee (%d)", total, fee)
	}
	change := total - fee
	if change > 0 && len(changeAddr) == 0 {
		return nil, errors.New("vote: change requires a change address")
	}

	var outputs []txBodyOutput
	if change > 0 {
		outputs = append(outputs, txBodyOutput{Address: changeAddr, Amount: change})
	}

	// Encode the voting_procedures map as hand-crafted CBOR because the key
	// types (voter, govActionID) contain []byte fields and are not hashable
	// in Go maps.  Each single-pair map uses the 0xa1 header byte.
	votesRaw, err := encodeVotingProcedures(
		voter{VoterType: 2, KeyHash: voterKeyHash},
		govActionID{TxID: govActionTxHash, Index: govActionIndex},
		votingProcedure{Vote: 1},
	)
	if err != nil {
		return nil, err
	}

	tx := conwayTxWithVotes{
		Body: txBodyWithVotes{
			Inputs:  bodyInputs,
			Outputs: outputs,
			Fee:     fee,
			TTL:     maxTTL,
			Votes:   cbor.RawMessage(votesRaw),
		},
		Witness: map[any]any{},
		IsValid: true,
	}

	txBytes, err := cbor.Encode(tx)
	if err != nil {
		return nil, fmt.Errorf("vote: CBOR encoding failed: %w", err)
	}
	return txBytes, nil
}

// cborMapOf1 is the CBOR header byte for a map with exactly 1 key-value pair.
const cborMapOf1 = 0xa1

// encodeVotingProcedures builds the CBOR for:
//
//	{ voter => { govActionID => votingProcedure } }
func encodeVotingProcedures(
	v voter,
	actionID govActionID,
	procedure votingProcedure,
) ([]byte, error) {
	actionIDBytes, err := cbor.Encode(actionID)
	if err != nil {
		return nil, fmt.Errorf("vote: encoding action ID: %w", err)
	}
	procedureBytes, err := cbor.Encode(procedure)
	if err != nil {
		return nil, fmt.Errorf("vote: encoding voting procedure: %w", err)
	}
	voterBytes, err := cbor.Encode(v)
	if err != nil {
		return nil, fmt.Errorf("vote: encoding voter: %w", err)
	}

	// Inner map: { action_id => procedure }
	inner := append([]byte{cborMapOf1}, actionIDBytes...)
	inner = append(inner, procedureBytes...)

	// Outer map: { voter => inner_map }
	outer := append([]byte{cborMapOf1}, voterBytes...)
	outer = append(outer, inner...)

	return outer, nil
}
