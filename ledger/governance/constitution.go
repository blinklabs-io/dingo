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

package governance

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// ErrConstitutionUnavailable reports that no usable enacted constitution
// could be read out of the ledger store.
//
// Guardrails validation (gouroboros' UtxoValidateGuardrailsScriptHash)
// derives the required policy hash of every parameter-change and
// treasury-withdrawal proposal from the enacted constitution, and reads a
// nil constitution as "the chain has no guardrails script". A Conway chain
// always has a constitution, so reporting nil for state we merely failed to
// record or read would accept proposals carrying no policy hash on a chain
// whose constitution does require one. Returning this error instead makes
// gouroboros wrap it in conway.ConstitutionLookupError and reject the
// transaction: missing or malformed constitution state fails closed. A
// store read that fails outright is propagated by the caller as its own
// wrapped error, which gouroboros classifies the same way.
var ErrConstitutionUnavailable = errors.New(
	"enacted constitution unavailable",
)

// ConstitutionFromModel maps a stored constitution row onto the shared
// gouroboros ledger-state contract (common.GovState.Constitution).
//
// The anchor URL and anchor hash become the constitution's GovAnchor; the
// optional guardrails policy hash becomes ScriptHash, which gouroboros
// compares against each proposal's policy hash by nil-ness as well as by
// value, so a stored zero-length policy hash is normalized to nil ("this
// constitution has no guardrails script") rather than passed through as an
// empty non-nil slice.
//
// A nil row (no constitution recorded) and a row whose anchor hash is not a
// full 32-byte blake2b-256 digest both fail closed with
// ErrConstitutionUnavailable: the caller has no constitution it can prove,
// and truncating or zero-padding a short digest into the contract's
// fixed-size array would silently publish a wrong anchor.
//
// The guardrails policy hash is deliberately not length-checked here. It is
// a variable-length []byte in the contract, so no value is lost by passing
// it through, and gouroboros rejects a non-nil hash that is not
// Blake2b224Size with conway.MalformedConstitutionError, which names the
// offending length. Pre-empting that with ErrConstitutionUnavailable would
// report a malformed guardrails hash as a failed lookup instead.
func ConstitutionFromModel(
	stored *models.Constitution,
) (*lcommon.Constitution, error) {
	if stored == nil {
		return nil, ErrConstitutionUnavailable
	}
	if len(stored.AnchorHash) != lcommon.Blake2b256Size {
		return nil, fmt.Errorf(
			"%w: anchor hash length %d, want %d",
			ErrConstitutionUnavailable,
			len(stored.AnchorHash),
			lcommon.Blake2b256Size,
		)
	}
	ret := &lcommon.Constitution{
		Anchor: lcommon.GovAnchor{
			Url: stored.AnchorURL,
		},
	}
	copy(ret.Anchor.DataHash[:], stored.AnchorHash)
	if len(stored.PolicyHash) > 0 {
		ret.ScriptHash = bytes.Clone(stored.PolicyHash)
	}
	return ret, nil
}

// ConstitutionFromGenesis maps the Conway genesis constitution onto a stored
// constitution row at slot 0.
//
// A Conway chain's constitution is enacted at genesis and replaced only by a
// NewConstitution governance action, so the genesis anchor and guardrails
// script hash are the enacted constitution until such an action is enacted
// at a later slot. Recording it at slot 0 lets the constitution lookup,
// which reads the highest non-deleted added_slot, return an enactment
// whenever one exists and the genesis constitution otherwise.
//
// A genesis config that records no constitution at all maps to a nil row and
// no error: there is nothing to seed. A recorded constitution whose anchor
// hash or guardrails script hash is not hex of the required length is
// reported as an error instead, because guardrails validation compares that
// script hash against the policy hash of every parameter-change and
// treasury-withdrawal proposal, and a wrong value would reject all of them.
func ConstitutionFromGenesis(
	genesis *conway.ConwayGenesis,
) (*models.Constitution, error) {
	if genesis == nil {
		return nil, nil
	}
	anchor := genesis.Constitution.Anchor
	script := genesis.Constitution.Script
	if anchor.DataHash == "" && anchor.Url == "" && script == "" {
		return nil, nil
	}
	anchorHash, err := decodeGenesisConstitutionHash(
		"anchor hash",
		anchor.DataHash,
		lcommon.Blake2b256Size,
	)
	if err != nil {
		return nil, err
	}
	var policyHash []byte
	if script != "" {
		policyHash, err = decodeGenesisConstitutionHash(
			"guardrails script hash",
			script,
			lcommon.Blake2b224Size,
		)
		if err != nil {
			return nil, err
		}
	}
	return &models.Constitution{
		AnchorURL:  anchor.Url,
		AnchorHash: anchorHash,
		PolicyHash: policyHash,
		AddedSlot:  0,
	}, nil
}

// decodeGenesisConstitutionHash decodes one hex-encoded genesis constitution
// hash and requires it to be exactly size bytes.
func decodeGenesisConstitutionHash(
	field string,
	encoded string,
	size int,
) ([]byte, error) {
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf(
			"decode genesis constitution %s: %w",
			field,
			err,
		)
	}
	if len(decoded) != size {
		return nil, fmt.Errorf(
			"genesis constitution %s length %d, want %d",
			field,
			len(decoded),
			size,
		)
	}
	return decoded, nil
}
