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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
// transaction: missing or unreadable constitution state fails closed.
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
