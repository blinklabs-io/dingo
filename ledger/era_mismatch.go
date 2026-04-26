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

package ledger

import (
	"fmt"

	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

// newEraMismatchError builds a *gledger.EraMismatch from the offered tx's
// era ID and the ledger's current era ID. Era names are looked up in
// dingo's era registry; both registries (dingo and gouroboros) use the
// same numeric IDs and the same canonical era names ("Byron", "Shelley",
// "Allegra", "Mary", "Alonzo", "Babbage", "Conway"), which match the
// strings the Haskell node emits via singleEraName.
//
// The returned value implements localtxsubmission.CborRejectReason via
// its MarshalCBOR method, so when surfaced from a SubmitTxFunc it is
// wire-encoded as canonical [2, [2, otherIdx, otherName], [2, ledgerIdx,
// ledgerName]] bytes — matching the Haskell golden bytes for
// HardForkApplyTxErrWrongEra.
//
// Era IDs that don't resolve via eras.GetEraById fall back to a synthetic
// "unknown(N)" name; this preserves the prior fmt.Errorf behaviour for
// unrecognised IDs without making the wire encoding fail. (A real-world
// SubmitTx that produces an unknown era ID is already going to be
// rejected for other reasons; the typed wire just stays informative.)
func newEraMismatchError(txEraId, ledgerEraId uint) *gledger.EraMismatch {
	return &gledger.EraMismatch{
		OtherEra: gledger.EraInfo{
			Index: uint8(txEraId), //nolint:gosec // era ids are small
			Name:  eraName(txEraId),
		},
		LedgerEra: gledger.EraInfo{
			Index: uint8(ledgerEraId), //nolint:gosec // era ids are small
			Name:  eraName(ledgerEraId),
		},
	}
}

func eraName(eraId uint) string {
	if e := eras.GetEraById(eraId); e != nil {
		return e.Name
	}
	return fmt.Sprintf("unknown(%d)", eraId)
}
