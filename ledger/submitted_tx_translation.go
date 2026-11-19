// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"fmt"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type submittedTxTranslationError struct {
	era uint
	err error
}

func (e *submittedTxTranslationError) Error() string       { return e.err.Error() }
func (e *submittedTxTranslationError) Unwrap() error       { return e.err }
func (e *submittedTxTranslationError) SubmissionEra() uint { return e.era }

func translationFailure(era uint, err error) error {
	return &submittedTxTranslationError{era: era, err: err}
}

// TranslateSubmittedTx injects a Shelley-family transaction from an older era
// into the current ledger era. The Cardano hard-fork combinator performs the
// same adjacent-era translations recursively before applying a submitted
// transaction. Byron transactions have no injection into Shelley.
func (ls *LedgerState) TranslateSubmittedTx(
	tx gledger.Transaction,
) (gledger.Transaction, error) {
	state := ls.loadConsensusSnapshot()
	return translateSubmittedTx(tx, state.currentEra, ls.eraList())
}

func translateSubmittedTx(
	tx lcommon.Transaction,
	currentEra eras.EraDesc,
	eraList []eras.EraDesc,
) (gledger.Transaction, error) {
	txEra := tx.Type()
	if txEra < 0 {
		return nil, newEraMismatchError(uint(txEra), currentEra.Id)
	}
	txEraID := uint(txEra)
	if txEraID == currentEra.Id {
		return tx, nil
	}

	txEraIndex := eraIndex(eraList, txEraID)
	currentEraIndex := eraIndex(eraList, currentEra.Id)
	if txEraID == byron.EraIdByron || txEraIndex < 0 ||
		currentEraIndex < 0 || txEraIndex >= currentEraIndex {
		return nil, newEraMismatchError(txEraID, currentEra.Id)
	}

	originalHash := tx.Hash()
	translated := gledger.Transaction(tx)
	for i := txEraIndex + 1; i <= currentEraIndex; i++ {
		nextEra := eraList[i]
		next, err := safedecode.Transaction(nextEra.Id, translated.Cbor())
		if err != nil {
			return nil, translationFailure(currentEra.Id, fmt.Errorf(
				"translate submitted transaction from %s to %s: %w",
				eraName(txEraID), nextEra.Name, err,
			))
		}
		if next.Hash() != originalHash {
			return nil, translationFailure(currentEra.Id, fmt.Errorf(
				"translate submitted transaction from %s to %s: transaction ID changed",
				eraName(txEraID),
				nextEra.Name,
			))
		}

		// A pre-Alonzo transaction has a three-field outer envelope. The
		// Alonzo injection retains its body and witness bytes (and therefore
		// its ID) while adding the implicit validity flag to the four-field
		// envelope used by later eras.
		store, ok := next.(cbor.DecodeStoreCborInterface)
		if !ok {
			return nil, translationFailure(currentEra.Id, fmt.Errorf(
				"translate submitted transaction from %s to %s: transaction does not expose its wire encoding",
				eraName(txEraID),
				nextEra.Name,
			))
		}
		store.SetCbor(nil)
		if len(next.Cbor()) == 0 {
			return nil, translationFailure(currentEra.Id, fmt.Errorf(
				"translate submitted transaction from %s to %s: empty translated encoding",
				eraName(txEraID),
				nextEra.Name,
			))
		}
		translated = next
	}
	return translated, nil
}

func eraIndex(eraList []eras.EraDesc, eraID uint) int {
	for i := range eraList {
		if eraList[i].Id == eraID {
			return i
		}
	}
	return -1
}
