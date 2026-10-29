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
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
)

// localStateQueryTransactionOutput projects the stored transaction output to
// the representation cardano-ledger serializes from its compact UTxO state.
// A Babbage output containing only ADA and no datum or reference script uses
// the backwards-compatible two-element array, even when its transaction used
// the post-Alonzo map form.
//
// A Byron output is stored in its Byron encoding, where the address is the
// CBOR structure itself. Every post-Byron era carries it as a Shelley output
// whose address is a byte string holding that structure
// (translateTxOutByronToShelley in ByronTranslation.hs), so serving the stored
// bytes would put a Byron address where a client decodes bytes.
func localStateQueryTransactionOutput(
	txOut ledger.TransactionOutput,
) ledger.TransactionOutput {
	if byronOut, ok := txOut.(*byron.ByronTransactionOutput); ok {
		return &mary.MaryTransactionOutput{
			OutputAddress: byronOut.OutputAddress,
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: byronOut.OutputAmount,
			},
		}
	}
	babbageOut, ok := txOut.(*babbage.BabbageTransactionOutput)
	if !ok || babbageOut.DatumOption != nil ||
		babbageOut.TxOutScriptRef != nil ||
		(babbageOut.OutputAmount.Assets != nil &&
			len(babbageOut.OutputAmount.Assets.Policies()) != 0) {
		return txOut
	}
	return &mary.MaryTransactionOutput{
		OutputAddress: babbageOut.OutputAddress,
		OutputAmount:  babbageOut.OutputAmount,
	}
}
