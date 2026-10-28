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
	"github.com/blinklabs-io/gouroboros/ledger/mary"
)

// localStateQueryTransactionOutput projects the stored transaction output to
// the representation cardano-ledger serializes from its compact UTxO state.
// A Babbage output containing only ADA and no datum or reference script uses
// the backwards-compatible two-element array, even when its transaction used
// the post-Alonzo map form.
func localStateQueryTransactionOutput(
	txOut ledger.TransactionOutput,
) ledger.TransactionOutput {
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
