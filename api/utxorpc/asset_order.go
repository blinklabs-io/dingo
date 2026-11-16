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

package utxorpc

import (
	"bytes"
	"slices"

	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// canonicalizeMultiassets sorts multiasset groups in place by policy ID bytes
// and each group's assets by name bytes. gouroboros builds these lists by
// ranging over Go maps, so without this the order differs between calls.
func canonicalizeMultiassets(groups []*cardano.Multiasset) {
	slices.SortStableFunc(groups, func(a, b *cardano.Multiasset) int {
		return bytes.Compare(a.GetPolicyId(), b.GetPolicyId())
	})
	for _, group := range groups {
		assets := group.GetAssets()
		slices.SortStableFunc(assets, func(a, b *cardano.Asset) int {
			return bytes.Compare(a.GetName(), b.GetName())
		})
	}
}

func canonicalizeOutputAssets(out *cardano.TxOutput) {
	if out != nil {
		canonicalizeMultiassets(out.GetAssets())
	}
}

func canonicalizeInputAssets(inputs []*cardano.TxInput) {
	for _, in := range inputs {
		canonicalizeOutputAssets(in.GetAsOutput())
	}
}

func canonicalizeTxAssets(tx *cardano.Tx) {
	if tx == nil {
		return
	}
	canonicalizeInputAssets(tx.GetInputs())
	for _, out := range tx.GetOutputs() {
		canonicalizeOutputAssets(out)
	}
	canonicalizeMultiassets(tx.GetMint())
	canonicalizeInputAssets(tx.GetReferenceInputs())
	if col := tx.GetCollateral(); col != nil {
		canonicalizeInputAssets(col.GetCollateral())
		canonicalizeOutputAssets(col.GetCollateralReturn())
	}
}

func canonicalizeBlockAssets(block *cardano.Block) {
	for _, tx := range block.GetBody().GetTx() {
		canonicalizeTxAssets(tx)
	}
}
