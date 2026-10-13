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

package blockfrost

// BlockTransactionCBORResponse is one entry of the block transaction CBOR
// listings.
type BlockTransactionCBORResponse struct {
	TxHash string `json:"tx_hash"`
	CBOR   string `json:"cbor"`
}

// BlockAddressTransactionResponse names one transaction touching an address.
type BlockAddressTransactionResponse struct {
	TxHash string `json:"tx_hash"`
}

// BlockAddressResponse is one entry of the addresses affected by a block.
type BlockAddressResponse struct {
	Address      string                            `json:"address"`
	Transactions []BlockAddressTransactionResponse `json:"transactions"`
}
