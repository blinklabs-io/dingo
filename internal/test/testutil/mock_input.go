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

package testutil

import (
	"errors"
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pdata "github.com/blinklabs-io/plutigo/data"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// MockInput is a reusable transaction input for database tests.
type MockInput struct {
	TxId       []byte
	IndexValue uint32
}

func NewMockInput(txId []byte, index uint32) *MockInput {
	return &MockInput{
		TxId:       txId,
		IndexValue: index,
	}
}

func (m *MockInput) Id() lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(m.TxId)
}

func (m *MockInput) Index() uint32 {
	return m.IndexValue
}

func (m *MockInput) String() string {
	return fmt.Sprintf("%x#%d", m.TxId, m.IndexValue)
}

func (m *MockInput) Utxorpc() (*cardano.TxInput, error) {
	return nil, errors.New("MockInput.Utxorpc: not implemented")
}

func (m *MockInput) ToPlutusData() pdata.PlutusData {
	return nil
}
