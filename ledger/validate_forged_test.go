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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// validateForgedBodyHashTest exercises validateForgedBodyHash in isolation
// using the stub block type from block_event_test.go (same package).

func TestValidateForgedBlockNilBlockReturnsError(t *testing.T) {
	ls := &LedgerState{}
	err := ls.ValidateForgedBlock(nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil block")
}

func TestValidateForgedBodyHashZeroHashReturnsError(t *testing.T) {
	ls := &LedgerState{}
	// Use a stub block with a non-Byron era and an all-zero body hash.
	block := &stubValidateBlock{
		slot:     100,
		bodyHash: lcommon.Blake2b256{},
	}
	err := ls.validateForgedBodyHash(block)
	require.Error(t, err)
	// Either "empty body hash bytes" or "all-zero body hash"
	assert.Contains(t, err.Error(), "body hash")
}

func TestValidateForgedBodyHashNonZeroHashPasses(t *testing.T) {
	ls := &LedgerState{}
	var h lcommon.Blake2b256
	raw := make([]byte, 32)
	raw[0] = 0x01
	h = lcommon.NewBlake2b256(raw)
	block := &stubValidateBlock{
		slot:     100,
		bodyHash: h,
	}
	err := ls.validateForgedBodyHash(block)
	require.NoError(t, err)
}

func TestValidateForgedTxsEmptyBlockPasses(t *testing.T) {
	ls := &LedgerState{}
	block := &stubValidateBlock{slot: 100}
	err := ls.validateForgedTxs(block)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Stub block for body-hash and empty-tx tests.
// ---------------------------------------------------------------------------

// stubValidateBlock satisfies ledger.Block for validate_forged_test tests.
// It uses a Babbage-like era so Byron checks are skipped.
type stubValidateBlock struct {
	slot     uint64
	bodyHash lcommon.Blake2b256
}

func (b *stubValidateBlock) Header() lcommon.BlockHeader              { return b }
func (b *stubValidateBlock) Type() int                                { return 0 }
func (b *stubValidateBlock) Transactions() []lcommon.Transaction      { return nil }
func (b *stubValidateBlock) Utxorpc() (*utxorpc_cardano.Block, error) { return nil, nil }
func (b *stubValidateBlock) Hash() lcommon.Blake2b256                 { return lcommon.Blake2b256{} }
func (b *stubValidateBlock) PrevHash() lcommon.Blake2b256             { return lcommon.Blake2b256{} }
func (b *stubValidateBlock) BlockNumber() uint64                      { return 0 }
func (b *stubValidateBlock) SlotNumber() uint64                       { return b.slot }
func (b *stubValidateBlock) IssuerVkey() lcommon.IssuerVkey           { return lcommon.IssuerVkey{} }
func (b *stubValidateBlock) BlockBodySize() uint64                    { return 0 }
func (b *stubValidateBlock) Era() lcommon.Era {
	// Return a non-Byron era so body-hash checks are applied.
	return lcommon.Era{Id: 5, Name: "Babbage"}
}
func (b *stubValidateBlock) Cbor() []byte                      { return nil }
func (b *stubValidateBlock) BlockBodyHash() lcommon.Blake2b256 { return b.bodyHash }
