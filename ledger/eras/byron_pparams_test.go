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

package eras

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mainnetByronGenesis() *byron.ByronGenesis {
	genesis := &byron.ByronGenesis{}
	data := &genesis.BlockVersionData
	data.MaxBlockSize = 2_000_000
	data.MaxHeaderSize = 2_000_000
	data.MaxTxSize = 4_096
	data.MaxProposalSize = 700
	data.SlotDuration = 20_000
	data.SoftforkRule.MinThd = 600_000_000_000_000
	data.TxFeePolicy.Summand = 155_381_000_000_000
	data.TxFeePolicy.Multiplier = 43_946_000_000
	data.UpdateImplicit = 10_000
	return genesis
}

func TestNewByronProtocolParametersFromGenesis(t *testing.T) {
	t.Parallel()
	params, err := NewByronProtocolParametersFromGenesis(mainnetByronGenesis())
	require.NoError(t, err)
	assert.Equal(t, uint64(155_381), params.TxFeeSummand)
	assert.Equal(
		t,
		0,
		big.NewInt(43_946_000_000).Cmp(params.TxFeeMultiplierNano),
	)
	assert.Equal(t, 0, big.NewInt(4_096).Cmp(params.MaxTxSize))
	assert.Equal(t, uint64(10_000), params.UpdateProposalTTL)
	// floor(0.6 * 7) = 4, the mainnet adoption threshold.
	assert.Equal(t, 4, params.UpdateAdoptionThreshold(7))

	genesis := mainnetByronGenesis()
	genesis.BlockVersionData.MaxTxSize = -1
	_, err = NewByronProtocolParametersFromGenesis(genesis)
	require.Error(t, err)
}

// TestByronProtocolParametersApplyUpdate covers PPU.apply, including the
// on-chain fee policy decode that rounds the summand ties to even where
// genesis truncates it.
func TestByronProtocolParametersApplyUpdate(t *testing.T) {
	t.Parallel()
	genesis, err := NewByronProtocolParametersFromGenesis(mainnetByronGenesis())
	require.NoError(t, err)

	unchanged, err := genesis.ApplyUpdate(
		byron.ByronUpdateProposalBlockVersionMod{},
	)
	require.NoError(t, err)
	assert.True(t, unchanged.Equal(genesis))

	raised, err := genesis.ApplyUpdate(byron.ByronUpdateProposalBlockVersionMod{
		MaxTxSize:      []*big.Int{big.NewInt(65_536)},
		MaxBlockSize:   []*big.Int{big.NewInt(3_000_000)},
		UpdateImplicit: []uint64{5},
	})
	require.NoError(t, err)
	assert.False(t, raised.Equal(genesis))
	assert.Equal(t, 0, big.NewInt(65_536).Cmp(raised.MaxTxSize))
	assert.Equal(t, 0, big.NewInt(3_000_000).Cmp(raised.MaxBlockSize))
	assert.Equal(t, uint64(5), raised.UpdateProposalTTL)
	assert.Equal(t, 0, big.NewInt(4_096).Cmp(genesis.MaxTxSize),
		"ApplyUpdate must not mutate its receiver")

	for _, test := range []struct {
		nano int64
		want uint64
	}{
		{999_999_999, 1},
		{1_499_999_999, 1},
		{1_500_000_000, 2},
		{2_500_000_000, 2},
		{2_500_000_001, 3},
		{3_500_000_000, 4},
	} {
		updated, err := genesis.ApplyUpdate(
			byron.ByronUpdateProposalBlockVersionMod{
				TxFeePolicy: []byron.ByronTxFeePolicy{{
					SummandNano:    big.NewInt(test.nano),
					MultiplierNano: big.NewInt(0),
				}},
			},
		)
		require.NoError(t, err)
		assert.Equal(t, test.want, updated.TxFeeSummand, "nano %d", test.nano)
	}
}

// TestValidateTxByron_AdoptedParameters covers #4379 and #4419: block
// application validates against the parameters adopted for the block's
// epoch, which it passes as pparams, not the genesis ones.
func TestValidateTxByron_AdoptedParameters(t *testing.T) {
	t.Parallel()
	genesis, err := NewByronProtocolParametersFromGenesis(mainnetByronGenesis())
	require.NoError(t, err)
	input := newTestInput(0x01, 0)
	newLS := func() *mockLedgerState {
		ls := newMockLedgerState()
		ls.byronFeeSummand = 155_381_000_000_000
		ls.byronFeeMultiplier = 43_946_000_000
		ls.byronMaxTxSize = 4_096
		ls.addUtxo(input, newTestOutput(1_000_000))
		return ls
	}
	tx := func(fee uint64, size int) *testByronTx {
		return &testByronTx{
			inputs: []lcommon.TransactionInput{input},
			outputs: []lcommon.TransactionOutput{
				newTestOutput(1_000_000 - fee),
			},
			cbor: make([]byte, size),
		}
	}
	// 155,381 + ceiling(43.946 * 200) = 164,171 under genesis.
	require.NoError(t, ValidateTxByron(tx(164_171, 200), 0, newLS(), nil))

	raisedFee, err := genesis.ApplyUpdate(
		byron.ByronUpdateProposalBlockVersionMod{
			TxFeePolicy: []byron.ByronTxFeePolicy{{
				SummandNano:    big.NewInt(200_000_000_000_000),
				MultiplierNano: big.NewInt(43_946_000_000),
			}},
		},
	)
	require.NoError(t, err)
	var feeErr FeeTooLowByronError
	require.ErrorAs(
		t,
		ValidateTxByron(tx(164_171, 200), 0, newLS(), raisedFee),
		&feeErr,
	)
	assert.Equal(t, big.NewInt(208_790), feeErr.Required)

	lowered, err := genesis.ApplyUpdate(
		byron.ByronUpdateProposalBlockVersionMod{
			TxFeePolicy: []byron.ByronTxFeePolicy{{
				SummandNano:    big.NewInt(100_000_000_000_000),
				MultiplierNano: big.NewInt(1_000_000_000),
			}},
		},
	)
	require.NoError(t, err)
	require.Error(t, ValidateTxByron(tx(100_200, 200), 0, newLS(), nil))
	require.NoError(t, ValidateTxByron(tx(100_200, 200), 0, newLS(), lowered))

	raisedSize, err := genesis.ApplyUpdate(
		byron.ByronUpdateProposalBlockVersionMod{
			MaxTxSize: []*big.Int{big.NewInt(8_192)},
		},
	)
	require.NoError(t, err)
	var tooLarge TxTooLargeByronError
	require.ErrorAs(
		t,
		ValidateTxByron(tx(999_000, 5_000), 0, newLS(), nil),
		&tooLarge,
	)
	require.NoError(
		t,
		ValidateTxByron(tx(999_000, 5_000), 0, newLS(), raisedSize),
	)
	loweredSize, err := genesis.ApplyUpdate(
		byron.ByronUpdateProposalBlockVersionMod{
			MaxTxSize: []*big.Int{big.NewInt(1_024)},
		},
	)
	require.NoError(t, err)
	require.ErrorAs(
		t,
		ValidateTxByron(tx(999_000, 1_025), 0, newLS(), loweredSize),
		&tooLarge,
	)
	require.NoError(
		t,
		ValidateTxByron(tx(999_000, 1_024), 0, newLS(), loweredSize),
	)
}
