package eras

import (
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"

	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two distinct custom-network magics. The pair matters: a rule that only
// distinguished "mainnet" from "not mainnet" would treat these as the same
// network and accept an address minted for one on the other.
const (
	customNetworkA uint32 = 42
	customNetworkB uint32 = 43
)

// addressedOutput is a transaction output carrying a real address, which the
// shared testOutput does not.
type addressedOutput struct {
	amount  *big.Int
	address lcommon.Address
}

func (o addressedOutput) Address() lcommon.Address { return o.address }

func (o addressedOutput) Amount() *big.Int { return o.amount }

func (o addressedOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	return nil
}

func (o addressedOutput) Datum() *lcommon.Datum { return nil }

func (o addressedOutput) DatumHash() *lcommon.Blake2b256 { return nil }

func (o addressedOutput) Cbor() []byte { return nil }

func (o addressedOutput) Utxorpc() (*utxorpc.TxOutput, error) {
	return &utxorpc.TxOutput{}, nil
}

func (o addressedOutput) ScriptRef() lcommon.Script { return nil }

func (o addressedOutput) ToPlutusData() data.PlutusData {
	return data.NewConstr(0)
}

func (o addressedOutput) String() string { return "addressedOutput" }

// newByronAddressForNetwork builds a Byron address belonging to the given
// network. A nil magic produces the mainnet encoding, which carries no
// network attribute at all; a non-nil one carries that magic explicitly.
func newByronAddressForNetwork(
	t *testing.T,
	magic *uint32,
	hashByte byte,
) lcommon.Address {
	t.Helper()
	hash := make([]byte, lcommon.AddressHashSize)
	hash[0] = hashByte
	attr := lcommon.ByronAddressAttributes{}
	if magic != nil {
		value := *magic
		attr.Network = &value
	}
	addr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		hash,
		attr,
	)
	require.NoError(t, err)
	return addr
}

// newNetworkTestLedgerState builds a ledger state validating for the given
// protocol magic, with a fee policy that demands nothing so the network rule
// is what decides each case.
func newNetworkTestLedgerState(protocolMagic uint32) *mockLedgerState {
	ls := newMockLedgerState()
	ls.protocolMagic = protocolMagic
	ls.byronFeeSummand = 0
	ls.byronFeeMultiplier = 0
	return ls
}

// newOutputNetworkTx funds one input and pays the whole value to the supplied
// output addresses.
func newOutputNetworkTx(
	ls *mockLedgerState,
	addresses []lcommon.Address,
) *testByronTx {
	input := newTestInput(0x01, 0)
	const perOutput = 1_000_000
	ls.addUtxo(input, newTestOutput(uint64(len(addresses))*perOutput))
	outputs := make([]lcommon.TransactionOutput, 0, len(addresses))
	for _, addr := range addresses {
		outputs = append(outputs, addressedOutput{
			amount:  new(big.Int).SetUint64(perOutput),
			address: addr,
		})
	}
	return &testByronTx{
		inputs:  []lcommon.TransactionInput{input},
		outputs: outputs,
	}
}

// TestValidateTxByron_OutputNetworkMagic covers the case matrix the issue
// calls for, through the ValidateTxByron entry point.
func TestValidateTxByron_OutputNetworkMagic(t *testing.T) {
	t.Parallel()

	magicA := customNetworkA
	magicB := customNetworkB

	tests := []struct {
		name          string
		protocolMagic uint32
		// outputs is one entry per output: nil means the mainnet encoding.
		outputs      []*uint32
		wantError    bool
		wantIndex    int
		wantExpected *uint32
		wantActual   *uint32
	}{
		{
			name:          "mainnet to mainnet",
			protocolMagic: byron.MainnetProtocolMagic,
			outputs:       []*uint32{nil},
		},
		{
			name:          "mainnet to testnet",
			protocolMagic: byron.MainnetProtocolMagic,
			outputs:       []*uint32{&magicA},
			wantError:     true,
			wantActual:    &magicA,
		},
		{
			name:          "custom network to its own network",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicA},
		},
		{
			name:          "custom network A to network B",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicB},
			wantError:     true,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
		{
			name:          "custom network to mainnet encoding",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{nil},
			wantError:     true,
			wantExpected:  &magicA,
		},
		{
			// The three multi-output cases below pin the bad output at each
			// position in turn. A validator that only inspected the last
			// output (or only the first) would still pass a suite that put
			// the mismatch in just one place; PR review on #4380 found this
			// gap, since the original matrix only exercised "bad output
			// last".
			name:          "multi-output, bad output last",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicA, &magicA, &magicB},
			wantError:     true,
			wantIndex:     2,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
		{
			name:          "multi-output, bad output first",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicB, &magicA, &magicA},
			wantError:     true,
			wantIndex:     0,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
		{
			name:          "multi-output, bad output in the middle",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicA, &magicB, &magicA},
			wantError:     true,
			wantIndex:     1,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ls := newNetworkTestLedgerState(test.protocolMagic)
			addresses := make([]lcommon.Address, 0, len(test.outputs))
			for i, magic := range test.outputs {
				addresses = append(
					addresses,
					newByronAddressForNetwork(t, magic, byte(0xB0+i)),
				)
			}
			tx := newOutputNetworkTx(ls, addresses)

			err := byronValidateOutputNetwork(tx, 0, ls, nil)
			if !test.wantError {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var mismatch NetworkMagicMismatchByronError
			require.ErrorAs(t, err, &mismatch)
			assert.Equal(t, test.wantIndex, mismatch.OutputIndex)
			if test.wantExpected == nil {
				assert.Nil(t, mismatch.Expected)
			} else {
				require.NotNil(t, mismatch.Expected)
				assert.Equal(t, *test.wantExpected, *mismatch.Expected)
			}
			if test.wantActual == nil {
				assert.Nil(t, mismatch.Actual)
			} else {
				require.NotNil(t, mismatch.Actual)
				assert.Equal(t, *test.wantActual, *mismatch.Actual)
			}
		})
	}
}

// TestValidateTxByron_OutputNetworkMagicThroughEntryPoint confirms the rule is
// reachable through ValidateTxByron and not merely as a standalone function,
// since a rule that is never registered enforces nothing.
func TestValidateTxByron_OutputNetworkMagicThroughEntryPoint(t *testing.T) {
	t.Parallel()

	magicB := customNetworkB
	ls := newNetworkTestLedgerState(customNetworkA)
	tx := newOutputNetworkTx(ls, []lcommon.Address{
		newByronAddressForNetwork(t, &magicB, 0xB0),
	})

	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	var mismatch NetworkMagicMismatchByronError
	require.ErrorAs(t, err, &mismatch)
}

// TestByronValidateOutputNetwork_SurfacesLookupFailure proves the rule fails
// closed when the protocol magic cannot be read, rather than skipping the
// network check.
func TestByronValidateOutputNetwork_SurfacesLookupFailure(t *testing.T) {
	t.Parallel()

	ls := newNetworkTestLedgerState(customNetworkA)
	ls.protocolMagicErr = errors.New("byron genesis unavailable")
	tx := newOutputNetworkTx(ls, []lcommon.Address{
		newByronAddressForNetwork(t, nil, 0xB0),
	})

	err := byronValidateOutputNetwork(tx, 0, ls, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get Byron protocol magic")
}

// TestByronValidateOutputNetwork_SkipsWithoutProvider preserves the behaviour
// the other Byron rules rely on: a lightweight ledger state that exposes no
// chain configuration must not make every transaction invalid.
func TestByronValidateOutputNetwork_SkipsWithoutProvider(t *testing.T) {
	t.Parallel()

	magicB := customNetworkB
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{newTestInput(0x01, 0)},
		outputs: []lcommon.TransactionOutput{addressedOutput{
			amount:  big.NewInt(1),
			address: newByronAddressForNetwork(t, &magicB, 0xB0),
		}},
	}

	require.NoError(
		t,
		byronValidateOutputNetwork(
			tx, 0, noProtocolMagicLedgerState{}, nil,
		),
	)
}

// noProtocolMagicLedgerState implements lcommon.LedgerState via the embedded
// nil interface but not ByronProtocolMagicProvider.
type noProtocolMagicLedgerState struct {
	lcommon.LedgerState
}
