package ledger

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// byronMagicOnlyLedgerState supplies the protocol magic and a UTxO lookup
// that always misses. The misses make the input and value rules fail, so the
// assertions below are about whether the network error joins them rather than
// about overall success.
type byronMagicOnlyLedgerState struct {
	lcommon.LedgerState
	protocolMagic uint32
}

func (s byronMagicOnlyLedgerState) UtxoById(
	lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	return lcommon.Utxo{}, errors.New("utxo not found")
}

func (s byronMagicOnlyLedgerState) ByronProtocolMagic() (uint32, error) {
	return s.protocolMagic, nil
}

// TestByronOutputNetworkAgainstRealMainnetBlock drives a real golden Byron
// mainnet block through the era's registered transaction validator, the same
// entry point the block-application loop calls per transaction.
//
// The block is mainnet, so its outputs carry no network attribute. Validating
// it as mainnet must raise no network error, and validating the identical
// block as a custom network must raise one for its first output. That
// asymmetry is the rule working on real chain bytes rather than on addresses
// this test constructed.
func TestByronOutputNetworkAgainstRealMainnetBlock(t *testing.T) {
	t.Parallel()

	block := loadEnvelopeByronFixture(
		t,
		"Block_Byron_regular",
		uint(gledger.BlockTypeByronMain),
	)
	txs := block.Transactions()
	require.NotEmpty(t, txs, "fixture block must carry a transaction")

	validateTx := eras.ByronEraDesc.ValidateTxFunc
	require.NotNil(t, validateTx, "Byron era must register a tx validator")

	// Sanity-check the fixture really is mainnet-encoded, so the two cases
	// below differ for the reason this test claims.
	for _, tx := range txs {
		for i, output := range tx.Outputs() {
			addr := output.Address()
			require.Equal(
				t,
				uint8(lcommon.AddressTypeByron),
				addr.Type(),
				"output %d must be a Byron address", i,
			)
			require.Nil(
				t,
				addr.ByronAttr().Network,
				"mainnet output %d must carry no network attribute", i,
			)
		}
	}

	t.Run("validated as mainnet", func(t *testing.T) {
		for _, tx := range txs {
			err := validateTx(
				tx,
				0,
				byronMagicOnlyLedgerState{
					protocolMagic: byron.MainnetProtocolMagic,
				},
				nil,
			)
			var mismatch eras.NetworkMagicMismatchByronError
			require.NotErrorAs(
				t,
				err,
				&mismatch,
				"a mainnet block's outputs must be valid on mainnet",
			)
		}
	})

	t.Run("validated as a custom network", func(t *testing.T) {
		const customMagic uint32 = 42
		for _, tx := range txs {
			err := validateTx(
				tx,
				0,
				byronMagicOnlyLedgerState{protocolMagic: customMagic},
				nil,
			)
			require.Error(t, err)
			var mismatch eras.NetworkMagicMismatchByronError
			require.ErrorAs(
				t,
				err,
				&mismatch,
				"mainnet outputs must be refused on a custom network",
			)
			assert.Equal(t, 0, mismatch.OutputIndex)
			require.NotNil(t, mismatch.Expected)
			assert.Equal(t, customMagic, *mismatch.Expected)
			assert.Nil(
				t,
				mismatch.Actual,
				"the offending address is mainnet-encoded",
			)
		}
	})
}

// networkMagicTestTx wraps byron.ByronTransaction to override Inputs() and
// Outputs(), the same pattern the eras package's testByronTx uses, so it
// satisfies lcommon.Transaction without hand-implementing every method.
type networkMagicTestTx struct {
	byron.ByronTransaction
	inputs  []lcommon.TransactionInput
	outputs []lcommon.TransactionOutput
}

func (t *networkMagicTestTx) Inputs() []lcommon.TransactionInput {
	return t.inputs
}

func (t *networkMagicTestTx) Outputs() []lcommon.TransactionOutput {
	return t.outputs
}

// networkMagicTestInput is a minimal lcommon.TransactionInput.
type networkMagicTestInput struct {
	txId  lcommon.Blake2b256
	index uint32
}

func (i networkMagicTestInput) Id() lcommon.Blake2b256 { return i.txId }
func (i networkMagicTestInput) Index() uint32          { return i.index }
func (i networkMagicTestInput) String() string {
	return fmt.Sprintf("%x#%d", i.txId[:], i.index)
}

func (i networkMagicTestInput) MarshalJSON() ([]byte, error) {
	return []byte(`"` + i.String() + `"`), nil
}

func (i networkMagicTestInput) ToPlutusData() data.PlutusData {
	return data.NewConstr(0)
}

func (i networkMagicTestInput) Utxorpc() (*utxorpc.TxInput, error) {
	return &utxorpc.TxInput{}, nil
}

// networkMagicTestOutput is a minimal lcommon.TransactionOutput carrying a
// real address, which is all byronValidateOutputNetwork reads.
type networkMagicTestOutput struct {
	address lcommon.Address
}

func (o networkMagicTestOutput) Address() lcommon.Address { return o.address }
func (o networkMagicTestOutput) Amount() *big.Int {
	return big.NewInt(1_000_000)
}

func (o networkMagicTestOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	return nil
}
func (o networkMagicTestOutput) Datum() *lcommon.Datum          { return nil }
func (o networkMagicTestOutput) DatumHash() *lcommon.Blake2b256 { return nil }
func (o networkMagicTestOutput) Cbor() []byte                   { return nil }
func (o networkMagicTestOutput) ScriptRef() lcommon.Script      { return nil }
func (o networkMagicTestOutput) ToPlutusData() data.PlutusData {
	return data.NewConstr(0)
}

func (o networkMagicTestOutput) String() string {
	return "networkMagicTestOutput"
}
func (o networkMagicTestOutput) Utxorpc() (*utxorpc.TxOutput, error) {
	return &utxorpc.TxOutput{}, nil
}

// newNetworkMagicNodeConfig builds a Byron genesis carrying the given
// protocol magic, the value byronValidateOutputNetwork derives its expected
// network from.
func newNetworkMagicNodeConfig(
	t *testing.T,
	protocolMagic int,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	genesis := fmt.Sprintf(`{
		"blockVersionData": {"slotDuration": "20000"},
		"protocolConsts": {"k": 2160, "protocolMagic": %d}
	}`, protocolMagic)
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(genesis)),
	)
	return cfg
}

// TestLedgerProcessBlockRejectsOutputNetworkMismatch drives a transaction
// with a mismatched-network output through ledgerProcessBlock, the real
// internal method ledgerProcessBlocksFromSource calls per block, with a real
// *LedgerState, a real database transaction, and a real Byron genesis
// providing the protocol magic. This is the production LedgerView wiring
// byronValidateOutputNetwork actually reads in from -- not the mock ledger
// state the package-level unit tests use.
//
// This does not exercise Byron PBFT header-signature verification: that
// check runs in advanceByronPBFTState, a separate step in
// ledgerProcessBlocksFromSource that ledgerProcessBlock itself does not
// perform (confirmed by reading both functions). Reaching it would require a
// block signed by a genuine Byron genesis delegate key, which is a
// substantially larger fixture with no bearing on the network-magic rule
// under test. envelope validation, per-transaction rule dispatch, and the
// real LedgerView/database wiring are exercised as production runs them.
func TestLedgerProcessBlockRejectsOutputNetworkMismatch(t *testing.T) {
	t.Parallel()

	const customMagic = 42
	const wrongMagic = 43

	db := newTestDB(t)
	nodeConfig := newNetworkMagicNodeConfig(t, customMagic)
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	wrongMagicValue := uint32(wrongMagic)
	mismatchedAddr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		make([]byte, lcommon.AddressHashSize),
		lcommon.ByronAddressAttributes{Network: &wrongMagicValue},
	)
	require.NoError(t, err)

	tx := &networkMagicTestTx{
		inputs: []lcommon.TransactionInput{
			networkMagicTestInput{txId: lcommon.Blake2b256{0x01}, index: 0},
		},
		outputs: []lcommon.TransactionOutput{
			networkMagicTestOutput{address: mismatchedAddr},
		},
	}

	block := &envelopeTestBlock{
		header: &envelopeTestHeader{
			cbor:   []byte{0x80},
			slot:   1,
			number: 1,
			era:    byron.EraByron,
		},
		cbor: []byte{0x82, 0x80, 0x80},
		txs:  []lcommon.Transaction{tx},
	}

	err = db.Transaction(true).Do(func(txn *database.Txn) error {
		_, procErr := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 1, Hash: block.Hash().Bytes()},
			block,
			true, // shouldValidate: exercises the real per-tx ValidateTxFunc loop
			false,
			false,
			nil,
			envelopeParent{origin: true},
			nil,
			eras.ByronEraDesc,
			&shelley.ShelleyProtocolParameters{},
			nil,
			0,
			0,
			false,
		)
		return procErr
	})

	require.Error(t, err)
	var mismatch eras.NetworkMagicMismatchByronError
	require.ErrorAs(
		t,
		err,
		&mismatch,
		"ledgerProcessBlock must reject the block over the output's network magic",
	)
	require.NotNil(t, mismatch.Expected)
	assert.Equal(t, uint32(customMagic), *mismatch.Expected)
	require.NotNil(t, mismatch.Actual)
	assert.Equal(t, uint32(wrongMagic), *mismatch.Actual)
}
