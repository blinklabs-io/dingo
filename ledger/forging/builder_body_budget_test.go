package forging

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func bodyBudgetParams(era eraKind, limit uint) lcommon.ProtocolParameters {
	switch era {
	case eraShelley, eraAllegra:
		return &shelley.ShelleyProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit,
			ProtocolMajor: uint(era) + 1,
		}
	case eraMary:
		return &mary.MaryProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit, ProtocolMajor: 4,
		}
	case eraAlonzo:
		return &alonzo.AlonzoProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit, ProtocolMajor: 5,
		}
	case eraBabbage:
		return &babbage.BabbageProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit, ProtocolMajor: 7,
		}
	default:
		return &conway.ConwayProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit,
		}
	}
}

func bodyBudgetTransaction(
	t testing.TB,
	era eraKind,
	index int,
	withMetadata bool,
) MempoolTransaction {
	t.Helper()
	var auxiliary any
	if withMetadata {
		metadata := map[uint]any{0: bytes.Repeat([]byte{0xab}, 64)}
		scripts := []any{[]any{uint(0), make([]byte, 28)}}
		switch era {
		case eraShelley:
			auxiliary = metadata
		case eraAllegra, eraMary:
			auxiliary = []any{metadata, scripts}
		default:
			auxiliary = cbor.Tag{
				Number: 259,
				Content: map[uint]any{
					0: metadata, 1: scripts, 2: []any{[]byte{1, 2, 3}},
				},
			}
		}
	}
	body := map[uint]any{
		0: []any{[]any{make([]byte, 32), uint(index)}},
		2: uint(200000),
	}
	if auxiliary != nil {
		encoded, err := cbor.Encode(auxiliary)
		require.NoError(t, err)
		body[7] = lcommon.Blake2b256Hash(encoded).Bytes()
	}
	parts := []any{body, map[uint]any{}}
	if era >= eraAlonzo {
		parts = append(parts, true)
	}
	parts = append(parts, auxiliary)
	encoded, err := cbor.Encode(parts)
	require.NoError(t, err)
	transaction := MempoolTransaction{
		Cbor: encoded, Type: uint(era),
	}
	decoded, err := decodeMempoolTx(transaction)
	require.NoError(t, err)
	transaction.Hash = decoded.Hash().String()
	return transaction
}

func bodyBudgetEncodedSize(
	t testing.TB,
	era eraKind,
	transactions []MempoolTransaction,
) uint64 {
	t.Helper()
	bodies := []cbor.RawMessage{}
	witnesses := []cbor.RawMessage{}
	metadata := map[uint]cbor.RawMessage{}
	for index, transaction := range transactions {
		var parts []cbor.RawMessage
		_, err := cbor.Decode(transaction.Cbor, &parts)
		require.NoError(t, err)
		bodies = append(bodies, parts[0])
		witnesses = append(witnesses, parts[1])
		auxiliary := parts[len(parts)-1]
		if !bytes.Equal(auxiliary, []byte{0xf6}) {
			metadata[uint(index)] = auxiliary
		}
	}
	components := []any{bodies, witnesses, metadata}
	if era == eraAlonzo {
		components = append(components, cbor.IndefLengthList{})
	} else if era > eraAlonzo {
		components = append(components, []uint{})
	}
	var size uint64
	for _, component := range components {
		encoded, err := cbor.Encode(component)
		require.NoError(t, err)
		size += uint64(len(encoded))
	}
	return size
}

func TestBuildBlockEncodedBodyBudget(t *testing.T) {
	credentials := setupTestCredentials(t)
	for _, era := range []eraKind{
		eraShelley, eraAllegra, eraMary, eraAlonzo, eraBabbage, eraConway,
	} {
		for _, metadata := range []bool{false, true} {
			for _, count := range []int{1, 3, 24, 25, 256, 257} {
				transactions := make([]MempoolTransaction, count)
				for index := range transactions {
					transactions[index] = bodyBudgetTransaction(
						t, era, index, metadata,
					)
				}
				exactSize := bodyBudgetEncodedSize(t, era, transactions)
				for _, delta := range []int{-1, 0, 1} {
					t.Run(fmt.Sprintf(
						"era=%d/metadata=%t/count=%d/delta=%d",
						era, metadata, count, delta,
					), func(t *testing.T) {
						limit := uint(int(exactSize) + delta)
						builder := setupCredentialValidationBuilder(
							t,
							credentials,
						)
						builder.mempool = &mockMempool{
							transactions: transactions,
						}
						builder.pparamsProvider = &mockPParamsProvider{
							pparams: bodyBudgetParams(era, limit),
						}
						block, encoded, err := builder.BuildBlock(1001, 0)
						require.NoError(
							t,
							err,
							"size overflow must retain the fitting prefix",
						)
						wantCount := count
						if delta < 0 {
							wantCount--
						}
						require.Len(
							t,
							block.Transactions(),
							wantCount,
							"admission must use encoded bytes, not raw transaction sizes",
						)
						for index, transaction := range block.Transactions() {
							require.Equal(t, transactions[index].Hash,
								transaction.Hash().String())
						}
						var fields []cbor.RawMessage
						_, err = cbor.Decode(encoded, &fields)
						require.NoError(t, err)
						var wireSize uint64
						for _, field := range fields[1:] {
							wireSize += uint64(len(field))
						}
						require.Equal(t, bodyBudgetEncodedSize(
							t, era, transactions[:wantCount],
						), wireSize)
						require.Equal(t, wireSize, block.BlockBodySize())
						require.LessOrEqual(t, wireSize, uint64(limit))
						var actualMetadata map[uint]cbor.RawMessage
						_, err = cbor.Decode(fields[3], &actualMetadata)
						require.NoError(t, err)
						wantMetadata := 0
						if metadata {
							wantMetadata = wantCount
						}
						require.Len(t, actualMetadata, wantMetadata)
					})
				}
			}
		}
	}
}

func TestBuildBlockEncodedBudgetRetainsPrefixAfterSkippedTransaction(
	t *testing.T,
) {
	transactions := []MempoolTransaction{
		bodyBudgetTransaction(t, eraConway, 0, false),
		bodyBudgetTransaction(t, eraConway, 1, true),
		bodyBudgetTransaction(t, eraConway, 2, true),
		bodyBudgetTransaction(t, eraConway, 3, true),
	}
	selected := []MempoolTransaction{transactions[0], transactions[2]}
	limit := bodyBudgetEncodedSize(t, eraConway, selected)
	builder := setupCredentialValidationBuilder(t, setupTestCredentials(t))
	builder.mempool = &mockMempool{transactions: transactions}
	builder.pparamsProvider = &mockPParamsProvider{
		pparams: bodyBudgetParams(eraConway, uint(limit)),
	}
	builder.txValidator = &mockTxValidator{
		rejectHashes: map[string]struct{}{transactions[1].Hash: {}},
	}
	block, encoded, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 2)
	require.Equal(t, selected[0].Hash, block.Transactions()[0].Hash().String())
	require.Equal(t, selected[1].Hash, block.Transactions()[1].Hash().String())
	require.Equal(t, limit, block.BlockBodySize())
	var fields []cbor.RawMessage
	_, err = cbor.Decode(encoded, &fields)
	require.NoError(t, err)
	var metadata map[uint]cbor.RawMessage
	_, err = cbor.Decode(fields[3], &metadata)
	require.NoError(t, err)
	require.Len(t, metadata, 1)
	require.Contains(t, metadata, uint(1))
}
