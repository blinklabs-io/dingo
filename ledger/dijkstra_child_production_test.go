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
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	childProdTopAmount   = uint64(10_000_000)
	childProdChildAmount = uint64(5_000_000)
	childProdFee         = uint64(1_000_000)
)

var (
	childProdTopInput   = bytes.Repeat([]byte{0x81}, 32)
	childProdChildInput = bytes.Repeat([]byte{0x82}, 32)
	childProdMissing    = bytes.Repeat([]byte{0x8f}, 32)
)

// childProdKeys is the payment key that funds both levels of a batch.
type childProdKeys struct {
	private ed25519.PrivateKey
	address []byte
}

func newChildProdKeys(t *testing.T) childProdKeys {
	t.Helper()
	private := ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{0x93}, ed25519.SeedSize),
	)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		lcommon.Blake2b224Hash(private.Public().(ed25519.PublicKey)).Bytes(),
		nil,
	)
	require.NoError(t, err)
	raw, err := address.Bytes()
	require.NoError(t, err)
	return childProdKeys{private: private, address: raw}
}

func (k childProdKeys) witnesses(bodyCbor []byte) map[uint]any {
	id := lcommon.Blake2b256Hash(bodyCbor)
	return map[uint]any{0: []any{[]any{
		[]byte(k.private.Public().(ed25519.PublicKey)),
		ed25519.Sign(k.private, id.Bytes()),
	}}}
}

func (k childProdKeys) output(amount uint64) map[uint]any {
	return map[uint]any{0: k.address, 1: amount}
}

// childProdBatch is one child and the top level's auxiliary data. The child
// spends childProdChildInput into one output of the same amount unless body
// overrides those fields. sibling, when set, is a second child given the
// first child's body ID.
type childProdBatch struct {
	body    func(childProdKeys) map[uint]any
	aux     []byte
	topAux  []byte
	sibling func(keys childProdKeys, firstID []byte) map[uint]any
	// redeemers, when set, is the child witness set's redeemer field.
	redeemers any
}

func (b childProdBatch) build(
	t *testing.T,
	keys childProdKeys,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	childBody := map[uint]any{
		0: []any{[]any{childProdChildInput, uint64(0)}},
		1: []any{keys.output(childProdChildAmount)},
	}
	if b.body != nil {
		for key, value := range b.body(keys) {
			childBody[key] = value
		}
	}
	childBodyCbor, err := cbor.Encode(childBody)
	require.NoError(t, err)
	var childAux any
	if b.aux != nil {
		childAux = cbor.RawMessage(b.aux)
	}
	childWitnesses := keys.witnesses(childBodyCbor)
	if b.redeemers != nil {
		childWitnesses[5] = b.redeemers
	}
	child, err := cbor.Encode([]any{
		cbor.RawMessage(childBodyCbor),
		childWitnesses,
		childAux,
	})
	require.NoError(t, err)
	children := []cbor.RawMessage{child}
	if b.sibling != nil {
		firstID := lcommon.Blake2b256Hash(childBodyCbor)
		siblingCbor, err := cbor.Encode(b.sibling(keys, firstID.Bytes()))
		require.NoError(t, err)
		sibling, err := cbor.Encode([]any{
			cbor.RawMessage(siblingCbor),
			keys.witnesses(siblingCbor),
			nil,
		})
		require.NoError(t, err)
		children = append(children, sibling)
	}
	topBody := map[uint]any{
		0:  []any{[]any{childProdTopInput, uint64(0)}},
		1:  []any{keys.output(childProdTopAmount - childProdFee)},
		2:  childProdFee,
		23: cbor.NewSetType(children, true),
	}
	var topAux any
	if b.topAux != nil {
		topBody[7] = lcommon.Blake2b256Hash(b.topAux).Bytes()
		topAux = cbor.RawMessage(b.topAux)
	}
	topBodyCbor, err := cbor.Encode(topBody)
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(topBodyCbor),
		keys.witnesses(topBodyCbor),
		topAux,
	})
	require.NoError(t, err)
	tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return tx
}

type childProdFixture struct {
	db       *database.Database
	ls       *LedgerState
	tx       *gdijkstra.DijkstraTransaction
	block    *gdijkstra.DijkstraBlock
	offsets  *database.BlockIngestionResult
	startTip ochainsync.Tip
}

func childProdParams() *gdijkstra.DijkstraProtocolParameters {
	params := dijkstraTestProtocolParameters()
	params.MaxBlockBodySize = 2_000_000
	params.MaxBlockHeaderSize = 100_000
	params.MaxTxSize = 65_536
	params.AdaPerUtxoByte = 4_310
	params.ExecutionCosts = lcommon.ExUnitPrice{
		MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1)},
		StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	params.MaxTxExUnits = lcommon.ExUnits{Memory: 10_000_000, Steps: 10_000_000}
	params.MaxBlockExUnits = lcommon.ExUnits{
		Memory: 50_000_000,
		Steps:  50_000_000,
	}
	return params
}

func newChildProdFixture(
	t *testing.T,
	tx *gdijkstra.DijkstraTransaction,
	keys childProdKeys,
) *childProdFixture {
	t.Helper()
	db := newTestDB(t)
	for _, seed := range []struct {
		id     []byte
		amount uint64
	}{
		{childProdTopInput, childProdTopAmount},
		{childProdChildInput, childProdChildAmount},
	} {
		outputCbor, err := cbor.Encode(keys.output(seed.amount))
		require.NoError(t, err)
		require.NoError(
			t,
			db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
				if err := db.CreateUtxo(context.Background(), txn, &models.Utxo{
					TxId:      seed.id,
					OutputIdx: 0,
				}); err != nil {
					return err
				}
				return db.Blob().SetUtxo(txn.Blob(), seed.id, 0, outputCbor)
			}),
		)
	}
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db: db,
		activeEras: []eras.EraDesc{
			eras.ConwayEraDesc,
			eras.DijkstraEraDesc,
		},
		currentEra:     eras.DijkstraEraDesc,
		currentPParams: childProdParams(),
		currentEpoch: models.Epoch{
			SlotLength:    1_000,
			LengthInSlots: 1_000,
			EraId:         gdijkstra.EraIdDijkstra,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.publishSnapshotsLocked()

	block := newDijkstraCollateralReturnBlock(t, tx)
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets, err := database.NewBlockIndexer(
		block.SlotNumber(), block.Hash().Bytes(),
	).ComputeOffsets(block.Cbor(), block)
	require.NoError(t, err)
	return &childProdFixture{
		db:      db,
		ls:      ls,
		tx:      tx,
		block:   block,
		offsets: offsets,
		startTip: ochainsync.Tip{Point: ocommon.Point{
			Slot: 1,
			Hash: []byte("pre-existing-tip"),
		}},
	}
}

func (f *childProdFixture) admit(t *testing.T) error {
	t.Helper()
	pool, err := dingomempool.NewMempool(dingomempool.MempoolConfig{
		Validator:       f.ls,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MempoolCapacity: 1 << 20,
	})
	require.NoError(t, err)
	require.NoError(t, pool.Start(context.Background()))
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	}()
	return pool.AddTransaction(uint(gdijkstra.TxTypeDijkstra), f.tx.Cbor())
}

func (f *childProdFixture) importBlock() error {
	return f.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		_, err := f.ls.ledgerProcessBlock(
			context.Background(),
			txn,
			ocommon.NewPoint(f.block.SlotNumber(), f.block.Hash().Bytes()),
			f.block,
			true, false, false,
			nil,
			envelopeParent{origin: true},
			f.offsets,
			eras.DijkstraEraDesc,
			f.ls.currentPParams,
			nil,
			0, 0, false,
		)
		return err
	})
}

func (f *childProdFixture) live(t *testing.T, id []byte, index uint32) bool {
	t.Helper()
	utxo, err := f.db.Metadata().GetUtxo(id, index, nil)
	require.NoError(t, err)
	return utxo != nil
}

func childProdChildID(tx *gdijkstra.DijkstraTransaction) []byte {
	id := tx.Body.TxSubTransactions.Items()[0].Body.Id()
	return id.Bytes()
}

func outputsHoldAmount(
	outputs []lcommon.TransactionOutput,
	amount uint64,
) bool {
	for _, output := range outputs {
		if output.Amount() != nil && output.Amount().Uint64() == amount {
			return true
		}
	}
	return false
}

// childProdRejection is a child-level predicate, matched on the error the
// rule reports for the child rather than on any rejection.
type childProdRejection struct {
	name    string
	batch   childProdBatch
	matches func(error) bool
	// replayRecovers marks a verdict replay treats as deterministic: it drops
	// the block from the chain and restarts rather than returning the error.
	replayRecovers bool
}

// childProdErrorAs matches an error of type T. Each call declares its own
// target because every case runs as parallel subtests that share the matcher.
func childProdErrorAs[T error]() func(error) bool {
	return func(err error) bool {
		var target T
		return errors.As(err, &target)
	}
}

func childProdRejections(t *testing.T) []childProdRejection {
	t.Helper()
	childAux := []byte{0xa1, 0x00, 0x01}
	childHash := lcommon.Blake2b256Hash(childAux)
	wrongHash := lcommon.Blake2b256{0xff}
	// Label 0 holding a 65-byte text exceeds the metadata text limit.
	longAux := append(
		[]byte{0xa1, 0x00, 0x78, 0x41},
		bytes.Repeat([]byte{'x'}, 65)...,
	)
	longHash := lcommon.Blake2b256Hash(longAux)
	mainnetAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkMainnet,
		bytes.Repeat([]byte{0x31}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	mainnetRaw, err := mainnetAddress.Bytes()
	require.NoError(t, err)
	// The derivation-path attribute holds a CBOR byte string.
	derivationPath, err := cbor.Encode(bytes.Repeat([]byte{0x22}, 100))
	require.NoError(t, err)
	byronAddress, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		bytes.Repeat([]byte{0x11}, lcommon.AddressHashSize),
		lcommon.ByronAddressAttributes{Payload: derivationPath},
	)
	require.NoError(t, err)
	byronRaw, err := byronAddress.Bytes()
	require.NoError(t, err)
	assets := make(map[cbor.ByteString]uint64, 200)
	for i := range 200 {
		name := bytes.Repeat([]byte{byte(i)}, 32)
		assets[cbor.NewByteString(name)] = 1
	}
	const tooSmall = uint64(1)
	const bigValue = uint64(4_999_999)

	return []childProdRejection{
		{
			name: "child output below minimum UTxO",
			batch: childProdBatch{body: func(k childProdKeys) map[uint]any {
				return map[uint]any{1: []any{
					k.output(tooSmall),
					k.output(childProdChildAmount - tooSmall),
				}}
			}},
			matches: func(err error) bool {
				var target shelley.OutputTooSmallUtxoError
				return errors.As(err, &target) &&
					outputsHoldAmount(target.Outputs, tooSmall)
			},
		},
		{
			name: "child output above maximum value size",
			batch: childProdBatch{body: func(k childProdKeys) map[uint]any {
				return map[uint]any{1: []any{map[uint]any{
					0: k.address,
					1: []any{bigValue, map[cbor.ByteString]any{
						cbor.NewByteString(bytes.Repeat([]byte{0x44}, 28)): assets,
					}},
				}}}
			}},
			matches: func(err error) bool {
				var target mary.OutputTooBigUtxoError
				return errors.As(err, &target) &&
					outputsHoldAmount(target.Outputs, bigValue)
			},
		},
		{
			name: "child validity interval not yet open",
			batch: childProdBatch{body: func(childProdKeys) map[uint]any {
				return map[uint]any{8: uint64(1_000_000)}
			}},
			matches: func(err error) bool {
				var target allegra.OutsideValidityIntervalUtxoError
				return errors.As(err, &target) &&
					target.ValidityIntervalStart == 1_000_000
			},
		},
		{
			name: "child output on the wrong network",
			batch: childProdBatch{body: func(childProdKeys) map[uint]any {
				return map[uint]any{1: []any{map[uint]any{
					0: mainnetRaw, 1: childProdChildAmount,
				}}}
			}},
			matches: func(err error) bool {
				var target shelley.WrongNetworkError
				if !errors.As(err, &target) {
					return false
				}
				for _, addr := range target.Addrs {
					if addr.String() == mainnetAddress.String() {
						return true
					}
				}
				return false
			},
		},
		{
			name: "child bootstrap output attributes too big",
			batch: childProdBatch{body: func(childProdKeys) map[uint]any {
				return map[uint]any{1: []any{map[uint]any{
					0: byronRaw, 1: childProdChildAmount,
				}}}
			}},
			matches: func(err error) bool {
				var target shelley.OutputBootAddrAttrsTooBigError
				return errors.As(err, &target) && len(target.Outputs) == 1
			},
		},
		{
			name: "child spends a missing input",
			batch: childProdBatch{body: func(childProdKeys) map[uint]any {
				return map[uint]any{0: []any{
					[]any{childProdMissing, uint64(0)},
				}}
			}},
			matches: func(err error) bool {
				var target shelley.BadInputsUtxoError
				if !errors.As(err, &target) {
					return false
				}
				for _, input := range target.Inputs {
					if bytes.Equal(input.Id().Bytes(), childProdMissing) {
						return true
					}
				}
				return false
			},
		},
		{
			name: "child spends an earlier child's output",
			batch: childProdBatch{sibling: func(
				k childProdKeys,
				firstID []byte,
			) map[uint]any {
				return map[uint]any{
					0: []any{[]any{firstID, uint64(0)}},
					1: []any{k.output(childProdChildAmount)},
				}
			}},
			matches: func(err error) bool {
				var target shelley.BadInputsUtxoError
				if !errors.As(err, &target) {
					return false
				}
				// The only input not seeded in the ledger is the first
				// child's output.
				for _, input := range target.Inputs {
					id := input.Id().Bytes()
					if !bytes.Equal(id, childProdTopInput) &&
						!bytes.Equal(id, childProdChildInput) {
						return true
					}
				}
				return false
			},
		},
		{
			name: "two children spend the same input",
			batch: childProdBatch{sibling: func(
				k childProdKeys,
				_ []byte,
			) map[uint]any {
				return map[uint]any{
					0: []any{[]any{childProdChildInput, uint64(0)}},
					1: []any{k.output(childProdChildAmount - 1)},
				}
			}},
			matches: func(err error) bool {
				var target shelley.DuplicateInputError
				return errors.As(err, &target)
			},
			replayRecovers: true,
		},
		{
			// The child pays no fee of its own: its declared execution units
			// raise the minimum fee of the enclosing transaction.
			name: "child execution units exceed the top-level fee",
			batch: childProdBatch{redeemers: map[any]any{
				[2]uint64{uint64(lcommon.RedeemerTagSpend), 0}: []any{
					uint64(0), []uint64{childProdFee, childProdFee},
				},
			}},
			matches: func(err error) bool {
				var target shelley.FeeTooSmallUtxoError
				return errors.As(err, &target) &&
					target.Provided.Uint64() == childProdFee &&
					target.Min.Uint64() >= 2*childProdFee
			},
		},
		{
			name:    "child metadata without a hash",
			batch:   childProdBatch{aux: childAux},
			matches: childProdErrorAs[lcommon.MissingTransactionAuxiliaryDataHashError](),
		},
		{
			name: "child hash without metadata",
			batch: childProdBatch{body: func(childProdKeys) map[uint]any {
				return map[uint]any{7: childHash.Bytes()}
			}},
			matches: childProdErrorAs[lcommon.MissingTransactionMetadataError](),
		},
		{
			name: "child hash mismatching its metadata",
			batch: childProdBatch{
				body: func(childProdKeys) map[uint]any {
					return map[uint]any{7: wrongHash.Bytes()}
				},
				aux: childAux,
			},
			matches: childProdErrorAs[lcommon.ConflictingMetadataHashError](),
		},
		{
			name: "top-level metadata does not satisfy the child hash",
			batch: childProdBatch{
				body: func(childProdKeys) map[uint]any {
					return map[uint]any{7: childHash.Bytes()}
				},
				topAux: childAux,
			},
			matches: childProdErrorAs[lcommon.MissingTransactionMetadataError](),
		},
		{
			name: "malformed child metadata",
			batch: childProdBatch{
				body: func(childProdKeys) map[uint]any {
					return map[uint]any{7: longHash.Bytes()}
				},
				aux: longAux,
			},
			matches: func(err error) bool {
				return err != nil &&
					strings.Contains(err.Error(), "metadata text exceeds")
			},
		},
	}
}

// TestDijkstraChildPredicatesRejectedOnEveryProductionPath drives a batch
// whose only defect is in its child through mempool admission, imported-block
// validation, forged-block revalidation and replay, before and after a
// rollback. Each path must report the child's own predicate and leave both
// inputs and the tip untouched. A phase-2-invalid batch still runs the
// child's structural predicates.
func TestDijkstraChildPredicatesRejectedOnEveryProductionPath(t *testing.T) {
	t.Parallel()
	keys := newChildProdKeys(t)
	for _, tc := range childProdRejections(t) {
		for _, valid := range []bool{true, false} {
			name := tc.name
			if !valid {
				name += " in a phase-2-invalid batch"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				tx := tc.batch.build(t, keys)
				tx.TxIsValid = valid
				f := newChildProdFixture(t, tx, keys)
				require.NoError(t, f.db.SetTip(f.startTip, nil))
				if valid {
					err := f.admit(t)
					require.Truef(t, tc.matches(err), "mempool: %v", err)
				}
				err := f.importBlock()
				require.Truef(t, tc.matches(err), "imported block: %v", err)
				err = f.ls.validateForgedTxs(context.Background(), f.block)
				require.Truef(t, tc.matches(err), "forged block: %v", err)
				tip, err := f.db.GetTip(nil)
				require.NoError(t, err)
				require.Equal(t, f.startTip, tip)
				require.True(t, f.live(t, childProdTopInput, 0))
				require.True(t, f.live(t, childProdChildInput, 0))

				replay := newReplayTestLedger(
					t, f.db, f.block, uint(gledger.BlockTypeDijkstra),
					eras.DijkstraEraDesc, childProdParams(),
				)
				replayRejects := func(stage string) {
					t.Helper()
					err := replayTestBlock(replay, f.block)
					if tc.replayRecovers {
						require.ErrorIsf(
							t,
							err,
							errRestartLedgerPipeline,
							"%s",
							stage,
						)
						require.NotEqual(
							t,
							f.block.Hash().Bytes(),
							replay.chain.Tip().Point.Hash,
							"%s: recovery drops the block from the chain",
							stage,
						)
						return
					}
					require.Truef(t, tc.matches(err), "%s: %v", stage, err)
				}
				replayRejects("replay")
				if tc.replayRecovers {
					// The recovery was the rollback; the inputs stay put.
					require.True(t, f.live(t, childProdTopInput, 0))
					require.True(t, f.live(t, childProdChildInput, 0))
					return
				}
				require.NoError(t, replay.chain.Rollback(context.Background(), ocommon.Point{}))
				require.NoError(t, replay.chain.AddRawBlocks(context.Background(), []chain.RawBlock{{
					Slot:        f.block.SlotNumber(),
					Hash:        f.block.Hash().Bytes(),
					BlockNumber: f.block.BlockNumber(),
					Type:        uint(gledger.BlockTypeDijkstra),
					PrevHash:    f.block.PrevHash().Bytes(),
					Cbor:        f.block.Cbor(),
				}}))
				replayRejects("replay after rollback")
				require.True(t, f.live(t, childProdTopInput, 0))
				require.True(t, f.live(t, childProdChildInput, 0))
			})
		}
	}
}

// TestDijkstraChildBatchAcceptedOnEveryProductionPath is the control: the
// same batch with a sound child, with and without distinct child and
// top-level metadata, is admitted, validated, applied and replayed, spending
// both inputs and creating the child's output.
func TestDijkstraChildBatchAcceptedOnEveryProductionPath(t *testing.T) {
	t.Parallel()
	keys := newChildProdKeys(t)
	childAux := []byte{0xa1, 0x00, 0x01}
	childHash := lcommon.Blake2b256Hash(childAux)
	for name, batch := range map[string]childProdBatch{
		"sound child": {},
		"distinct child and top-level metadata": {
			body: func(childProdKeys) map[uint]any {
				return map[uint]any{7: childHash.Bytes()}
			},
			aux:    childAux,
			topAux: []byte{0xa1, 0x00, 0x02},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tx := batch.build(t, keys)
			f := newChildProdFixture(t, tx, keys)
			require.NoError(t, f.admit(t))
			require.NoError(t, f.ls.validateForgedTxs(context.Background(), f.block))

			replay := newReplayTestLedger(
				t, f.db, f.block, uint(gledger.BlockTypeDijkstra),
				eras.DijkstraEraDesc, childProdParams(),
			)
			require.NoError(t, replayTestBlock(replay, f.block))
			require.False(t, f.live(t, childProdTopInput, 0))
			require.False(t, f.live(t, childProdChildInput, 0))
			require.True(t, f.live(t, childProdChildID(tx), 0))
			require.True(t, f.live(t, tx.Hash().Bytes(), 0))
		})
	}
}
