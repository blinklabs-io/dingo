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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

const (
	parameterChangeInputValue    = uint64(2_000_000_000)
	parameterChangeProposalValue = uint64(1_000_000_000)
	parameterChangeBlockSlot     = uint64(10)
)

type parameterChangeFixture struct {
	db         *database.Database
	ls         *LedgerState
	tx         *gdijkstra.DijkstraTransaction
	txCbor     []byte
	block      *gdijkstra.DijkstraBlock
	blockCbor  []byte
	offsets    *database.BlockIngestionResult
	originHash []byte
	pparams    *gdijkstra.DijkstraProtocolParameters
}

// newParameterChangeFixture builds a funded, signed Dijkstra transaction that
// carries one ParameterChange proposal with update map ppu, a block holding
// it, and a ledger state that validates and applies it.
func newParameterChangeFixture(
	t *testing.T,
	ppu map[uint]any,
) *parameterChangeFixture {
	t.Helper()
	db := newTestDB(t)
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x93
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	paymentHash := lcommon.Blake2b224Hash(publicKey)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		paymentHash[:],
		nil,
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)

	inputTxID := bytes.Repeat([]byte{0x94}, lcommon.Blake2b256Size)
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.CreateUtxo(txn, &models.Utxo{
			TxId:       inputTxID,
			OutputIdx:  0,
			PaymentKey: paymentHash.Bytes(),
			AddedSlot:  1,
			Amount:     dbtypes.Uint64(parameterChangeInputValue),
		}); err != nil {
			return err
		}
		encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: address,
			OutputAmount:  parameterChangeInputValue,
		})
		if err != nil {
			return err
		}
		return db.Blob().SetUtxo(txn.Blob(), inputTxID, 0, encoded)
	}))

	rewardAccount := append(
		[]byte{0xe0},
		bytes.Repeat([]byte{0x95}, lcommon.Blake2b224Size)...,
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    rewardAccount[1:],
		CredentialTag: 0,
		Active:        true,
	}))
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/constitution",
		AnchorHash: bytes.Repeat([]byte{0x96}, lcommon.Blake2b256Size),
		AddedSlot:  0,
	}, nil))
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{inputTxID, uint64(0)}},
		},
		1: []any{[]any{
			addressBytes,
			parameterChangeInputValue - parameterChangeProposalValue,
		}},
		2: uint64(0),
		20: []any{[]any{
			parameterChangeProposalValue,
			rewardAccount,
			[]any{
				uint64(lcommon.GovActionTypeParameterChange),
				nil,
				ppu,
				nil,
			},
			[]any{"https://example.invalid/update", make([]byte, 32)},
		}},
	}
	bodyCbor, err := cbor.Encode(body)
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(bodyCbor)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{0: []any{[]any{
			[]byte(publicKey),
			ed25519.Sign(privateKey, bodyHash.Bytes()),
		}}},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)

	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	pparams.GovActionDeposit = parameterChangeProposalValue
	pparams.GovActionValidityPeriod = 6
	originHash := bytes.Repeat([]byte{0xf3}, lcommon.Blake2b256Size)
	originTip := ochainsync.Tip{Point: ocommon.Point{Slot: 1, Hash: originHash}}
	require.NoError(t, db.SetTip(originTip, nil))
	config := newTestShelleyGenesisCfg(t)
	config.ShelleyGenesis().NetworkId = "Testnet"
	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1,
		LengthInSlots: 1_000,
		EraId:         eras.DijkstraEraDesc.Id,
	}
	ls := &LedgerState{
		db:                   db,
		activeEras:           []eras.EraDesc{eras.DijkstraEraDesc},
		currentEra:           eras.DijkstraEraDesc,
		currentEpoch:         epoch,
		epochCache:           []models.Epoch{epoch},
		currentPParams:       pparams,
		currentTip:           originTip,
		currentTipBlockNonce: bytes.Repeat([]byte{0xf2}, lcommon.Blake2b256Size),
		validationEnabled:    true,
		config: LedgerStateConfig{
			CardanoNodeConfig: config,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()

	block := &gdijkstra.DijkstraBlock{
		BlockHeader: &gdijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        parameterChangeBlockSlot,
					PrevHash:    lcommon.NewBlake2b256(originHash),
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
		BlockBody: gdijkstra.DijkstraBlockBody{
			Transactions: []gdijkstra.DijkstraTransaction{*tx},
		},
	}
	blockBodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(blockBodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	point := ocommon.Point{Slot: parameterChangeBlockSlot, Hash: block.Hash().Bytes()}
	offsets, err := database.NewBlockIndexer(point.Slot, point.Hash).
		ComputeOffsets(blockCbor, block)
	require.NoError(t, err)

	return &parameterChangeFixture{
		db:         db,
		ls:         ls,
		tx:         tx,
		txCbor:     txCbor,
		block:      block,
		blockCbor:  blockCbor,
		offsets:    offsets,
		originHash: originHash,
		pparams:    pparams,
	}
}

func (fx *parameterChangeFixture) proposalStored(t *testing.T) bool {
	t.Helper()
	_, err := fx.db.GetGovernanceProposal(fx.tx.Id().Bytes(), 0, nil)
	if err == nil {
		return true
	}
	require.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
	return false
}

func (fx *parameterChangeFixture) admitToMempool(t *testing.T) error {
	t.Helper()
	pool, err := dingomempool.NewMempool(dingomempool.MempoolConfig{
		Validator:       fx.ls,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, pool.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})
	err = pool.AddTransaction(uint(gdijkstra.TxTypeDijkstra), fx.txCbor)
	if err == nil {
		require.Len(t, pool.Transactions(), 1)
	} else {
		require.Empty(t, pool.Transactions())
	}
	return err
}

func (fx *parameterChangeFixture) applyLiveBlock(t *testing.T) error {
	t.Helper()
	point := ocommon.Point{
		Slot: fx.block.SlotNumber(),
		Hash: fx.block.Hash().Bytes(),
	}
	return fx.db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := fx.ls.ledgerProcessBlock(
			txn,
			point,
			fx.block,
			true,
			false,
			false,
			fx.originHash,
			envelopeParent{origin: true},
			fx.offsets,
			eras.DijkstraEraDesc,
			fx.pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

// replayBlock replays the block from the chain. A trusted replay applies it
// without transaction validation, as fast sync and historical backfill do.
func (fx *parameterChangeFixture) replayBlock(
	t *testing.T,
	trusted bool,
) error {
	t.Helper()
	if trusted {
		fx.ls.config.TrustedReplay = true
		fx.ls.validationEnabled = false
		fx.ls.publishSnapshotsLocked()
	}
	point := ocommon.Point{
		Slot: fx.block.SlotNumber(),
		Hash: fx.block.Hash().Bytes(),
	}
	require.NoError(t, fx.db.BlockCreate(models.Block{
		Slot:     point.Slot,
		Hash:     point.Hash,
		PrevHash: fx.originHash,
		Number:   fx.block.BlockNumber(),
		Type:     gledger.BlockTypeDijkstra,
		Cbor:     fx.blockCbor,
	}, nil))
	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	results <- readChainResult{
		blocks: []gledger.Block{fx.block},
		done:   done,
	}
	close(results)
	return fx.ls.ledgerProcessBlocksFromSource(t.Context(), results)
}

// TestParameterChangeDecisionsAgreeAcrossPaths submits the same ParameterChange
// through mempool admission, live block application, and replay both with and
// without transaction validation. Every path must reach the
// same decision, and a refused proposal must leave no governance_proposal row
// behind.
func TestParameterChangeDecisionsAgreeAcrossPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ppu     map[uint]any
		wantErr string
	}{
		{
			name:    "zero govActionDeposit",
			ppu:     map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 0},
			wantErr: "govActionDeposit",
		},
		{
			name:    "zero coinsPerUTxOByte",
			ppu:     map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
			wantErr: "coinsPerUTxOByte",
		},
		{
			name: "nonzero govActionDeposit",
			ppu:  map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 1},
		},
	}
	for _, test := range tests {
		assertDecision := func(t *testing.T, fx *parameterChangeFixture, err error) {
			t.Helper()
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.False(t, fx.proposalStored(t))
				return
			}
			require.NoError(t, err)
		}
		t.Run(test.name+"/mempool admission", func(t *testing.T) {
			t.Parallel()
			fx := newParameterChangeFixture(t, test.ppu)
			assertDecision(t, fx, fx.admitToMempool(t))
			require.False(t, fx.proposalStored(t))
		})
		t.Run(test.name+"/live block", func(t *testing.T) {
			t.Parallel()
			fx := newParameterChangeFixture(t, test.ppu)
			assertDecision(t, fx, fx.applyLiveBlock(t))
			if test.wantErr == "" {
				require.True(t, fx.proposalStored(t))
			}
		})
		for _, trusted := range []bool{false, true} {
			name := test.name + "/replay"
			if trusted {
				name = test.name + "/trusted replay"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				fx := newParameterChangeFixture(t, test.ppu)
				assertDecision(t, fx, fx.replayBlock(t, trusted))
				if test.wantErr == "" {
					require.True(t, fx.proposalStored(t))
				}
			})
		}
	}
}
