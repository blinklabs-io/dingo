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
	"crypto/ed25519"
	"encoding/hex"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// forgeMempool is a fixed MempoolProvider that records which transactions
// forgeBlock evicted as confirmed.
type forgeMempool struct {
	mu      sync.Mutex
	pending []PendingTransaction
	removed []string
}

func (m *forgeMempool) Transactions() []PendingTransaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]PendingTransaction(nil), m.pending...)
}

func (m *forgeMempool) RemoveTxsByHash(hashes []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removed = append(m.removed, hashes...)
}

type forgeRevalidationFixture struct {
	ls      *LedgerState
	db      *database.Database
	mempool *forgeMempool
	key     ed25519.PrivateKey
	address lcommon.Address
	nextIn  byte
}

func newForgeRevalidationFixture(t *testing.T) *forgeRevalidationFixture {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x61
	key := ed25519.NewKeyFromSeed(seed)
	paymentHash := lcommon.Blake2b224Hash(key.Public().(ed25519.PublicKey))
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		paymentHash[:],
		nil,
	)
	require.NoError(t, err)

	pparams := &conway.ConwayProtocolParameters{
		ProtocolVersion:  lcommon.ProtocolParametersProtocolVersion{Major: 10},
		MaxTxSize:        16_384,
		MaxBlockBodySize: 90_112,
		MaxValueSize:     5_000,
		MaxBlockExUnits:  lcommon.ExUnits{Memory: 62_000_000, Steps: 20_000_000_000},
	}
	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: 1 << 40,
		EraId:         eras.ConwayEraDesc.Id,
	}
	config := newTestShelleyGenesisCfg(t)
	config.ShelleyGenesis().NetworkId = "Testnet"
	mempool := &forgeMempool{}
	ls := &LedgerState{
		db:                db,
		chain:             cm.PrimaryChain(),
		mempool:           mempool,
		activeEras:        []eras.EraDesc{eras.ConwayEraDesc},
		currentEra:        eras.ConwayEraDesc,
		currentEpoch:      epoch,
		epochCache:        []models.Epoch{epoch},
		currentPParams:    pparams,
		validationEnabled: true,
		config: LedgerStateConfig{
			CardanoNodeConfig: config,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
	return &forgeRevalidationFixture{
		ls: ls, db: db, mempool: mempool, key: key, address: address,
	}
}

// seedUtxo creates an unspent output owned by the fixture key and returns the
// input that spends it.
func (f *forgeRevalidationFixture) seedUtxo(
	t *testing.T,
	value uint64,
) shelley.ShelleyTransactionInput {
	t.Helper()
	f.nextIn++
	txId := bytes.Repeat([]byte{f.nextIn}, lcommon.Blake2b256Size)
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: f.address,
		OutputAmount:  value,
	}
	require.NoError(t, f.db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := f.db.CreateUtxo(txn, &models.Utxo{
			TxId:       txId,
			OutputIdx:  0,
			PaymentKey: f.address.PaymentKeyHash().Bytes(),
			AddedSlot:  1,
			Amount:     dbtypes.Uint64(value),
		}); err != nil {
			return err
		}
		encoded, err := cbor.Encode(output)
		if err != nil {
			return err
		}
		return f.db.Blob().SetUtxo(txn.Blob(), txId, 0, encoded)
	}))
	return shelley.ShelleyTransactionInput{
		TxId:        lcommon.NewBlake2b256(txId),
		OutputIndex: 0,
	}
}

// pendingSpend signs a transaction spending value from input with key and returns it in
// the form the mempool hands to forgeBlock.
func (f *forgeRevalidationFixture) pendingSpend(
	t *testing.T,
	input shelley.ShelleyTransactionInput,
	value, fee uint64,
	key ed25519.PrivateKey,
) PendingTransaction {
	t.Helper()
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAddress: f.address,
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: value - fee,
				},
			}},
			TxFee: fee,
		},
		TxIsValid: true,
	}
	bodyCbor, err := cbor.Encode(tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	hash := tx.Body.Id()
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      key.Public().(ed25519.PublicKey),
			Signature: ed25519.Sign(key, hash[:]),
		}},
		false,
	)
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	return PendingTransaction{
		Hash: tx.Hash().String(),
		Cbor: txCbor,
		Type: uint(conway.TxTypeConway),
	}
}

// forge runs one dev-mode forging round over the given mempool contents and
// returns the hashes of the transactions in the forged block.
func (f *forgeRevalidationFixture) forge(
	t *testing.T,
	pending ...PendingTransaction,
) []string {
	t.Helper()
	f.mempool.pending = pending
	f.ls.forgeBlock()
	tip := f.ls.chain.Tip()
	require.Equal(t, uint64(1), tip.BlockNumber, "a block must be forged")
	stored, err := f.ls.chain.BlockByPoint(tip.Point, nil)
	require.NoError(t, err)
	block, err := conway.NewConwayBlockFromCbor(stored.Cbor)
	require.NoError(t, err)
	hashes := make([]string, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		hashes = append(hashes, tx.Hash().String())
	}
	return hashes
}

// TestForgeBlockRevalidatesMempoolTransactions drives the dev-mode forging
// path with mempool contents that ledger validation accepts or rejects.
// Rejected transactions must be skipped without aborting the block or
// blocking later valid ones.
func TestForgeBlockRevalidatesMempoolTransactions(t *testing.T) {
	t.Parallel()
	const (
		value = 5_000_000
		fee   = 1_000
	)
	otherSeed := make([]byte, ed25519.SeedSize)
	otherSeed[0] = 0x62
	otherKey := ed25519.NewKeyFromSeed(otherSeed)

	t.Run("accepted", func(t *testing.T) {
		t.Parallel()
		f := newForgeRevalidationFixture(t)
		tx := f.pendingSpend(t, f.seedUtxo(t, value), value, fee, f.key)
		require.Equal(t, []string{tx.Hash}, f.forge(t, tx))
		require.Equal(t, []string{tx.Hash}, f.mempool.removed)
	})

	t.Run("rejected unknown input", func(t *testing.T) {
		t.Parallel()
		f := newForgeRevalidationFixture(t)
		missing := shelley.ShelleyTransactionInput{
			TxId: lcommon.NewBlake2b256(
				bytes.Repeat([]byte{0xee}, lcommon.Blake2b256Size),
			),
		}
		bad := f.pendingSpend(t, missing, value, fee, f.key)
		good := f.pendingSpend(t, f.seedUtxo(t, value), value, fee, f.key)
		require.Equal(t, []string{good.Hash}, f.forge(t, bad, good))
		require.Equal(t, []string{good.Hash}, f.mempool.removed)
	})

	t.Run("rejected wrong signer", func(t *testing.T) {
		t.Parallel()
		f := newForgeRevalidationFixture(t)
		bad := f.pendingSpend(t, f.seedUtxo(t, value), value, fee, otherKey)
		require.Empty(t, f.forge(t, bad))
		require.Empty(t, f.mempool.removed)
	})

	t.Run("mutated mempool double spend", func(t *testing.T) {
		t.Parallel()
		f := newForgeRevalidationFixture(t)
		in := f.seedUtxo(t, value)
		first := f.pendingSpend(t, in, value, fee, f.key)
		second := f.pendingSpend(t, in, value, 2*fee, f.key)
		require.Equal(t, []string{first.Hash}, f.forge(t, first, second))
	})

	t.Run("mutated mempool input spent by ledger", func(t *testing.T) {
		t.Parallel()
		f := newForgeRevalidationFixture(t)
		in := f.seedUtxo(t, value)
		queued := f.pendingSpend(t, in, value, fee, f.key)
		require.NoError(t, f.db.MarkUtxosDeletedAtSlot(
			nil,
			[]dbtypes.UtxoKey{{TxId: in.TxId.Bytes(), OutputIdx: 0}},
			2,
		))
		require.Empty(t, f.forge(t, queued))
		require.Empty(t, f.mempool.removed)
	})

	t.Run("dependent chain accepted in order", func(t *testing.T) {
		t.Parallel()
		f := newForgeRevalidationFixture(t)
		parent := f.pendingSpend(t, f.seedUtxo(t, value), value, fee, f.key)
		parentHash, err := hex.DecodeString(parent.Hash)
		require.NoError(t, err)
		child := f.pendingSpend(t, shelley.ShelleyTransactionInput{
			TxId: lcommon.NewBlake2b256(parentHash),
		}, value-fee, fee, f.key)
		require.Equal(
			t,
			[]string{parent.Hash, child.Hash},
			f.forge(t, parent, child),
		)
	})
}
