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

// Package leiosclosurefixture builds a certified Leios closure and projects
// the rows applying it writes, so the ledger's live apply and the IndexerDB
// writer are held to the same recorded result.
package leiosclosurefixture

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/*.golden
var golden embed.FS

// Slots of the announcing and the certifying ranking block.
const (
	AnnouncingSlot uint64 = 1000
	CertifyingSlot uint64 = 1010
	// BoundaryContextSlot is the slot a closure applied ahead of an epoch
	// boundary is dated at: the certifying block's parent.
	BoundaryContextSlot = AnnouncingSlot
	seedAmount          = 5_000_000
)

// Scenario is an announcing ranking block, a certifying child, and the
// endorser block the first announced: a transaction spending a seeded UTxO
// and a transaction spending its output.
type Scenario struct {
	SeedTxID     []byte
	Announcing   gledger.Block
	Certifying   gledger.Block
	EBHash       lcommon.Blake2b256
	ManifestCbor []byte
	Raw          []cbor.RawMessage
	// Txs holds the decoded transactions of Raw, in order.
	Txs []lcommon.Transaction
}

func paymentAddress() []byte {
	return append([]byte{0x60}, bytes.Repeat([]byte{0x11}, 28)...)
}

// Payloads are the datum, script and metadata the first transaction of a
// scenario built by NewWithPayloads carries in its witness set and auxiliary
// data.
var (
	PayloadDatum  = []byte{0xd8, 0x79, 0x9f, 0x01, 0xff}
	PayloadScript = []byte{0x01, 0x02, 0x03, 0x04}
)

func rawTx(spendTxID []byte, amount uint64) (cbor.RawMessage, error) {
	return buildRawTx(spendTxID, amount, false)
}

func buildRawTx(
	spendTxID []byte,
	amount uint64,
	payloads bool,
) (cbor.RawMessage, error) {
	fee := uint64(200_000)
	body, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{[]any{spendTxID, uint64(0)}}},
		1: []any{map[uint]any{0: paymentAddress(), 1: amount - fee}},
		2: fee,
	})
	if err != nil {
		return nil, err
	}
	witnesses := map[uint]any{}
	var aux any
	if payloads {
		witnesses[4] = []cbor.RawMessage{PayloadDatum}
		witnesses[6] = [][]byte{PayloadScript}
		aux = map[uint]any{674: "closure"}
	}
	tx, err := cbor.Encode(
		[]any{cbor.RawMessage(body), witnesses, aux, true},
	)
	return cbor.RawMessage(tx), err
}

// New builds the scenario.
func New() (*Scenario, error) { return build(false) }

// NewWithPayloads builds the scenario with a datum, a Plutus V2 script and
// metadata on the closure's first transaction, which live only in the endorser
// block's transaction list.
func NewWithPayloads() (*Scenario, error) { return build(true) }

func build(payloads bool) (*Scenario, error) {
	s := &Scenario{SeedTxID: bytes.Repeat([]byte{0x71}, 32)}
	first, err := buildRawTx(s.SeedTxID, seedAmount, payloads)
	if err != nil {
		return nil, err
	}
	firstTx, err := safedecode.DijkstraBlockTransaction(first)
	if err != nil {
		return nil, err
	}
	second, err := rawTx(firstTx.Hash().Bytes(), seedAmount-200_000)
	if err != nil {
		return nil, err
	}
	if err := s.endorse([]cbor.RawMessage{first, second}); err != nil {
		return nil, err
	}
	return s, nil
}

// NewWithTransactions builds the scenario with the given transactions in the
// endorser block, in order, and no seeded UTxO.
func NewWithTransactions(raw []cbor.RawMessage) (*Scenario, error) {
	s := &Scenario{}
	if err := s.endorse(raw); err != nil {
		return nil, err
	}
	return s, nil
}

// endorse puts raw in the endorser block and builds the two ranking blocks.
func (s *Scenario) endorse(raw []cbor.RawMessage) error {
	s.Raw = raw
	manifest := &lcommon.LeiosEndorserBlock{}
	for _, raw := range s.Raw {
		tx, err := safedecode.DijkstraBlockTransaction(raw)
		if err != nil {
			return err
		}
		s.Txs = append(s.Txs, tx)
		manifest.TransactionReferences = append(
			manifest.TransactionReferences,
			lcommon.LeiosTransactionReference{
				TransactionHash: lcommon.Blake2b256Hash(raw),
				TransactionSize: uint32(len(raw)), //nolint:gosec
			},
		)
	}
	s.EBHash = manifest.Hash()
	var err error
	if s.ManifestCbor, err = manifest.MarshalCBOR(); err != nil {
		return err
	}
	if s.Announcing, err = s.block(1, AnnouncingSlot, nil, false, true); err != nil {
		return err
	}
	s.Certifying, err = s.block(
		2, CertifyingSlot, s.Announcing.Hash().Bytes(), true, false,
	)
	return err
}

func (s *Scenario) block(
	number, slot uint64,
	prev []byte,
	certified, announce bool,
) (gledger.Block, error) {
	announcement, err := cbor.Encode(nil)
	if announce {
		announcement, err = cbor.Encode([]any{s.EBHash.Bytes(), uint64(4096)})
	}
	if err != nil {
		return nil, err
	}
	certifiedFlag, err := cbor.Encode(certified)
	if err != nil {
		return nil, err
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: number,
					Slot:        slot,
					PrevHash:    lcommon.NewBlake2b256(prev),
					VrfKey:      make([]byte, 32),
					VrfResult: lcommon.VrfResult{
						Output: make([]byte, 64),
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
				Signature: make([]byte, 448),
			},
		},
	}
	block.BlockHeader.LeiosHeaderExtension = []cbor.RawMessage{
		certifiedFlag, announcement,
	}
	if certified {
		block.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
			Signers:             []byte{0x80},
			AggregatedSignature: make([]byte, 48),
		}
	}
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	body, err := block.BlockBody.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	block.BlockHeader.Body.BlockBodySize = uint64(len(body))
	blockCbor, err := block.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	// A header hashes the CBOR it was decoded from, so a built block is
	// decoded back; before that every built block has one hash.
	return models.DecodeBlockCbor(uint(dijkstra.BlockTypeDijkstra), blockCbor)
}

// Model returns the stored form of a scenario block.
func Model(b gledger.Block) models.Block {
	return models.Block{
		Slot:     b.SlotNumber(),
		Hash:     b.Hash().Bytes(),
		PrevHash: b.PrevHash().Bytes(),
		Number:   b.BlockNumber(),
		Type:     uint(dijkstra.BlockTypeDijkstra),
		Cbor:     b.Cbor(),
	}
}

// Seed stores the UTxO the closure's first transaction spends, and the
// announcing block, certifying block and endorser-block data a reader
// resolves the closure from.
func (s *Scenario) Seed(t *testing.T, db *database.Database) {
	t.Helper()
	address := paymentAddress()
	outputCbor, err := cbor.Encode(
		map[uint]any{0: address, 1: uint64(seedAmount)},
	)
	require.NoError(t, err)
	require.NoError(t, db.Transaction(context.Background(), true).Do(
		func(txn *database.Txn) error {
			if err := db.CreateUtxo(context.Background(), txn, &models.Utxo{
				TxId:       s.SeedTxID,
				PaymentKey: address[1:],
				AddedSlot:  1,
				Amount:     types.Uint64(seedAmount),
				Cbor:       outputCbor,
			}); err != nil {
				return err
			}
			return db.Blob().SetUtxo(txn.Blob(), s.SeedTxID, 0, outputCbor)
		}))
}

// StoreChain stores the two ranking blocks and the endorser-block data.
func (s *Scenario) StoreChain(t *testing.T, db *database.Database) {
	t.Helper()
	require.NoError(t, db.BlockCreate(Model(s.Announcing), nil))
	require.NoError(t, db.BlockCreate(Model(s.Certifying), nil))
	require.NoError(t, db.SetLeiosEB(
		AnnouncingSlot, s.EBHash.Bytes(), s.ManifestCbor, s.Raw,
	))
}

const projectionQueries = `
SELECT 'tx ' || hex(t.hash) || ' slot=' || COALESCE(a.slot, '') ||
  ' index=' || COALESCE(a.block_index, '') || ' block=' || hex(a.block_hash)
FROM applied_transaction a JOIN "transaction" t ON t.id = a.tx_id
UNION ALL
SELECT 'utxo ' || hex(t.hash) || '#' || u.output_idx ||
  ' amount=' || COALESCE(u.amount, '') ||
  ' added=' || COALESCE(u.added_slot, '') ||
  ' deleted=' || COALESCE(u.deleted_slot, '') ||
  ' spent_by=' || COALESCE(hex(st.hash), '')
FROM utxo u JOIN "transaction" t ON t.id = u.tx_id
LEFT JOIN "transaction" st ON st.id = u.spent_at_tx_id
UNION ALL
SELECT 'context ' || hex(t.hash) || ' slot=' || c.slot
FROM leios_transaction_context c JOIN "transaction" t ON t.id = c.transaction_id
ORDER BY 1`

// Project renders the transaction, UTxO and closure-context rows of a
// metadata store, one line each, in a canonical order.
func Project(t *testing.T, raw *sql.DB) string {
	t.Helper()
	rows, err := raw.Query(projectionQueries)
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n") + "\n"
}

// Golden returns the rows the ledger's live apply of the closure writes. name
// is "boundary" for a closure applied ahead of an epoch boundary and
// "mid_epoch" for one applied inside an epoch.
func Golden(t *testing.T, name string) string {
	t.Helper()
	data, err := golden.ReadFile(fmt.Sprintf("testdata/%s.golden", name))
	require.NoError(t, err)
	return string(data)
}
