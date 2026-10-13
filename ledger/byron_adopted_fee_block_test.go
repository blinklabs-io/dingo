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
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// processByronBlockWithPParams applies one Byron block holding tx through
// ledgerProcessBlock, validating it against pparams as block application
// does with the parameters adopted for the block's epoch.
func processByronBlockWithPParams(
	t *testing.T,
	db *database.Database,
	nodeConfig *cardano.CardanoNodeConfig,
	tx lcommon.Transaction,
	pparams *eras.ByronProtocolParameters,
) error {
	t.Helper()
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
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
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  1,
				ByteLength: uint32(len(tx.Cbor())), // #nosec G115
			},
		},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	for _, utxo := range tx.Produced() {
		offsets.UtxoOffsets[database.UtxoRef{
			TxId:      txHash,
			OutputIdx: utxo.Id.Index(),
		}] = database.CborOffset{BlockSlot: 1, ByteLength: 1}
	}
	return db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(context.Background(),
			txn,
			ocommon.Point{Slot: 1, Hash: block.Hash().Bytes()},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			offsets,
			eras.ByronEraDesc,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

// TestLedgerProcessBlockByronAdoptedFeePolicy covers through block
// application: a real, signed Byron transaction paying a 200 lovelace fee is
// judged by the fee policy adopted for the block, which ledgerProcessBlock
// receives as pparams, and not by the genesis policy. Each case changes the
// summand or the multiplier so that the adopted policy and the genesis one
// disagree about the same transaction.
func TestLedgerProcessBlockByronAdoptedFeePolicy(t *testing.T) {
	t.Parallel()
	const (
		protocolMagic = 764824073
		fee           = 200
		nanoPerUnit   = 1_000_000_000
	)
	keyA := newByronBlockTestKey(t, 0x71)
	payTo := newByronBlockTestKey(t, 0x72).address
	build := func(t *testing.T, db *database.Database) *byron.ByronTransaction {
		a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
		return buildByronBlockTestTx(t, protocolMagic,
			[]byronBlockTestInput{{a, 0}},
			[]byronBlockTestOutput{{payTo, 1_000 - fee}},
			nil, []byronBlockTestKey{keyA})
	}
	size := eras.TxSizeForFee(build(t, newTestDB(t)))
	require.Positive(t, size)
	// perByteNano makes the multiplier alone require exactly fee lovelace or
	// slightly less, so doubling it requires more than fee.
	perByteNano := int64(fee * nanoPerUnit / size)

	nodeConfigFor := func(
		t *testing.T,
		summand, multiplier int64,
	) *cardano.CardanoNodeConfig {
		t.Helper()
		nodeConfig := &cardano.CardanoNodeConfig{}
		genesisJSON := fmt.Sprintf(
			`{"blockVersionData": {"slotDuration": "20000", `+
				`"maxTxSize": "4096", "txFeePolicy": `+
				`{"summand": "%d", "multiplier": "%d"}}, `+
				`"protocolConsts": {"k": 2160, "protocolMagic": %d}}`,
			summand, multiplier, protocolMagic,
		)
		require.NoError(t, loadByronGenesisForTest(
			t, nodeConfig, strings.NewReader(genesisJSON),
		))
		return nodeConfig
	}
	adopt := func(
		t *testing.T,
		nodeConfig *cardano.CardanoNodeConfig,
		summandNano, multiplierNano int64,
	) *eras.ByronProtocolParameters {
		t.Helper()
		genesis, err := eras.NewByronProtocolParametersFromGenesis(
			nodeConfig.ByronGenesis(),
		)
		require.NoError(t, err)
		adopted, err := genesis.ApplyUpdate(
			byron.ByronUpdateProposalBlockVersionMod{
				TxFeePolicy: []byron.ByronTxFeePolicy{{
					SummandNano:    big.NewInt(summandNano),
					MultiplierNano: big.NewInt(multiplierNano),
				}},
			},
		)
		require.NoError(t, err)
		return adopted
	}

	tests := []struct {
		name string
		// genesis is the Byron genesis fee policy {summand, multiplier} in
		// nano-lovelace; adopted is the policy an update adopted.
		genesis, adopted [2]int64
		wantRejected     bool
	}{
		{
			name:    "lowered summand accepts what genesis rejects",
			genesis: [2]int64{(fee + 1) * nanoPerUnit, 0},
			adopted: [2]int64{fee * nanoPerUnit, 0},
		},
		{
			name:         "raised summand rejects what genesis accepts",
			genesis:      [2]int64{fee * nanoPerUnit, 0},
			adopted:      [2]int64{(fee + 1) * nanoPerUnit, 0},
			wantRejected: true,
		},
		{
			name:    "lowered multiplier accepts what genesis rejects",
			genesis: [2]int64{0, 2 * perByteNano},
			adopted: [2]int64{0, perByteNano},
		},
		{
			name:         "raised multiplier rejects what genesis accepts",
			genesis:      [2]int64{0, perByteNano},
			adopted:      [2]int64{0, 2 * perByteNano},
			wantRejected: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			nodeConfig := nodeConfigFor(t, test.genesis[0], test.genesis[1])
			genesis, err := eras.NewByronProtocolParametersFromGenesis(
				nodeConfig.ByronGenesis(),
			)
			require.NoError(t, err)
			adopted := adopt(t, nodeConfig, test.adopted[0], test.adopted[1])

			// Control: the genesis policy reaches the opposite verdict.
			db := newTestDB(t)
			err = processByronBlockWithPParams(
				t, db, nodeConfig, build(t, db), genesis,
			)
			var feeErr eras.FeeTooLowByronError
			if test.wantRejected {
				require.NoError(t, err, "genesis policy")
			} else {
				require.ErrorAs(t, err, &feeErr, "genesis policy")
			}

			db = newTestDB(t)
			err = processByronBlockWithPParams(
				t, db, nodeConfig, build(t, db), adopted,
			)
			if !test.wantRejected {
				require.NoError(t, err, "adopted policy")
				return
			}
			require.ErrorAs(t, err, &feeErr, "adopted policy")
			required, err := adopted.MinFee(size)
			require.NoError(t, err)
			require.Equal(t, required, feeErr.Required)
		})
	}
}
