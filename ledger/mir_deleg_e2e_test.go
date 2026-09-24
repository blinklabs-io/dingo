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
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerProcessBlockRejectsLateMIRCertificate is the block-application
// half of #4362: a block carrying a MIR certificate inside the final
// stability window fails validation, so the certificate is never stored for
// applyMIRCerts to credit at the boundary.
func TestLedgerProcessBlockRejectsLateMIRCertificate(t *testing.T) {
	t.Parallel()

	// k=432 and f=1/20 give a 25,920-slot window, so the epoch
	// [568,000, 1,000,000) has its cutoff at 974,080.
	const blockSlot = uint64(974_080)
	// [6, [reserves, {[key hash, 0x4e 00..00]: 1}]], hand-encoded because
	// the gouroboros MIR reward type does not marshal back to its wire form.
	mirCert, err := hex.DecodeString(
		"82068200a18200581c4e" + strings.Repeat("00", 27) + "01",
	)
	require.NoError(t, err)
	certsCbor, err := cbor.Encode([]any{cbor.RawMessage(mirCert)})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		map[uint]any{
			0: []any{},
			1: []any{},
			2: uint64(0),
			4: cbor.RawMessage(certsCbor),
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := babbage.NewBabbageTransactionFromCbor(txCbor)
	require.NoError(t, err)

	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  blockSlot,
				ByteLength: uint32(len(txCbor)), // #nosec G115
			},
		},
	}
	block := &babbage.BabbageBlock{
		BlockHeader: &babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber:  1,
				Slot:         blockSlot,
				ProtoVersion: babbage.BabbageProtoVersion{Major: 8},
			},
		},
		TransactionBodies: []babbage.BabbageTransactionBody{tx.Body},
		TransactionWitnessSets: []babbage.BabbageTransactionWitnessSet{
			tx.WitnessSet,
		},
	}
	// The header declares the body size: the four body components as
	// they are encoded inside the block.
	var bodySize int
	for _, part := range []any{
		block.TransactionBodies,
		block.TransactionWitnessSets,
		map[uint]any{},
		[]uint{},
	} {
		encoded, err := cbor.Encode(part)
		require.NoError(t, err)
		bodySize += len(encoded)
	}
	block.BlockHeader.Body.BlockBodySize = uint64(bodySize) // #nosec G115
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)

	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	db := newTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	epoch := models.Epoch{
		EpochId:       1,
		StartSlot:     568_000,
		LengthInSlots: 432_000,
		EraId:         eras.BabbageEraDesc.Id,
	}
	ls.Lock()
	ls.currentEpoch = epoch
	ls.epochCache = []models.Epoch{epoch}
	ls.publishSnapshotsLocked()
	ls.Unlock()
	pparams := &babbage.BabbageProtocolParameters{
		ProtocolMajor:      8,
		MaxBlockBodySize:   100_000,
		MaxBlockHeaderSize: 100_000,
		MaxTxSize:          16_384,
	}

	err = db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{
				Slot: blockSlot,
				Hash: []byte("late-mir-certificate"),
			},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{},
			offsets,
			eras.BabbageEraDesc,
			pparams,
			nil,
			epoch.EpochId,
			epoch.StartSlot,
			false,
		)
		return err
	})
	var tooLate eras.MIRCertificateTooLateError
	require.ErrorAs(t, err, &tooLate)
	require.Equal(t, uint64(974_080), tooLate.Cutoff)

	effects, err := db.GetMIRCertsInSlotRange(
		epoch.StartSlot, epoch.StartSlot+uint64(epoch.LengthInSlots), nil,
	)
	require.NoError(t, err)
	require.Empty(
		t,
		effects,
		"a rejected block must not store its MIR certificate",
	)
}
