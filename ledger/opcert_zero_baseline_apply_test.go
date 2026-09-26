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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// opCertBaselineIssuer is the cold key every block in these tests carries.
// Its hash is the pool key the counter is recorded under.
func opCertBaselineIssuer() lcommon.IssuerVkey {
	var issuer lcommon.IssuerVkey
	for i := range issuer {
		issuer[i] = byte(0x40 + i)
	}
	return issuer
}

// opCertBaselineBlock builds an empty block of the given era carrying counter,
// with its CBOR and declared body size set so the validated apply path's
// envelope checks pass and the stateful counter rule is what decides.
func opCertBaselineBlock(
	t *testing.T,
	praos bool,
	slot uint64,
	counter uint64,
) gledger.Block {
	t.Helper()
	issuer := opCertBaselineIssuer()
	if praos {
		block := &babbage.BabbageBlock{
			BlockHeader: &babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        slot,
					IssuerVkey:  issuer,
					OpCert: babbage.BabbageOpCert{
						SequenceNumber: counter,
					},
					ProtoVersion: babbage.BabbageProtoVersion{Major: 7},
				},
			},
		}
		encoded, err := cbor.EncodeGeneric(block)
		require.NoError(t, err)
		block.SetCbor(encoded)
		size, err := serializedBlockBodySize(block)
		require.NoError(t, err)
		block.BlockHeader.Body.BlockBodySize = size
		block.SetCbor(nil)
		encoded, err = cbor.EncodeGeneric(block)
		require.NoError(t, err)
		block.SetCbor(encoded)
		return block
	}
	block := &alonzo.AlonzoBlock{
		BlockHeader: &alonzo.AlonzoBlockHeader{
			ShelleyBlockHeader: shelley.ShelleyBlockHeader{
				Body: shelley.ShelleyBlockHeaderBody{
					BlockNumber:          1,
					Slot:                 slot,
					IssuerVkey:           issuer,
					OpCertSequenceNumber: counter,
					ProtoMajorVersion:    6,
				},
			},
		},
	}
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	size, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = size
	block.SetCbor(nil)
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	return block
}

// applyOpCertBaselineBlock drives block through ledgerProcessBlock with
// validation enabled, the production path that enforces the counter rule.
func applyOpCertBaselineBlock(
	t *testing.T,
	ls *LedgerState,
	praos bool,
	slot uint64,
	counter uint64,
) error {
	t.Helper()
	block := opCertBaselineBlock(t, praos, slot, counter)
	era := eras.AlonzoEraDesc
	var pparams lcommon.ProtocolParameters = &alonzo.AlonzoProtocolParameters{
		MaxBlockBodySize:   100_000,
		MaxBlockHeaderSize: 100_000,
		ProtocolMajor:      6,
	}
	if praos {
		era = eras.BabbageEraDesc
		pparams = &babbage.BabbageProtocolParameters{
			MaxBlockBodySize:   100_000,
			MaxBlockHeaderSize: 100_000,
			ProtocolMajor:      7,
		}
	}
	return ls.db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: slot, Hash: block.Hash().Bytes()},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			nil,
			era,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

func newOpCertBaselineLedgerState(
	t *testing.T,
	mithrilLedgerSlot uint64,
) *LedgerState {
	t.Helper()
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	return &LedgerState{
		db:                newTestDB(t),
		mithrilLedgerSlot: mithrilLedgerSlot,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

func opCertBaselineRecorded(
	t *testing.T,
	ls *LedgerState,
) (uint64, bool) {
	t.Helper()
	issuer := opCertBaselineIssuer()
	stored, found, err := ls.db.LatestPoolOpCertSequence(
		lcommon.PoolKeyHash(issuer.Hash()),
		nil,
	)
	require.NoError(t, err)
	return stored, found
}

// TestLedgerProcessBlockOpCertMithrilWithoutCertifiedCounterMapFailsClosed
// covers a Mithril-restored ledger that holds no certified counter at its
// trust boundary at all: a database imported before the HeaderState counter
// map was persisted. The pool's real counter is unknown there, and zero is
// not a substitute for it -- a pool several rotations in would have its next
// valid block rejected. Block application must refuse rather than guess.
func TestLedgerProcessBlockOpCertMithrilWithoutCertifiedCounterMapFailsClosed(
	t *testing.T,
) {
	t.Parallel()

	const boundarySlot = uint64(100)
	ls := newOpCertBaselineLedgerState(t, boundarySlot)

	err := applyOpCertBaselineBlock(t, ls, true, boundarySlot+10, 1)

	require.ErrorIs(t, err, errOpCertBaselineNotImported)
	_, found := opCertBaselineRecorded(t, ls)
	require.False(t, found)
}
