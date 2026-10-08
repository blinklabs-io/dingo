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
	return ls.db.Transaction(context.Background(), true).
		Do(func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(
				context.Background(),
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
		context.Background(),
		lcommon.PoolKeyHash(issuer.Hash()),
		nil,
	)
	require.NoError(t, err)
	return stored, found
}

// TestLedgerProcessBlockOpCertFirstCounterUsesZeroBaseline pins the reference
// rule for a producer with no recorded counter through block application.
// ouroboros-consensus Praos doValidateKESSignature resolves currentIssueNo to
// 0 for a pool in the stake distribution with no counter, then requires
// m <= n <= m+1, so a first Praos counter is 0 or 1. cardano-ledger TPraos
// OCERT uses the same 0 but only requires m <= n.
func TestLedgerProcessBlockOpCertFirstCounterUsesZeroBaseline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		praos   bool
		counter uint64
		wantErr string
	}{
		{name: "praos first counter zero", praos: true, counter: 0},
		{name: "praos first counter one", praos: true, counter: 1},
		{
			name:    "praos first counter two is gapped",
			praos:   true,
			counter: 2,
			wantErr: "opcert counter 2 skips ahead of last seen 0",
		},
		{name: "tpraos large first counter", praos: false, counter: 490},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ls := newOpCertBaselineLedgerState(t, 0)

			err := applyOpCertBaselineBlock(t, ls, tt.praos, 10, tt.counter)

			stored, found := opCertBaselineRecorded(t, ls)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.False(
					t,
					found,
					"a rejected block must not record its counter",
				)
				return
			}
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, tt.counter, stored)
		})
	}
}

// TestLedgerProcessBlockOpCertMithrilPoolWithoutCertifiedCounter covers a
// Mithril-restored ledger whose certified HeaderState counter map was
// imported but does not name this pool: the reference has no counter for it
// either, so the zero baseline applies to its first block after the boundary.
func TestLedgerProcessBlockOpCertMithrilPoolWithoutCertifiedCounter(
	t *testing.T,
) {
	t.Parallel()

	const boundarySlot = uint64(100)
	for _, tt := range []struct {
		name    string
		counter uint64
		wantErr string
	}{
		{name: "counter one", counter: 1},
		{
			name:    "counter two is gapped",
			counter: 2,
			wantErr: "opcert counter 2 skips ahead of last seen 0",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ls := newOpCertBaselineLedgerState(t, boundarySlot)
			otherPool := lcommon.PoolKeyHash(
				lcommon.NewBlake2b224([]byte("certified-other-pool")),
			)
			require.NoError(
				t,
				ls.db.UpdatePoolOpCertSequence(
					context.Background(),
					otherPool, 7, boundarySlot, nil,
				),
			)

			err := applyOpCertBaselineBlock(
				t, ls, true, boundarySlot+10, tt.counter,
			)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			stored, found := opCertBaselineRecorded(t, ls)
			require.True(t, found)
			require.Equal(t, tt.counter, stored)
		})
	}
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
