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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// parameterChangeGovernanceTestBlock is a minimal ledger.Block carrying a
// single transaction, used to drive LedgerState.ledgerProcessBlock -- the
// production entry point that runs the real, unstubbed era ValidateTxFunc
// before any transaction (including its governance proposals) can be
// persisted. It is deliberately self-contained rather than reusing
// validityOutcomeTestBlock from block_transaction_validity_test.go, which
// only special-cases Byron and Dijkstra block types.
type parameterChangeGovernanceTestBlock struct {
	header lcommon.BlockHeader
	tx     lcommon.Transaction
}

func (b *parameterChangeGovernanceTestBlock) Type() int {
	return conway.BlockTypeConway
}

func (b *parameterChangeGovernanceTestBlock) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256Hash(
		[]byte("parameter-change-governance-persistence-block"),
	)
}

func (b *parameterChangeGovernanceTestBlock) Header() lcommon.BlockHeader {
	return b.header
}

func (b *parameterChangeGovernanceTestBlock) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (b *parameterChangeGovernanceTestBlock) BlockNumber() uint64 { return 1 }
func (b *parameterChangeGovernanceTestBlock) SlotNumber() uint64  { return 10 }
func (b *parameterChangeGovernanceTestBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *parameterChangeGovernanceTestBlock) BlockBodySize() uint64 { return 1 }
func (b *parameterChangeGovernanceTestBlock) Era() lcommon.Era {
	return conway.EraConway
}

func (b *parameterChangeGovernanceTestBlock) Transactions() []lcommon.Transaction {
	return []lcommon.Transaction{b.tx}
}
func (b *parameterChangeGovernanceTestBlock) Cbor() []byte {
	return []byte{0x82, 0x80, 0x80}
}

func (b *parameterChangeGovernanceTestBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}

func (b *parameterChangeGovernanceTestBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

// TestLedgerProcessBlockRejectsParameterChangeProtocolVersionAndPersistsNoProposal
// is the dingo#4439 "verify no proposal row is persisted" regression, driven
// through the real production pipeline rather than asserted structurally.
// It runs the transaction through LedgerState.ledgerProcessBlock with the
// real, unstubbed eras.ConwayEraDesc.ValidateTxFunc (ValidateTxConway) --
// the same function TestEraDescWiresProtocolVersionProtection (package eras)
// pins ConwayEraDesc to -- so this exercises the actual validate-before-persist
// ordering ledgerProcessBlock enforces (ledger/state.go), not a re-derivation
// of it. Asserts both that block processing rejects the transaction and that
// no governance_proposal row exists afterward.
func TestLedgerProcessBlockRejectsParameterChangeProtocolVersionAndPersistsNoProposal(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	txHash := bytes.Repeat([]byte{0x44}, 32)
	minFeeA := uint(1)
	proposal := conway.ConwayProposalProcedure{
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeParameterChange),
			Action: &conway.ConwayParameterChangeGovAction{
				ParamUpdate: conway.ConwayProtocolParameterUpdate{
					MinFeeA: &minFeeA,
					ProtocolVersion: &lcommon.ProtocolParametersProtocolVersion{
						Major: conway.MinProtocolVersionConway,
					},
				},
			},
		},
	}
	tx := omockledger.NewTransactionBuilder()
	tx.WithId(txHash)
	tx.WithType(conway.TxTypeConway)
	tx.WithValid(true)
	tx.WithProposalProcedures(proposal)

	pparams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: conway.MinProtocolVersionConway,
		},
		MaxBlockBodySize:   100_000,
		MaxBlockHeaderSize: 100_000,
	}
	currentEra := eras.ConwayEraDesc
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{currentEra},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
		currentEra: currentEra,
	}
	block := &parameterChangeGovernanceTestBlock{
		header: &conway.ConwayBlockHeader{},
		tx:     tx,
	}

	processErr := db.Transaction(true).
		Do(func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(
				txn,
				ocommon.NewPoint(10, block.Hash().Bytes()),
				block,
				true,
				false,
				false,
				nil,
				envelopeParent{origin: true},
				nil,
				currentEra,
				pparams,
				nil,
				0,
				0,
				false,
			)
			return err
		})

	var protocolVersionErr eras.ParameterChangeProtocolVersionError
	require.ErrorAs(
		t,
		processErr,
		&protocolVersionErr,
		"block carrying a protocol-version ParameterChange must be rejected",
	)

	_, getErr := db.GetGovernanceProposal(txHash, 0, nil)
	require.ErrorIs(
		t,
		getErr,
		models.ErrGovernanceProposalNotFound,
		"a rejected block must never persist its governance proposal",
	)
}

// TestLedgerProcessBlockRejectsDijkstraParameterChangeProtocolVersionAndPersistsNoProposal
// is the Dijkstra analogue.
func TestLedgerProcessBlockRejectsDijkstraParameterChangeProtocolVersionAndPersistsNoProposal(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	txHash := bytes.Repeat([]byte{0x45}, 32)
	minFeeA := uint(1)
	proposal := conway.ConwayProposalProcedure{
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeParameterChange),
			Action: &gdijkstra.DijkstraParameterChangeGovAction{
				ParamUpdate: gdijkstra.DijkstraProtocolParameterUpdate{
					MinFeeA: &minFeeA,
					ProtocolVersion: &lcommon.ProtocolParametersProtocolVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
	}
	tx := omockledger.NewTransactionBuilder()
	tx.WithId(txHash)
	tx.WithType(gdijkstra.TxTypeDijkstra)
	tx.WithValid(true)
	tx.WithProposalProcedures(proposal)

	pparams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MaxBlockBodySize:   100_000,
			MaxBlockHeaderSize: 100_000,
		},
	}
	currentEra := eras.DijkstraEraDesc
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{currentEra},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
		currentEra: currentEra,
	}
	block := &parameterChangeGovernanceTestBlock{
		header: &gdijkstra.DijkstraBlockHeader{},
		tx:     tx,
	}

	processErr := db.Transaction(true).
		Do(func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(
				txn,
				ocommon.NewPoint(10, block.Hash().Bytes()),
				block,
				true,
				false,
				false,
				nil,
				envelopeParent{origin: true},
				nil,
				currentEra,
				pparams,
				nil,
				0,
				0,
				false,
			)
			return err
		})

	var protocolVersionErr eras.ParameterChangeProtocolVersionError
	require.ErrorAs(
		t,
		processErr,
		&protocolVersionErr,
		"block carrying a protocol-version ParameterChange must be rejected",
	)

	_, getErr := db.GetGovernanceProposal(txHash, 0, nil)
	require.ErrorIs(
		t,
		getErr,
		models.ErrGovernanceProposalNotFound,
		"a rejected block must never persist its governance proposal",
	)
}
