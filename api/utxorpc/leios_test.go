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

package utxorpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

// certifiedEndorserLedger backs the UTxO RPC servers with a real ledger state
// holding a certifying ranking block whose transactions live in the endorser
// block its parent announced.
type certifiedEndorserLedger struct {
	*ledger.LedgerState
	fixture *testfixtures.CertifiedEndorserBlock
	txHash  []byte
}

func (l *certifiedEndorserLedger) SecurityParam() int { return 2 }

func newCertifiedEndorserLedger(t *testing.T) *certifiedEndorserLedger {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	f, err := testfixtures.GenerateCertifiedEndorserBlock(100)
	require.NoError(t, err)
	for _, block := range []gledger.Block{f.Announcer, f.Certifier} {
		require.NoError(t, db.BlockCreate(models.Block{
			Hash:     block.Hash().Bytes(),
			PrevHash: block.PrevHash().Bytes(),
			Slot:     block.SlotNumber(),
			Number:   block.BlockNumber(),
			ID:       block.BlockNumber() + database.BlockInitialIndex,
			Type:     uint(block.Type()),
			Cbor:     block.Cbor(),
		}, nil))
	}
	require.NoError(t, db.SetLeiosEB(
		f.Announcer.SlotNumber(), f.EndorserHash.Bytes(), f.Manifest,
		f.Transactions,
	))
	tx, err := safedecode.DijkstraBlockTransaction(f.Transactions[0])
	require.NoError(t, err)
	// Transactions of a certified endorser block are indexed under the
	// certifying ranking block.
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO "transaction" (hash, block_hash, slot, fee, collateral_fee, ttl, block_index, valid) VALUES (?, ?, ?, '0', '0', '0', 0, TRUE)`,
		tx.Hash().Bytes(),
		f.Certifier.Hash().Bytes(),
		f.Certifier.SlotNumber(),
	)
	require.NoError(t, err)
	return &certifiedEndorserLedger{
		LedgerState: ls,
		fixture:     f,
		txHash:      tx.Hash().Bytes(),
	}
}

func TestReadTxFindsCertifiedEndorserTransaction(t *testing.T) {
	t.Parallel()
	ls := newCertifiedEndorserLedger(t)
	srv := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: ls,
	})}

	resp, err := srv.ReadTx(context.Background(), connect.NewRequest(
		&query.ReadTxRequest{Hash: ls.txHash},
	))
	require.NoError(t, err)
	require.Equal(t, []byte(ls.fixture.Transactions[0]), resp.Msg.GetTx().GetNativeBytes())
	require.Equal(
		t,
		ls.fixture.Certifier.Hash().Bytes(),
		resp.Msg.GetTx().GetBlockRef().GetHash(),
	)
}

func TestWatchTxRollbackUndoesCertifiedEndorserTransaction(t *testing.T) {
	t.Parallel()
	ls := newCertifiedEndorserLedger(t)
	srv := &watchServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: ls,
	})}

	// Rolling back to the announcing block undoes the certifying block, whose
	// transactions are the endorser block's.
	msgs, err := srv.watchTxFetchRollbackUndoFromBlocks(
		context.Background(),
		ls.fixture.Certifier.Hash().Bytes(),
		ocommon.NewPoint(
			ls.fixture.Announcer.SlotNumber(),
			ls.fixture.Announcer.Hash().Bytes(),
		),
		func(gledger.Transaction) bool { return true },
	)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	undo := msgs[0].GetUndo()
	require.NotNil(t, undo)
	require.Equal(t, ls.txHash, undo.GetCardano().GetHash())
}
