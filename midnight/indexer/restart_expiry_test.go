package indexer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/historyexpiry"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type fixedLedgerWindow struct{ current, window uint64 }

func (w fixedLedgerWindow) CurrentSlot() (uint64, error) { return w.current, nil }
func (w fixedLedgerWindow) StabilityWindow() uint64      { return w.window }

// The registration datum is read through the database's real payload
// resolver. The block it names is kept while the registration is unspent, so a
// restart restores it, and while its spend can be rolled back.
func TestRestartRestoresRegistrationWhoseBlockIsOld(t *testing.T) {
	t.Parallel()
	consensus, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(consensus)) })
	indexerDB, err := dbtest.NewIndexerDatabase(t, consensus, dbtest.IndexerOptions{})
	require.NoError(t, err)

	datum, err := cbor.Encode([]byte("registration-datum-91c4e07b5a2d6f38"))
	require.NoError(t, err)
	block := validRegistrationHistoryBlockWithDatum(t, datum)
	require.NoError(t, consensus.BlockCreate(block, nil))

	cfg := Config{
		Metadata: indexerDB.Metadata(),
		SlotTimer: timestampFunc(func(slot uint64) (time.Time, error) {
			return time.UnixMilli(int64(slot)), nil
		}),
		Logger:                  slog.New(slog.NewTextHandler(os.Stderr, nil)),
		MappingValidatorAddress: testMappingAddr,
		AuthTokenPolicyID:       testAuthPolicyID,
		AuthTokenAssetName:      testAuthAssetNameHex,
	}
	idx, err := New(cfg)
	require.NoError(t, err)
	regTx := pad32("dd100001")
	require.NoError(t, idx.processBlock(block, []lcommon.Transaction{buildTx(
		t, regTx,
		[]lcommon.TransactionInput{buildInput(t, pad32("dd100000"), 0)},
		[]lcommon.TransactionOutput{buildAuthOutput(
			t, testAuthPolicyID, testAuthAssetNameHex, datum,
		)},
	)}, 1_000))

	// The pruner runs on its ticker, as in the node. Every Held call is a
	// signal: a round checks the held block once, so the second call means the
	// first round ran to completion.
	type check struct {
		phase uint64
		held  bool
	}
	var phase atomic.Uint64
	checks := make(chan check, 1024)
	pruner := historyexpiry.NewPruner(historyexpiry.PrunerConfig{
		LedgerState: fixedLedgerWindow{current: 300, window: 5},
		DB:          consensus,
		Frequency:   5 * time.Millisecond,
		Held: func(ctx context.Context, slot uint64, hash []byte) (bool, error) {
			checkedPhase := phase.Load()
			held, err := indexerDB.BlockHoldsActivePayloads(ctx, slot, hash, 0)
			select {
			case checks <- check{phase: checkedPhase, held: held}:
			default:
			}
			return held, err
		},
	})
	require.NoError(t, pruner.Start(t.Context()))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pruner.Stop(stopCtx))
	})
	expired := func() bool {
		txn := consensus.BlobTxn(false)
		defer txn.Release()
		//nolint:nilaway // database.New requires a non-nil blob store
		_, _, err := consensus.Blob().GetBlock(txn.Blob(), block.Slot, block.Hash)
		return errors.Is(err, types.ErrHistoryExpired)
	}
	requireCheck := func(want uint64, name string) {
		t.Helper()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case got := <-checks:
				if got.phase >= want {
					require.True(t, got.held, "%s observed the block as unheld", name)
					return
				}
			case <-deadline.C:
				t.Fatalf("timeout waiting for %s", name)
			}
		}
	}
	requireCheck(0, "first round")
	requireCheck(0, "second round")
	require.False(t, expired(), "an unspent registration keeps its block")

	restarted, err := New(cfg)
	require.NoError(t, err, "startup reads the datum of a live registration")
	restarted.mu.RLock()
	reg, ok := restarted.regUTxOs[utxoKey{TxHash: regTx, Index: 0}]
	restarted.mu.RUnlock()
	require.True(t, ok)
	require.Equal(t, datum, reg.FullDatum)

	spendBlock := models.Block{Number: 2, Slot: 400, Hash: bytes.Repeat([]byte{0x0e}, 32)}
	spendPhase := phase.Add(1)
	require.NoError(t, restarted.processBlock(spendBlock, []lcommon.Transaction{
		buildTx(t, pad32("dd200001"),
			[]lcommon.TransactionInput{buildInput(t, regTx, 0)},
			[]lcommon.TransactionOutput{anyOutput(t)}),
	}, 2_000))
	// The spend can still be rolled back, which restores the registration's
	// datum from its block, so the block stays while the spend is inside the
	// rollback window.
	requireCheck(spendPhase, "round after the spend")
	requireCheck(spendPhase, "second round after the spend")
	require.False(t, expired(), "the block stays while the spend can be rolled back")
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, pruner.Stop(stopCtx))
	cancelStop()
	require.NoError(t, restarted.rollbackBlock(spendBlock),
		"rolling back the spend reads the registration's block")
	restarted.mu.RLock()
	restored, ok := restarted.regUTxOs[utxoKey{TxHash: regTx, Index: 0}]
	restarted.mu.RUnlock()
	require.True(t, ok, "the rollback restores the registration")
	require.Equal(t, datum, restored.FullDatum)
	_, err = New(cfg)
	require.NoError(t, err, "startup never reads an expired payload")
}
