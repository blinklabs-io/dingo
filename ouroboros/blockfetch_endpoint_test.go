package ouroboros

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func TestBlockfetchServerSendBatch_ExactEndpointContract(t *testing.T) {
	endBlock := testBlockfetchIteratorBlock(200)
	boundaryBlock := testBlockfetchIteratorBlock(200)
	boundaryBlock.Block.Type = gledger.BlockTypeByronEbb
	boundaryBlock.Point.Hash = []byte{0xeb}
	tests := []struct {
		name       string
		steps      []blockfetchIteratorStep
		wantBlocks int
		wantDone   bool
	}{
		{
			name: "complete range",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{result: endBlock},
			},
			wantBlocks: 2,
			wantDone:   true,
		},
		{
			name: "earlier boundary block at endpoint slot",
			steps: []blockfetchIteratorStep{
				{result: boundaryBlock},
				{result: endBlock},
			},
			wantBlocks: 2,
			wantDone:   true,
		},
		{
			name: "rollback after partial delivery",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{result: &chain.ChainIteratorResult{Rollback: true}},
			},
			wantBlocks: 1,
		},
		{
			name: "nil result after partial delivery",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{},
			},
			wantBlocks: 1,
		},
		{
			name: "overshoots endpoint",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{result: testBlockfetchIteratorBlock(201)},
			},
			wantBlocks: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			bus := event.NewEventBus(nil, logger)
			t.Cleanup(bus.Close)
			node := newOuroboros(OuroborosConfig{
				Logger:   logger,
				EventBus: bus,
			})
			iter := &stubBlockfetchIterator{steps: test.steps}
			server := &stubBlockfetchBatchServer{}
			conn := &stubBlockfetchConnection{errChan: make(chan error)}
			err := node.blockfetchServerSendBatch(
				testConnId().String(),
				test.steps[0].result.Point,
				endBlock.Point,
				iter,
				server,
				conn,
			)
			if test.wantDone {
				require.NoError(t, err)
				require.Equal(t, 1, server.batchDoneCalls)
				require.Zero(t, conn.closeCalls)
			} else {
				require.Error(t, err)
				require.Zero(t, server.batchDoneCalls)
				require.Equal(t, 1, conn.closeCalls)
			}
			require.Equal(t, test.wantBlocks, server.blockCalls)
			require.Equal(t, 1, iter.cancelCalls)
		})
	}
}
