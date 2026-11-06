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
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	syncapi "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	watchapi "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
)

// expiredHistoryLedger fails every history read with the error the blob
// store returns for a block whose CBOR has expired. Methods the handlers
// reach before the failing read are supplied; any other call panics through
// the nil embedded interface.
type expiredHistoryLedger struct {
	UtxorpcLedgerState
	expired error
}

func newExpiredHistoryLedger(slot uint64) *expiredHistoryLedger {
	return &expiredHistoryLedger{
		expired: fmt.Errorf(
			"get block: %w",
			&types.HistoryExpiredError{Slot: slot, Hash: expiredHash()},
		),
	}
}

func expiredHash() []byte { return bytes.Repeat([]byte{0xaa}, 32) }

func (l *expiredHistoryLedger) Tip() ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(9000, bytes.Repeat([]byte{0xbb}, 32)),
		BlockNumber: 90,
	}
}

func (l *expiredHistoryLedger) GetBlock(
	ocommon.Point,
) (models.Block, error) {
	return models.Block{}, l.expired
}

func (l *expiredHistoryLedger) GetBlockContext(
	context.Context,
	ocommon.Point,
) (models.Block, error) {
	return models.Block{}, l.expired
}

func (l *expiredHistoryLedger) GetChainFromPointContext(
	context.Context,
	ocommon.Point,
	bool,
) (*chain.ChainIterator, error) {
	return nil, l.expired
}

func (l *expiredHistoryLedger) GetIntersectPoint(
	_ context.Context,
	points []ocommon.Point,
) (*ocommon.Point, error) {
	return &points[0], nil
}

func (l *expiredHistoryLedger) TransactionByHash(
	context.Context,
	[]byte,
) (*models.Transaction, error) {
	return &models.Transaction{BlockHash: expiredHash()}, nil
}

func (l *expiredHistoryLedger) BlockByHash(
	context.Context,
	[]byte,
) (models.Block, error) {
	return models.Block{}, l.expired
}

func (l *expiredHistoryLedger) BlockTransactions(
	context.Context,
	models.Block,
) ([]lcommon.Transaction, error) {
	return nil, l.expired
}

func newExpiredHistoryServer(ls UtxorpcLedgerState) *Utxorpc {
	return NewUtxorpc(UtxorpcConfig{
		Logger:      discardLogger(),
		LedgerState: ls,
	})
}

// requireHistoryExpired checks the wire contract for expired history: the
// caller gets FailedPrecondition, not an internal error, and the message
// names the expired point so the client can pick a retained one.
func requireHistoryExpired(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(
		t,
		connect.CodeFailedPrecondition,
		connect.CodeOf(err),
		"expired history must not surface as %v: %v",
		connect.CodeOf(err),
		err,
	)
	require.ErrorContains(t, err, "history expired")
	require.ErrorContains(t, err, "slot=")
}

func TestFetchBlock_ExpiredHistoryIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	u := newExpiredHistoryServer(newExpiredHistoryLedger(10))
	_, err := (&syncServiceServer{utxorpc: u}).FetchBlock(
		t.Context(),
		connect.NewRequest(&syncapi.FetchBlockRequest{
			Ref: []*syncapi.BlockRef{{Slot: 10, Hash: expiredHash()}},
		}),
	)
	requireHistoryExpired(t, err)
	requireBulkSlotFree(t, u)
}

func TestDumpHistory_ExpiredHistoryIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	u := newExpiredHistoryServer(newExpiredHistoryLedger(10))
	_, err := (&syncServiceServer{utxorpc: u}).DumpHistory(
		t.Context(),
		connect.NewRequest(&syncapi.DumpHistoryRequest{
			StartToken: &syncapi.BlockRef{Slot: 10, Hash: expiredHash()},
		}),
	)
	requireHistoryExpired(t, err)
	requireBulkSlotFree(t, u)
}

func TestReadTx_ExpiredHistoryIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	u := newExpiredHistoryServer(newExpiredHistoryLedger(10))
	_, err := (&queryServiceServer{utxorpc: u}).ReadTx(
		t.Context(),
		connect.NewRequest(
			&query.ReadTxRequest{Hash: bytes.Repeat([]byte{0x01}, 32)},
		),
	)
	requireHistoryExpired(t, err)
	requireBulkSlotFree(t, u)
}

// A tx whose containing block exists but whose CBOR has expired fails in
// BlockTransactions rather than BlockByHash.
func TestReadTx_ExpiredBlockBodyIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	ls := &expiredBlockBodyLedger{
		expiredHistoryLedger: *newExpiredHistoryLedger(10),
	}
	u := newExpiredHistoryServer(ls)
	_, err := (&queryServiceServer{utxorpc: u}).ReadTx(
		t.Context(),
		connect.NewRequest(
			&query.ReadTxRequest{Hash: bytes.Repeat([]byte{0x01}, 32)},
		),
	)
	requireHistoryExpired(t, err)
}

type expiredBlockBodyLedger struct {
	expiredHistoryLedger
}

func (l *expiredBlockBodyLedger) BlockByHash(
	context.Context,
	[]byte,
) (models.Block, error) {
	return models.Block{Slot: 10, Hash: expiredHash()}, nil
}

func TestFollowTip_ExpiredIntersectIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	u := newExpiredHistoryServer(newExpiredHistoryLedger(10))
	err := (&syncServiceServer{utxorpc: u}).FollowTip(
		t.Context(),
		connect.NewRequest(&syncapi.FollowTipRequest{
			Intersect: []*syncapi.BlockRef{{Slot: 10, Hash: expiredHash()}},
		}),
		nil,
	)
	requireHistoryExpired(t, err)
}

// The intersect resolves, then the ledger cannot find the block behind it.
func TestWatchTx_ExpiredIntersectIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	u := newExpiredHistoryServer(newExpiredHistoryLedger(10))
	err := (&watchServiceServer{utxorpc: u}).WatchTx(
		t.Context(),
		connect.NewRequest(&watchapi.WatchTxRequest{
			Intersect: []*watchapi.BlockRef{{Slot: 10, Hash: expiredHash()}},
		}),
		nil,
	)
	requireHistoryExpired(t, err)
}

// The intersect block is retained, so the replay check passes and the
// expiry surfaces when the chain iterator is created.
func TestWatchTx_ExpiredIteratorStartIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	ls := &expiredIteratorLedger{
		expiredHistoryLedger: *newExpiredHistoryLedger(10),
	}
	u := newExpiredHistoryServer(ls)
	err := (&watchServiceServer{utxorpc: u}).WatchTx(
		t.Context(),
		connect.NewRequest(&watchapi.WatchTxRequest{
			Intersect: []*watchapi.BlockRef{{Slot: 10, Hash: expiredHash()}},
		}),
		nil,
	)
	require.True(t, ls.iteratorRequested.Load(), "chain iterator not reached")
	requireHistoryExpired(t, err)
}

type expiredIteratorLedger struct {
	expiredHistoryLedger
	iteratorRequested atomic.Bool
}

func (l *expiredIteratorLedger) GetBlock(
	ocommon.Point,
) (models.Block, error) {
	return models.Block{Slot: 10, Hash: expiredHash(), Number: 89}, nil
}

func (l *expiredIteratorLedger) GetChainFromPointContext(
	ctx context.Context,
	point ocommon.Point,
	inclusive bool,
) (*chain.ChainIterator, error) {
	l.iteratorRequested.Store(true)
	return l.expiredHistoryLedger.GetChainFromPointContext(
		ctx,
		point,
		inclusive,
	)
}

// A non-expiry failure must keep its original code.
func TestHistoryExpiredError_LeavesOtherErrorsAlone(t *testing.T) {
	t.Parallel()
	other := fmt.Errorf("disk on fire")
	require.Same(t, other, mapHistoryExpired(other))
	require.NoError(t, mapHistoryExpired(nil))
	nf := connect.NewError(connect.CodeNotFound, other)
	require.Same(t, nf, mapHistoryExpired(nf))
}
