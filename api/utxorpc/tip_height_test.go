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
	"errors"
	"io"
	"log/slog"
	"testing"

	connect "connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
)

// tipHeightLedgerStub reports a tip whose height the ledger already knows and
// fails every block lookup. That is the shape of the inconsistency this
// contract has to survive: the tip itself is known, its stored block row is
// not readable.
//
// The interface is embedded rather than implemented in full so an unexpected
// call panics instead of silently returning a zero value.
type tipHeightLedgerStub struct {
	UtxorpcLedgerState

	tip ochainsync.Tip
	// blockLookups counts reads of the stored block. The tip carries its own
	// height, so building a chain point must not need one at all.
	blockLookups int
}

func (s *tipHeightLedgerStub) Tip() ochainsync.Tip {
	return s.tip
}

func (s *tipHeightLedgerStub) GetBlock(
	ocommon.Point,
) (models.Block, error) {
	s.blockLookups++
	return models.Block{}, errors.New("no such block")
}

func (s *tipHeightLedgerStub) BlockByHash(
	[]byte,
) (models.Block, error) {
	s.blockLookups++
	return models.Block{}, errors.New("no such block")
}

func newTipHeightServers(
	t *testing.T,
	ls UtxorpcLedgerState,
) (*syncServiceServer, *queryServiceServer) {
	t.Helper()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: ls,
	})
	return &syncServiceServer{utxorpc: u}, &queryServiceServer{utxorpc: u}
}

// TestReadTip_HeightComesFromTheLedgerTip covers ReadTip when the tip's block
// row cannot be read.
//
// height is a plain proto3 uint64, so a client cannot tell an unknown height
// from a real one: reporting 0 beside a non-origin slot and hash asserts that
// the tip is the origin block, which is a different claim from "unknown". The
// ledger tip already carries its block number, so the height never has to be
// re-derived from storage and this state cannot arise.
func TestReadTip_HeightComesFromTheLedgerTip(t *testing.T) {
	stub := &tipHeightLedgerStub{
		tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(4321, []byte{0xBE, 0xEF}),
			BlockNumber: 7,
		},
	}
	syncSrv, _ := newTipHeightServers(t, stub)

	out, err := syncSrv.ReadTip(
		context.Background(),
		connect.NewRequest(&sync.ReadTipRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(4321), tip.GetSlot())
	assert.Equal(t, []byte{0xBE, 0xEF}, tip.GetHash())
	assert.Equal(t, uint64(7), tip.GetHeight(),
		"the height must be the one the ledger tip reports")
	assert.Zero(t, stub.blockLookups,
		"the tip carries its height; no block read should be needed")
}

// TestReadTip_OriginTipStaysZero is the negative case. Height 0 is the correct
// answer at the origin, so the fix must not turn a genuine zero into an error
// or a fabricated value.
func TestReadTip_OriginTipStaysZero(t *testing.T) {
	stub := &tipHeightLedgerStub{tip: ochainsync.Tip{}}
	syncSrv, _ := newTipHeightServers(t, stub)

	out, err := syncSrv.ReadTip(
		context.Background(),
		connect.NewRequest(&sync.ReadTipRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetTip()
	require.NotNil(t, tip)
	assert.Zero(t, tip.GetSlot())
	assert.Zero(t, tip.GetHeight())
}

// TestReadParams_LedgerTipHeightComesFromTheLedgerTip covers the same contract
// on the query service, which reports the tip alongside the parameter set.
func TestReadParams_LedgerTipHeightComesFromTheLedgerTip(t *testing.T) {
	stub := &shelleyLedgerStub{
		byronLedgerStub: byronLedgerStub{
			tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(42, []byte{0xab, 0xcd}),
				BlockNumber: 9,
			},
		},
		pparams: testShelleyPParams(),
	}
	srv := newByronQueryServer(t, stub)

	out, err := srv.ReadParams(
		context.Background(),
		connect.NewRequest(&query.ReadParamsRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetLedgerTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(42), tip.GetSlot())
	assert.Equal(t, uint64(9), tip.GetHeight(),
		"the height must be the one the ledger tip reports")
}

// TestReadData_LedgerTipHeightComesFromTheLedgerTip covers ReadData, which
// never looked the height up at all: it built its chain point from the tip's
// point alone and left Height at the zero value, so every response claimed the
// tip was the origin block.
func TestReadData_LedgerTipHeightComesFromTheLedgerTip(t *testing.T) {
	stub := &tipHeightLedgerStub{
		tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(555, []byte{0x11, 0x22}),
			BlockNumber: 12,
		},
	}
	_, querySrv := newTipHeightServers(t, stub)

	out, err := querySrv.ReadData(
		context.Background(),
		connect.NewRequest(&query.ReadDataRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetLedgerTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(555), tip.GetSlot())
	assert.Equal(t, uint64(12), tip.GetHeight(),
		"the height must be the one the ledger tip reports")
}
