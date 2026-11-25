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

package chain_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// originTestGenesisHash stands in for the network's Byron genesis hash.
var originTestGenesisHash = bytes32(0x77)

func bytes32(fill byte) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = fill
	}
	return b
}

// mockByronOriginHeader is a Byron-era header whose position fields are
// chosen by the test.
type mockByronOriginHeader struct {
	ledger.ByronMainBlockHeader
	hash        []byte
	prevHash    []byte
	slot        uint64
	blockNumber uint64
}

func (h *mockByronOriginHeader) Hash() common.Blake2b256 {
	return common.NewBlake2b256(h.hash)
}

func (h *mockByronOriginHeader) PrevHash() common.Blake2b256 {
	return common.NewBlake2b256(h.prevHash)
}

func (h *mockByronOriginHeader) SlotNumber() uint64 { return h.slot }

func (h *mockByronOriginHeader) BlockNumber() uint64 { return h.blockNumber }

// freshOriginChain returns a chain that has never held a block, with the
// genesis hash configured when anchored is true.
func freshOriginChain(t *testing.T, anchored bool) *chain.Chain {
	t.Helper()
	cm, err := chain.NewManager(context.Background(), newTestDB(t), nil)
	require.NoError(t, err)
	if anchored {
		require.NoError(t, cm.SetGenesisHash(originTestGenesisHash))
	}
	return cm.PrimaryChain()
}

func conwayOriginHeader(blockNumber uint64, prevHash string) *MockBlock {
	return &MockBlock{
		MockBlockNumber: blockNumber,
		MockSlot:        3360,
		MockHash:        testHashPrefix + "beef",
		MockPrevHash:    prevHash,
	}
}

func byronOriginHeader(blockNumber uint64, prevHash []byte) *mockByronOriginHeader {
	return &mockByronOriginHeader{
		hash:        bytes32(0x11),
		prevHash:    prevHash,
		slot:        0,
		blockNumber: blockNumber,
	}
}

func requireOriginReject(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "header accepted as the first header of an empty chain")
	_, ok := errors.AsType[chain.BlockNotFitChainTipError](err)
	require.Truef(t, ok, "want BlockNotFitChainTipError, got %T: %v", err, err)
}

func TestAddBlockHeaderAtOriginAcceptsFirstHeader(t *testing.T) {
	t.Parallel()
	t.Run("post-Byron start with null previous hash", func(t *testing.T) {
		t.Parallel()
		c := freshOriginChain(t, true)
		require.NoError(t, c.AddBlockHeader(
			context.Background(), conwayOriginHeader(0, ""),
		))
		require.Equal(t, 1, c.HeaderCount())
	})
	t.Run("Byron EBB chaining onto the genesis hash", func(t *testing.T) {
		t.Parallel()
		c := freshOriginChain(t, true)
		require.NoError(t, c.AddBlockHeader(
			context.Background(),
			byronOriginHeader(0, originTestGenesisHash),
		))
		require.Equal(t, 1, c.HeaderCount())
	})
	t.Run("genesis hash not configured leaves a fresh chain open", func(t *testing.T) {
		t.Parallel()
		c := freshOriginChain(t, false)
		require.NoError(t, c.AddBlockHeader(
			context.Background(), conwayOriginHeader(7, testHashPrefix+"0001"),
		))
	})
}

func TestAddBlockHeaderAtOriginRejectsWrongBlockNumber(t *testing.T) {
	t.Parallel()
	for _, n := range []uint64{1, 2, 160} {
		t.Run(fmt.Sprintf("number=%d", n), func(t *testing.T) {
			t.Parallel()
			c := freshOriginChain(t, true)
			requireOriginReject(t, c.AddBlockHeader(
				context.Background(), conwayOriginHeader(n, ""),
			))
			requireOriginReject(t, c.AddBlockHeader(
				context.Background(),
				byronOriginHeader(n, originTestGenesisHash),
			))
			require.Zero(t, c.HeaderCount())
		})
	}
}

func TestAddBlockHeaderAtOriginRejectsWrongPrevHash(t *testing.T) {
	t.Parallel()
	t.Run("post-Byron header naming a block", func(t *testing.T) {
		t.Parallel()
		c := freshOriginChain(t, true)
		requireOriginReject(t, c.AddBlockHeader(
			context.Background(), conwayOriginHeader(0, testHashPrefix+"0001"),
		))
		require.Zero(t, c.HeaderCount())
	})
	t.Run("Byron header with another previous hash", func(t *testing.T) {
		t.Parallel()
		c := freshOriginChain(t, true)
		requireOriginReject(t, c.AddBlockHeader(
			context.Background(), byronOriginHeader(0, bytes32(0x55)),
		))
		require.Zero(t, c.HeaderCount())
	})
	t.Run("Byron header with a null previous hash", func(t *testing.T) {
		t.Parallel()
		c := freshOriginChain(t, true)
		requireOriginReject(t, c.AddBlockHeader(
			context.Background(), byronOriginHeader(0, bytes32(0x00)),
		))
	})
}
