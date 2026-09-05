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
	"time"

	"github.com/blinklabs-io/dingo/chain"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type readChainMockBlock struct {
	slot uint64
	hash []byte
}

func (m *readChainMockBlock) Era() lcommon.Era {
	return babbage.EraBabbage
}

func (m *readChainMockBlock) SlotNumber() uint64 {
	return m.slot
}

func (m *readChainMockBlock) Hash() lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(m.hash)
}

func (m *readChainMockBlock) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (m *readChainMockBlock) BlockNumber() uint64 {
	return 1
}

func (m *readChainMockBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}

func (m *readChainMockBlock) BlockBodySize() uint64 {
	return 0
}

func (m *readChainMockBlock) Cbor() []byte {
	return nil
}

func (m *readChainMockBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (m *readChainMockBlock) Header() lcommon.BlockHeader {
	return nil
}

func (m *readChainMockBlock) Type() int {
	return int(babbage.BlockTypeBabbage)
}

func (m *readChainMockBlock) Transactions() []lcommon.Transaction {
	return nil
}

func (m *readChainMockBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}

type scriptedLedgerReadIterator struct {
	ctx     context.Context
	results []*chain.ChainIteratorResult
}

func (s *scriptedLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	if len(s.results) > 0 {
		result := s.results[0]
		s.results = s.results[1:]
		return result, nil
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func TestTrimReadBatchForRollbackKeepsCanonicalPrefix(t *testing.T) {
	blocks := []gledger.Block{
		&readChainMockBlock{slot: 10, hash: testHashBytes("trim-1")},
		&readChainMockBlock{slot: 20, hash: testHashBytes("trim-2")},
		&readChainMockBlock{slot: 30, hash: testHashBytes("trim-3")},
	}

	trimmed, emitRollback := trimReadBatchForRollback(
		blocks,
		ocommon.NewPoint(20, testHashBytes("trim-2")),
	)

	require.Len(t, trimmed, 2)
	assert.Equal(t, uint64(10), trimmed[0].SlotNumber())
	assert.Equal(t, uint64(20), trimmed[1].SlotNumber())
	assert.False(t, emitRollback)
}

func TestTrimReadBatchForRollbackRequestsRollbackWhenPointMissing(
	t *testing.T,
) {
	blocks := []gledger.Block{
		&readChainMockBlock{slot: 10, hash: testHashBytes("missing-1")},
		&readChainMockBlock{slot: 20, hash: testHashBytes("missing-2")},
	}

	trimmed, emitRollback := trimReadBatchForRollback(
		blocks,
		ocommon.NewPoint(15, testHashBytes("missing-mid")),
	)

	assert.Nil(t, trimmed)
	assert.True(t, emitRollback)
}

func TestLedgerReadChainIteratorWaitsForResultCompletion(t *testing.T) {
	ctx := t.Context()

	resultCh := make(chan readChainResult)
	iter := &scriptedLedgerReadIterator{
		ctx: ctx,
		results: []*chain.ChainIteratorResult{
			{
				Rollback: true,
				Point:    ocommon.NewPoint(10, testHashBytes("rollback-1")),
			},
			{
				Rollback: true,
				Point:    ocommon.NewPoint(20, testHashBytes("rollback-2")),
			},
		},
	}
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	first := <-resultCh
	require.True(t, first.rollback)
	require.NotNil(t, first.done)

	select {
	case <-resultCh:
		t.Fatal("reader advanced before current result was completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(first.done)

	second := <-resultCh
	require.True(t, second.rollback)
	require.NotNil(t, second.done)
	close(second.done)
}
