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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/mempool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMempoolMock struct {
	mempool.Service
	txs      []mempool.MempoolTransaction
	capacity int64
}

func (m *testMempoolMock) GetTransaction(
	hash string,
) (mempool.MempoolTransaction, bool) {
	for _, tx := range m.txs {
		if strings.EqualFold(tx.Hash, hash) {
			return tx, true
		}
	}
	return mempool.MempoolTransaction{}, false
}

func (m *testMempoolMock) Transactions() []mempool.MempoolTransaction {
	return m.txs
}

func (m *testMempoolMock) CapacityBytes() int64 {
	return m.capacity
}

func TestGetMempoolInfo(t *testing.T) {
	t.Parallel()

	// 1. Nil Mempool
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "test-nil", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)

	clTrNil, srvTrNil := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := serverNil.Connect(ctx, srvTrNil, nil)
	require.NoError(t, err)

	clientNil := mcp.NewClient(
		&mcp.Implementation{Name: "client-nil", Version: "1.0"},
		nil,
	)
	csNil, err := clientNil.Connect(ctx, clTrNil, nil)
	require.NoError(t, err)
	defer csNil.Close()

	resNil, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_mempool_info",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, resNil.IsError)
	assert.Contains(
		t,
		resNil.Content[0].(*mcp.TextContent).Text,
		"mempool service is currently inactive",
	)

	// 2. Active Mempool Mock
	mockMP := &testMempoolMock{
		capacity: 1048576, // 1 MiB
		txs: []mempool.MempoolTransaction{
			{
				Hash:     "aaaabbbbccccdddd0000111122223333444455556666777788889999aaaabbbb",
				Type:     6,
				Cbor:     make([]byte, 512),
				LastSeen: time.Now().Add(-10 * time.Second),
			},
			{
				Hash:     "bbbbccccddddeeee0000111122223333444455556666777788889999bbbbcccc",
				Type:     6,
				Cbor:     make([]byte, 1024),
				LastSeen: time.Now().Add(-5 * time.Second),
			},
		},
	}

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-mp", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, mockMP, "preview", 5*time.Second)

	clTr, srvTr := mcp.NewInMemoryTransports()
	_, err = server.Connect(ctx, srvTr, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clTr, nil)
	require.NoError(t, err)
	defer cs.Close()

	// Summary inspection
	resSummary, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_mempool_info",
		Arguments: map[string]any{"limit": 5},
	})
	require.NoError(t, err)
	assert.False(t, resSummary.IsError)
	summaryText := resSummary.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, summaryText, "Pending Transactions**: 2")
	assert.Contains(t, summaryText, "Buffered Volume**: 1536 bytes")
	assert.Contains(t, summaryText, "Mempool Capacity**: 1048576 bytes")
	assert.Contains(
		t,
		summaryText,
		"aaaabbbbccccdddd0000111122223333444455556666777788889999aaaabbbb",
	)

	// Specific Tx Found
	resTx, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_mempool_info",
		Arguments: map[string]any{
			"tx_hash": "aaaabbbbccccdddd0000111122223333444455556666777788889999aaaabbbb",
		},
	})
	require.NoError(t, err)
	assert.False(t, resTx.IsError)
	txText := resTx.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, txText, "Payload Size**: 512 bytes")
	assert.Contains(t, txText, "Pending Block Inclusion")

	// Specific Tx Not Found
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_mempool_info",
		Arguments: map[string]any{
			"tx_hash": "0000000000000000000000000000000000000000000000000000000000000000",
		},
	})
	require.NoError(t, err)
	assert.False(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"not currently present",
	)
}
