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
	"fmt"
	"slices"
	"strings"
	"sync"
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
	// sizeBytes overrides the aggregate size when non-zero; otherwise it is
	// the sum of the transaction CBOR lengths.
	sizeBytes int64

	mu               sync.Mutex
	fullEnumerations int
	boundedItems     []int
}

func (m *testMempoolMock) SizeBytes() int64 {
	if m.sizeBytes != 0 {
		return m.sizeBytes
	}
	var total int64
	for _, tx := range m.txs {
		total += int64(len(tx.Cbor))
	}
	return total
}

func (m *testMempoolMock) TransactionsBounded(
	maxItems int,
	_ int64,
) ([]mempool.MempoolTransaction, int) {
	m.mu.Lock()
	m.boundedItems = append(m.boundedItems, maxItems)
	m.mu.Unlock()
	out := m.txs
	if maxItems > 0 && len(out) > maxItems {
		out = out[:maxItems]
	}
	return out, len(m.txs)
}

func (m *testMempoolMock) enumerations() (int, []int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fullEnumerations, slices.Clone(m.boundedItems)
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
	m.mu.Lock()
	m.fullEnumerations++
	m.mu.Unlock()
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

func TestGetMempoolInfoSummaryIsBounded(t *testing.T) {
	t.Parallel()

	const poolSize = 120
	// A size distinct from the sum of the displayed CBOR proves the totals
	// come from the aggregate counter, not from the enumerated prefix.
	const aggregateBytes = int64(7_654_321)
	txs := make([]mempool.MempoolTransaction, poolSize)
	for i := range txs {
		txs[i] = mempool.MempoolTransaction{
			Hash:     fmt.Sprintf("%064x", i+1),
			Type:     6,
			Cbor:     make([]byte, 100),
			LastSeen: time.Now().Add(-time.Duration(i) * time.Second),
		}
	}

	tests := []struct {
		name      string
		args      map[string]any
		wantLimit int
	}{
		{"default", map[string]any{}, 10},
		{"explicit", map[string]any{"limit": 7}, 7},
		{"clamped to max", map[string]any{"limit": 500}, 50},
		{"negative uses default", map[string]any{"limit": -3}, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mp := &testMempoolMock{
				capacity:  25 * 1024 * 1024,
				txs:       txs,
				sizeBytes: aggregateBytes,
			}
			ctx, cancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)
			defer cancel()
			server := mcp.NewServer(
				&mcp.Implementation{Name: "test-mp", Version: "1.0"}, nil,
			)
			RegisterCardanoTools(server, nil, nil, mp, "preview", 5*time.Second)
			clTr, srvTr := mcp.NewInMemoryTransports()
			_, err := server.Connect(ctx, srvTr, nil)
			require.NoError(t, err)
			cs, err := mcp.NewClient(
				&mcp.Implementation{Name: "client", Version: "1.0"}, nil,
			).Connect(ctx, clTr, nil)
			require.NoError(t, err)
			defer cs.Close()

			res, err := cs.CallTool(ctx, &mcp.CallToolParams{
				Name:      "get_mempool_info",
				Arguments: tc.args,
			})
			require.NoError(t, err)
			require.False(t, res.IsError)
			text := res.Content[0].(*mcp.TextContent).Text

			full, bounded := mp.enumerations()
			assert.Zero(t, full, "summary must not enumerate the whole pool")
			assert.Equal(t, []int{tc.wantLimit}, bounded)
			assert.Contains(t, text, "Pending Transactions**: 120")
			assert.Contains(
				t, text, fmt.Sprintf("Buffered Volume**: %d bytes", aggregateBytes),
			)
			assert.Equal(
				t, tc.wantLimit, strings.Count(text, "| `"),
				"table rows must equal the display limit",
			)
			prev := -1
			for i := range tc.wantLimit {
				pos := strings.Index(text, fmt.Sprintf("| `%064x` |", i+1))
				require.GreaterOrEqual(t, pos, 0, "row %d missing", i)
				require.Greater(t, pos, prev, "row %d out of queue order", i)
				prev = pos
			}
			assert.Contains(
				t, text,
				fmt.Sprintf("showing up to %d of %d", tc.wantLimit, poolSize),
			)
		})
	}
}
