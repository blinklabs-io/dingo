// Copyright 2025 Blink Labs Software
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
	"testing"

	"github.com/stretchr/testify/require"

	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

func TestPaginateSearchResults_NoPagination(t *testing.T) {
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 00,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x02}, Index: 0}},
			slot:       100,
			blockIndex: 1,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x03}, Index: 0}},
			slot:       101,
			blockIndex: 0,
			outputIdx:  0,
		},
	}

	paginated, nextToken, err := paginateSearchResults(items, "", 0)
	require.NoError(t, err)
	require.Len(t, paginated, 3)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_WithMaxItems_FirstPage(t *testing.T) {
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x02}, Index: 0}},
			slot:       100,
			blockIndex: 1,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x03}, Index: 0}},
			slot:       101,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x04}, Index: 0}},
			slot:       101,
			blockIndex: 1,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x05}, Index: 0}},
			slot:       102,
			blockIndex: 0,
			outputIdx:  0,
		},
	}

	paginated, nextToken, err := paginateSearchResults(items, "", 2)
	require.NoError(t, err)
	require.Len(t, paginated, 2)
	require.Equal(t, "100:1:0", nextToken)
}

func TestPaginateSearchResults_WithMaxItems_SecondPage(t *testing.T) {
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x02}, Index: 0}},
			slot:       100,
			blockIndex: 1,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x03}, Index: 0}},
			slot:       101,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x04}, Index: 0}},
			slot:       101,
			blockIndex: 1,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x05}, Index: 0}},
			slot:       102,
			blockIndex: 0,
			outputIdx:  0,
		},
	}

	paginated, nextToken, err := paginateSearchResults(items, "100:1:0", 2)
	require.NoError(t, err)
	require.Len(t, paginated, 2)
	require.Equal(t, "101:1:0", nextToken)
}

func TestPaginateSearchResults_LastPage_NoNextToken(t *testing.T) {
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x02}, Index: 0}},
			slot:       100,
			blockIndex: 1,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x03}, Index: 0}},
			slot:       101,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x04}, Index: 0}},
			slot:       101,
			blockIndex: 1,
			outputIdx:  0,
		},
	}

	paginated, nextToken, err := paginateSearchResults(items, "100:1:0", 5)
	require.NoError(t, err)
	require.Len(t, paginated, 2)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_StartTokenBeyondRange(t *testing.T) {
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x02}, Index: 0}},
			slot:       100,
			blockIndex: 1,
			outputIdx:  0,
		},
	}

	paginated, nextToken, err := paginateSearchResults(items, "999:0:0", 2)
	require.NoError(t, err)
	require.Len(t, paginated, 0)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_InvalidStartToken(t *testing.T) {
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 0,
			outputIdx:  0,
		},
	}

	paginated, nextToken, err := paginateSearchResults(
		items,
		"invalid-format",
		1,
	)
	require.Error(t, err)
	require.Nil(t, paginated)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_InvalidOutputIndex(t *testing.T) {
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 0,
			outputIdx:  0,
		},
	}

	paginated, nextToken, err := paginateSearchResults(
		items,
		"100:0:invalid",
		1,
	)
	require.Error(t, err)
	require.Nil(t, paginated)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_CursorUtxoSpent_ContinuesFromNext(t *testing.T) {
	// Simulates: cursor points to slot 100, blockIndex 1, but that UTxO was spent
	// Should continue from the next UTxO in sort order (slot 101)
	items := []*utxoWithOrderingInfo{
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x01}, Index: 0}},
			slot:       100,
			blockIndex: 0,
			outputIdx:  0,
		},
		// UTxO at slot 100, blockIndex 1 was spent (not in list)
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x03}, Index: 0}},
			slot:       101,
			blockIndex: 0,
			outputIdx:  0,
		},
		{
			data:       &query.AnyUtxoData{TxoRef: &query.TxoRef{Hash: []byte{0x04}, Index: 0}},
			slot:       101,
			blockIndex: 1,
			outputIdx:  0,
		},
	}

	// Cursor points to the spent UTxO (100:1:0)
	paginated, nextToken, err := paginateSearchResults(items, "100:1:0", 2)
	require.NoError(t, err)
	require.Len(t, paginated, 2)
	// Should return items at slot 101
	require.Equal(t, []byte{0x03}, paginated[0].TxoRef.Hash)
	require.Equal(t, []byte{0x04}, paginated[1].TxoRef.Hash)
	require.Empty(t, nextToken) // No more items
}
