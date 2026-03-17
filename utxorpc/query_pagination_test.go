package utxorpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

func TestPaginateSearchResults_NoPagination(t *testing.T) {
	items := []*query.AnyUtxoData{
		{},
		{},
		{},
	}

	paginated, nextToken, err := paginateSearchResults(items, "", 0)
	require.NoError(t, err)
	require.Len(t, paginated, 3)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_WithMaxItems_FirstPage(t *testing.T) {
	items := []*query.AnyUtxoData{
		{},
		{},
		{},
		{},
		{},
	}

	paginated, nextToken, err := paginateSearchResults(items, "", 2)
	require.NoError(t, err)
	require.Len(t, paginated, 2)
	require.Equal(t, "2", nextToken)
}

func TestPaginateSearchResults_WithMaxItems_SecondPage(t *testing.T) {
	items := []*query.AnyUtxoData{
		{},
		{},
		{},
		{},
		{},
	}

	paginated, nextToken, err := paginateSearchResults(items, "2", 2)
	require.NoError(t, err)
	require.Len(t, paginated, 2)
	require.Equal(t, "4", nextToken)
}

func TestPaginateSearchResults_LastPage_NoNextToken(t *testing.T) {
	items := []*query.AnyUtxoData{
		{},
		{},
		{},
		{},
	}

	paginated, nextToken, err := paginateSearchResults(items, "2", 5)
	require.NoError(t, err)
	require.Len(t, paginated, 2)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_StartTokenBeyondRange(t *testing.T) {
	items := []*query.AnyUtxoData{
		{},
		{},
	}

	paginated, nextToken, err := paginateSearchResults(items, "10", 2)
	require.NoError(t, err)
	require.Len(t, paginated, 0)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_InvalidStartToken(t *testing.T) {
	items := []*query.AnyUtxoData{
		{},
	}

	paginated, nextToken, err := paginateSearchResults(items, "not-a-number", 1)
	require.Error(t, err)
	require.Nil(t, paginated)
	require.Empty(t, nextToken)
}

func TestPaginateSearchResults_NegativeStartToken(t *testing.T) {
	items := []*query.AnyUtxoData{
		{},
	}

	paginated, nextToken, err := paginateSearchResults(items, "-1", 1)
	require.Error(t, err)
	require.Nil(t, paginated)
	require.Empty(t, nextToken)
}

