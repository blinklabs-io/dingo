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

package blockfrost

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupPoolMetadataRow creates a pool with a registered metadata pointer
// (URL/hash) and a cached OffchainMetadata row with the given status,
// content, and LastError, exercising the exact real-DB path PoolMetadata
// reads from (metadata store -> pool registration -> offchain_metadata cache
// row), rather than a hand-built PoolMetadataInfo. seed distinguishes the
// pool key hash, URL, and on-chain hash across subtests sharing one
// adapter/store.
func setupPoolMetadataRow(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	seed byte,
	status string,
	content []byte,
	lastError string,
) (poolID string) {
	t.Helper()
	poolKeyHash := bytes.Repeat([]byte{seed}, 28)
	metadataHash := bytes.Repeat([]byte{seed}, 32)
	url := "https://example.com/pool-" + hex.EncodeToString([]byte{seed}) +
		".json"

	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		Registration: []models.PoolRegistration{
			{
				PoolKeyHash:  poolKeyHash,
				MetadataUrl:  url,
				MetadataHash: metadataHash,
				AddedSlot:    1,
			},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)

	doc := models.OffchainMetadata{
		SourceType: models.OffchainMetadataSourcePool,
		URL:        url,
		Hash:       metadataHash,
		Status:     status,
		Content:    content,
		LastError:  lastError,
	}
	require.NoError(t, store.DB().Create(&doc).Error)

	return hex.EncodeToString(poolKeyHash)
}

// TestNodeAdapterPoolMetadataRejectsEmptyContent guards against the exact
// regression reported in #2995: a cached row with empty fetched content must
// not resolve as a successful URL/hash-only response. It must classify as
// DECODE_ERROR, matching cardano-api's validateAndHashStakePoolMetadata,
// which fails to decode empty bytes as JSON.
func TestNodeAdapterPoolMetadataRejectsEmptyContent(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	poolID := setupPoolMetadataRow(
		t, store, 0x01, models.OffchainMetadataStatusFetched, []byte{}, "",
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.NotNil(t, info.URL)
	require.NotNil(t, info.Hash)
	require.NotNil(t, info.Error)
	assert.Equal(t, "DECODE_ERROR", info.Error.Code)
	assert.Nil(t, info.Name)
	assert.Nil(t, info.Description)
	assert.Nil(t, info.Ticker)
	assert.Nil(t, info.Homepage)
}

// TestNodeAdapterPoolMetadataRejectsEmptyObject guards against the second
// regression in #2995: "{}" unmarshals successfully into an all-pointer
// struct with every field left nil. It must classify as DECODE_ERROR instead
// of silently returning a response with all four fields absent.
func TestNodeAdapterPoolMetadataRejectsEmptyObject(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	poolID := setupPoolMetadataRow(
		t,
		store,
		0x02,
		models.OffchainMetadataStatusFetched,
		[]byte(`{}`),
		"",
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.NotNil(t, info.Error)
	assert.Equal(t, "DECODE_ERROR", info.Error.Code)
	assert.Nil(t, info.Name)
	assert.Nil(t, info.Description)
	assert.Nil(t, info.Ticker)
	assert.Nil(t, info.Homepage)
}

// TestNodeAdapterPoolMetadataRejectsMissingRequiredField covers a document
// missing one required field (ticker): stake-pool field constraints must be
// enforced even though every PoolMetadataInfo field is a pointer.
func TestNodeAdapterPoolMetadataRejectsMissingRequiredField(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	content := []byte(
		`{"name":"Test Pool","description":"A pool.",` +
			`"homepage":"https://example.com"}`,
	)
	poolID := setupPoolMetadataRow(
		t, store, 0x03, models.OffchainMetadataStatusFetched, content, "",
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.NotNil(t, info.Error)
	assert.Equal(t, "DECODE_ERROR", info.Error.Code)
	assert.Nil(t, info.Ticker)
}

// TestNodeAdapterPoolMetadataRejectsInvalidFieldConstraint covers a document
// with every required field present but one violating its length
// constraint (a 2-character ticker; the reference validator requires 3-5).
func TestNodeAdapterPoolMetadataRejectsInvalidFieldConstraint(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	content := []byte(
		`{"name":"Test Pool","description":"A pool.","ticker":"AB",` +
			`"homepage":"https://example.com"}`,
	)
	poolID := setupPoolMetadataRow(
		t, store, 0x04, models.OffchainMetadataStatusFetched, content, "",
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.NotNil(t, info.Error)
	assert.Equal(t, "DECODE_ERROR", info.Error.Code)
	assert.Nil(t, info.Ticker)
}

// TestNodeAdapterPoolMetadataRejectsOversizedHashValidContent covers a
// hash-valid document that exceeds the stake-pool 512-byte limit: it must
// classify as SIZE_EXCEEDED even though it is well under the shared
// off-chain fetcher's generic 1 MiB limit, and even though this row is
// (as a legacy row would be) already persisted as Fetched.
func TestNodeAdapterPoolMetadataRejectsOversizedHashValidContent(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	padding := strings.Repeat("a", 500)
	content := []byte(
		`{"name":"` + padding + `","description":"d","ticker":"TEST",` +
			`"homepage":"https://example.com"}`,
	)
	require.Greater(t, len(content), 512)
	poolID := setupPoolMetadataRow(
		t, store, 0x05, models.OffchainMetadataStatusFetched, content, "",
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.NotNil(t, info.Error)
	assert.Equal(t, "SIZE_EXCEEDED", info.Error.Code)
	assert.Nil(t, info.Name)
}

// TestNodeAdapterPoolMetadataAcceptsValidContent verifies that valid,
// hash-matching stake-pool metadata continues to populate all four fields:
// this fix must not regress the success path.
func TestNodeAdapterPoolMetadataAcceptsValidContent(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	content := []byte(
		`{"name":"Test Pool","description":"A pool used for testing.",` +
			`"ticker":"TEST","homepage":"https://example.com"}`,
	)
	poolID := setupPoolMetadataRow(
		t, store, 0x06, models.OffchainMetadataStatusFetched, content, "",
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.Nil(t, info.Error)
	require.NotNil(t, info.Name)
	require.NotNil(t, info.Description)
	require.NotNil(t, info.Ticker)
	require.NotNil(t, info.Homepage)
	assert.Equal(t, "Test Pool", *info.Name)
	assert.Equal(t, "A pool used for testing.", *info.Description)
	assert.Equal(t, "TEST", *info.Ticker)
	assert.Equal(t, "https://example.com", *info.Homepage)
}

// TestNodeAdapterPoolMetadataSurfacesFetcherPersistedSizeExceeded verifies
// that a row the fetcher itself marked Failed with the fetch-time size
// classification (rather than a legacy Fetched row later found invalid on
// read) surfaces the same SIZE_EXCEEDED classification through the existing
// offchainFetchError path, confirming the two paths agree.
func TestNodeAdapterPoolMetadataSurfacesFetcherPersistedSizeExceeded(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	poolID := setupPoolMetadataRow(
		t,
		store,
		0x07,
		models.OffchainMetadataStatusFailed,
		nil,
		models.OffchainFetchErrBodyTooLargePrefix+" 512 bytes",
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.NotNil(t, info.Error)
	assert.Equal(t, "SIZE_EXCEEDED", info.Error.Code)
}

// TestNodeAdapterPoolMetadataSurfacesFetcherPersistedDecodeError verifies
// that a row the fetcher itself marked Failed with the fetch-time decode
// classification surfaces DECODE_ERROR through the existing
// offchainFetchError path.
func TestNodeAdapterPoolMetadataSurfacesFetcherPersistedDecodeError(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	poolID := setupPoolMetadataRow(
		t,
		store,
		0x08,
		models.OffchainMetadataStatusFailed,
		nil,
		models.OffchainFetchErrDecodeErrorPrefix+
			`: missing required field "name"`,
	)

	info, err := adapter.PoolMetadata(poolID)

	require.NoError(t, err)
	require.NotNil(t, info.Error)
	assert.Equal(t, "DECODE_ERROR", info.Error.Code)
}
