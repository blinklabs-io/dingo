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

package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type tokenRegistryStore interface {
	UpsertTokenRegistryEntries(
		context.Context,
		[]models.TokenRegistryEntry,
		time.Time,
		types.Txn,
	) (int, error)
	GetTokenRegistryEntry(
		string,
		types.Txn,
	) (*models.TokenRegistryEntry, error)
}

//go:fix inline
func intPtr(v int) *int { return new(v) }

var testSyncedAt = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

const (
	testSubjectNut  = "00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae6e7574636f696e"
	testSubjectDjed = "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344"
)

func TestSharedSQLStoreTokenRegistryRoundTrip(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	exerciseTokenRegistryStore(t, store)
}

func exerciseTokenRegistryStore(t *testing.T, store tokenRegistryStore) {
	t.Helper()
	ctx := t.Context()

	written, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{
			{
				Subject:     testSubjectNut,
				Name:        "nutcoin",
				Ticker:      "NUT",
				Description: "The legendary Nutcoin.",
				URL:         "https://fivebinaries.com/nutcoin",
				Logo:        "iVBORw0KGgo=",
			},
			{
				Subject:  testSubjectDjed,
				Name:     "Djed USD",
				Ticker:   "DJED",
				Decimals: new(6),
			},
		},
		testSyncedAt,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, written)

	nut, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, nut)
	require.Equal(t, testSubjectNut, nut.Subject)
	require.Equal(t, "nutcoin", nut.Name)
	require.Equal(t, "NUT", nut.Ticker)
	require.Equal(t, "The legendary Nutcoin.", nut.Description)
	require.Equal(t, "https://fivebinaries.com/nutcoin", nut.URL)
	require.Equal(t, "iVBORw0KGgo=", nut.Logo)
	require.Nil(t, nut.Decimals, "absent decimals must not read back as zero")

	djed, err := store.GetTokenRegistryEntry(testSubjectDjed, nil)
	require.NoError(t, err)
	require.NotNil(t, djed)
	require.NotNil(t, djed.Decimals)
	require.Equal(t, 6, *djed.Decimals)
	require.Empty(t, djed.Logo)
}

func TestSharedSQLStoreTokenRegistryUpsertReplacesProperties(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	ctx := t.Context()

	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{
			Subject:  testSubjectNut,
			Name:     "old name",
			Ticker:   "OLD",
			Decimals: new(2),
		}},
		testSyncedAt,
		nil,
	)
	require.NoError(t, err)

	// A later sync is authoritative: a property dropped upstream must be
	// cleared here too, otherwise the node keeps serving a ticker or a
	// decimals value the registry has since removed.
	_, err = store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{
			Subject: testSubjectNut,
			Name:    "new name",
		}},
		testSyncedAt.Add(time.Hour),
		nil,
	)
	require.NoError(t, err)

	entry, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "new name", entry.Name)
	require.Empty(t, entry.Ticker)
	require.Nil(t, entry.Decimals)
}

func TestSharedSQLStoreTokenRegistryMissingSubject(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	entry, err := store.GetTokenRegistryEntry(testSubjectNut, nil)

	require.NoError(t, err, "an unknown subject is absence, not an error")
	require.Nil(t, entry)
}

func TestSharedSQLStoreTokenRegistryLookupNormalizesSubject(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	ctx := t.Context()

	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{
			Subject: testSubjectNut,
			Name:    "nutcoin",
		}},
		testSyncedAt,
		nil,
	)
	require.NoError(t, err)

	entry, err := store.GetTokenRegistryEntry(
		strings.ToUpper(testSubjectNut),
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "nutcoin", entry.Name)
}

func TestSharedSQLStoreTokenRegistryUpsertEmptyBatch(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	written, err := store.UpsertTokenRegistryEntries(
		t.Context(),
		nil,
		testSyncedAt,
		nil,
	)

	require.NoError(t, err)
	require.Zero(t, written)
}

func TestSharedSQLStoreTokenRegistryRejectsBlankSubject(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	_, err := store.UpsertTokenRegistryEntries(
		t.Context(),
		[]models.TokenRegistryEntry{{Name: "no subject"}},
		testSyncedAt,
		nil,
	)

	require.Error(t, err)
}

type tokenRegistryPruneStore interface {
	tokenRegistryStore
	PruneTokenRegistryEntriesBefore(
		context.Context,
		time.Time,
		types.Txn,
	) (int, error)
}

// TestSharedSQLStoreTokenRegistryPrune covers the reconciliation half of a
// snapshot: subjects the upstream registry has dropped must stop being served,
// which an upsert-only sync cannot achieve on its own.
func TestSharedSQLStoreTokenRegistryPrune(t *testing.T) {
	t.Parallel()
	var store tokenRegistryPruneStore
	sqlStore, _ := newSharedSQLStore(t)
	store = sqlStore
	ctx := t.Context()

	firstSync := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{
			{Subject: testSubjectNut, Name: "nutcoin"},
			{Subject: testSubjectDjed, Name: "Djed USD"},
		},
		firstSync,
		nil,
	)
	require.NoError(t, err)

	// A later snapshot carries only one of the two subjects.
	secondSync := firstSync.Add(time.Hour)
	_, err = store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{
			{Subject: testSubjectNut, Name: "nutcoin"},
		},
		secondSync,
		nil,
	)
	require.NoError(t, err)

	pruned, err := store.PruneTokenRegistryEntriesBefore(ctx, secondSync, nil)

	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	survivor, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, survivor)
	dropped, err := store.GetTokenRegistryEntry(testSubjectDjed, nil)
	require.NoError(t, err)
	require.Nil(t, dropped, "a subject absent from the snapshot must be gone")
}

func TestSharedSQLStoreTokenRegistryPruneKeepsCurrentSnapshot(t *testing.T) {
	t.Parallel()
	var store tokenRegistryPruneStore
	sqlStore, _ := newSharedSQLStore(t)
	store = sqlStore
	ctx := t.Context()

	syncedAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{Subject: testSubjectNut, Name: "nutcoin"}},
		syncedAt,
		nil,
	)
	require.NoError(t, err)

	// The cutoff is the snapshot's own stamp, so rows written by that
	// snapshot are at the boundary and must survive it.
	pruned, err := store.PruneTokenRegistryEntriesBefore(ctx, syncedAt, nil)

	require.NoError(t, err)
	require.Zero(t, pruned)
	entry, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, entry)
}
