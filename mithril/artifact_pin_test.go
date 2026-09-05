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

package mithril

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixtureLedgerStateSlot is the tip slot of the ledger state a
// validImmutable+fallbackLedgerState fixture carries, and therefore the slot
// half of the ledger-state import key.
const fixtureLedgerStateSlot uint64 = 1000

// importKeyFor rebuilds the ledger-state import key the production path builds
// in mithril.importLedgerState: the first 16 characters of the artifact digest,
// a colon, and the snapshot's tip slot.
func importKeyFor(digest string, slot uint64) string {
	if len(digest) > 16 {
		digest = digest[:16]
	}
	return fmt.Sprintf("%s:%d", digest, slot)
}

// seedInterruptedImport puts dataDir into the state an interrupted Mithril v2
// bootstrap leaves behind: sync_status set (so Sync dispatches as a resume),
// the artifact pin the interrupted run recorded, and a ledger-state phase
// checkpoint under that artifact's import key.
//
// pin is skipped when digest is empty, which is the state a build without
// artifact pinning left.
func seedInterruptedImport(t *testing.T, dataDir, digest string, epoch uint64) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		StorageMode: "core",
	})
	require.NoError(t, err)
	require.NoError(
		t, db.SetSyncState("sync_status", syncStatusInProgress, nil),
	)
	if digest != "" {
		require.NoError(t, setPinnedArtifact(db, pinnedArtifact{
			Backend:             BackendV2,
			Network:             "preprod",
			Digest:              digest,
			Epoch:               epoch,
			ImmutableFileNumber: 0,
		}))
		require.NoError(t, db.Metadata().SetImportCheckpoint(
			&models.ImportCheckpoint{
				ImportKey: importKeyFor(digest, fixtureLedgerStateSlot),
				Phase:     models.ImportPhaseUTxO,
			},
			nil,
		))
	}
	require.NoError(t, dbtest.CloseDatabase(db))
}

// openSyncedDB reopens a database Sync has finished with, for assertions.
func openSyncedDB(t *testing.T, dataDir string) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		StorageMode: "core",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	return db
}

func syncConfigForFixture(
	fixture *v2Fixture,
	dataDir string,
) SyncConfig {
	return SyncConfig{
		Network:           "preprod",
		DataDir:           dataDir,
		StorageMode:       "core",
		Backend:           BackendV2,
		AggregatorURL:     fixture.server.URL,
		AllowInsecureHTTP: true,
		VerifyCertChain:   false,
		CleanupAfterLoad:  false,
		StoragePlugins:    testStoragePlugins(),
		DatabaseWorkers:   1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestSyncResumesPinnedArtifactAfterAggregatorAdvance drives the real Sync
// dispatch, not a helper that reads the pin: the database is left exactly as an
// interrupted import leaves it (sync_status set, artifact pinned, a ledger-state
// phase checkpoint under that artifact's import key) and the aggregator then
// publishes a newer artifact, which is what GetLatestCardanoDatabaseSnapshot
// would otherwise select.
//
// Without the pin the run imports the newer artifact's ledger state on top of
// the partially imported rows of the pinned one. The import is
// insert-if-absent and runs with reconciliation disabled on the bootstrap path,
// so the database ends up holding the union of two snapshots' live sets, and
// the phase checkpoint — keyed by "{digest}:{slot}" — is silently orphaned
// under the old key while a second one appears under the new one. All three
// assertions below fail in that case.
func TestSyncResumesPinnedArtifactAfterAggregatorAdvance(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		missingAncillary:    true,
		validImmutable:      true,
		fallbackLedgerState: true,
	})
	pinned := fixture.artifact
	newer := fixture.publishNewerArtifact(t)

	dataDir := t.TempDir()
	seedInterruptedImport(t, dataDir, pinned.Hash, pinned.Beacon.Epoch)

	result, err := Sync(
		context.Background(), syncConfigForFixture(fixture, dataDir),
	)
	require.NoError(t, err)
	require.NotNil(t, result.Snapshot)
	assert.Equal(
		t, pinned.Hash, result.Snapshot.Digest,
		"a resumed sync must import the artifact it pinned, not the newest "+
			"one the aggregator now publishes",
	)

	db := openSyncedDB(t, dataDir)
	pinnedCheckpoint, err := db.Metadata().GetImportCheckpoint(
		importKeyFor(pinned.Hash, fixtureLedgerStateSlot), nil,
	)
	require.NoError(t, err)
	require.NotNil(
		t, pinnedCheckpoint,
		"the pinned artifact's import checkpoint must be the one carried "+
			"forward",
	)
	assert.Equal(t, models.ImportPhaseTip, pinnedCheckpoint.Phase)

	newerCheckpoint, err := db.Metadata().GetImportCheckpoint(
		importKeyFor(newer.Hash, fixtureLedgerStateSlot), nil,
	)
	require.NoError(t, err)
	assert.Nil(
		t, newerCheckpoint,
		"no import may have run under the newer artifact's key",
	)
}

// TestSyncFreshBootstrapSelectsLatestArtifact is the negative case: pinning
// must not make a clean, uninterrupted run stick to anything. A database with
// no in-progress marker takes the newest published artifact.
func TestSyncFreshBootstrapSelectsLatestArtifact(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		missingAncillary:    true,
		validImmutable:      true,
		fallbackLedgerState: true,
	})
	newer := fixture.publishNewerArtifact(t)

	dataDir := t.TempDir()
	result, err := Sync(
		context.Background(), syncConfigForFixture(fixture, dataDir),
	)
	require.NoError(t, err)
	require.NotNil(t, result.Snapshot)
	assert.Equal(t, newer.Hash, result.Snapshot.Digest)

	// A completed sync clears the whole ephemeral sync lifecycle, so the pin
	// it recorded before downloading must not survive it.
	db := openSyncedDB(t, dataDir)
	_, hasPin, err := getPinnedArtifact(db)
	require.NoError(t, err)
	assert.False(t, hasPin, "a completed sync must leave no artifact pin")
}

// TestSyncRefusesResumeWithoutArtifactPin covers the interrupted database a
// build without artifact pinning left: the artifact its partial rows belong to
// cannot be identified, so no artifact may be imported over them.
func TestSyncRefusesResumeWithoutArtifactPin(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		missingAncillary:    true,
		validImmutable:      true,
		fallbackLedgerState: true,
	})
	dataDir := t.TempDir()
	seedInterruptedImport(t, dataDir, "", 0)

	_, err := Sync(
		context.Background(), syncConfigForFixture(fixture, dataDir),
	)
	require.ErrorIs(t, err, errNoArtifactPin)
}

// TestSyncRefusesResumeWhenPinnedArtifactIsGone covers the aggregator having
// dropped the pinned artifact: an explicit recovery decision rather than a
// silent switch to whatever is published now.
func TestSyncRefusesResumeWhenPinnedArtifactIsGone(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		missingAncillary:    true,
		validImmutable:      true,
		fallbackLedgerState: true,
	})
	gone := "0000000000000000000000000000000000000000000000000000000000000001"
	dataDir := t.TempDir()
	seedInterruptedImport(t, dataDir, gone, 294)

	_, err := Sync(
		context.Background(), syncConfigForFixture(fixture, dataDir),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "fetching pinned Cardano database snapshot")
	assert.ErrorContains(t, err, gone)
}

// TestSyncRefusesResumeWhenCertifiedTipMoved covers the cache/checkpoint
// mismatch case: the pinned artifact still resolves, but the extraction it
// resolves to no longer produces the certified ImmutableDB tip the interrupted
// run recorded.
func TestSyncRefusesResumeWhenCertifiedTipMoved(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		missingAncillary:    true,
		validImmutable:      true,
		fallbackLedgerState: true,
	})
	dataDir := t.TempDir()
	seedInterruptedImport(
		t, dataDir, fixture.artifact.Hash, fixture.artifact.Beacon.Epoch,
	)

	db := openSyncedDB(t, dataDir)
	pin, ok, err := getPinnedArtifact(db)
	require.NoError(t, err)
	require.True(t, ok)
	pin.CertifiedTipSlot = fixtureLedgerStateSlot + 5000
	require.NoError(t, setPinnedArtifact(db, pin))
	require.NoError(t, dbtest.CloseDatabase(db))

	_, err = Sync(
		context.Background(), syncConfigForFixture(fixture, dataDir),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "certified ImmutableDB tip")
	assert.ErrorContains(t, err, "no longer matches the pinned artifact")
}
