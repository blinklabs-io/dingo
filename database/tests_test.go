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

package database

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// BenchmarkTransactionCreate benchmarks creating a read-only transaction
func BenchmarkTransactionCreate(b *testing.B) {
	// Create a temporary database
	config := &Config{
		DataDir: "", // In-memory
	}
	db, err := newTestDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer() // Reset timer after setup
	for b.Loop() {
		txn := db.Transaction(context.Background(), false)
		if err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCloseIsIdempotentWithSizeMetrics(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir:      t.TempDir(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
	})

	require.NoError(t, err)
	require.NotNil(t, db.sizeMetricsStop)

	require.NoError(t, db.Close())
	require.NoError(t, db.Close())
}

// TestWriteDBInfoSidecarOnFirstStart pins that a normal, fully-configured
// first start actually produces the dbinfo sidecar on disk with the
// configured metadata plugin name -- without this, internal/settingsresolve's
// pre-open check would have nothing to read on any real database.
func TestWriteDBInfoSidecarOnFirstStart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: sqlite.ProviderName,
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	info, err := dbinfo.Read(dataDir)
	require.NoError(t, err)
	require.Equal(t, "sqlite", info.MetadataPlugin)
}

// TestWriteDBInfoSidecarSkippedOnPartialConfig guards the first of
// writeDBInfoSidecar's two guards: mithril/sync.go and
// database/lifecycle/restore.go reopen an existing database with a Config
// that never sets MetadataPlugin, so writing an empty plugin name would
// poison the pre-open check for every later, complete start against the
// same directory.
func TestWriteDBInfoSidecarSkippedOnPartialConfig(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
		// No MetadataPlugin, mirroring the partial-Config callers.
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	info, err := dbinfo.Read(dataDir)
	require.NoError(t, err)
	require.Empty(t, info.MetadataPlugin)
}

// TestWriteDBInfoSidecarNeverOverwritesExisting guards writeDBInfoSidecar's
// second guard: a sidecar already present, even one naming a different
// plugin than what is about to open successfully, must be left alone --
// overwriting it would erase the exact mismatch signal the file exists to
// carry.
func TestWriteDBInfoSidecarNeverOverwritesExisting(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	require.NoError(t, dbinfo.Write(dataDir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: "postgres",
	}))

	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	info, err := dbinfo.Read(dataDir)
	require.NoError(t, err)
	require.Equal(
		t,
		"postgres",
		info.MetadataPlugin,
		"an existing sidecar naming a different plugin must never be overwritten",
	)
}

// TestWriteDBInfoSidecarRecreatedOnSteadyStateStart is a regression test for
// a bug where evaluateAndPersistGates returned early whenever there was
// nothing new to write to node_settings_gate -- the normal case for every
// start after the first -- without ever reaching writeGateValues, the only
// place that called writeDBInfoSidecar. An operator who deleted the sidecar
// (or lost it to a partial restore) would never get it back on any later
// steady-state start, silently disabling internal/settingsresolve's
// pre-open metadata-plugin check from then on.
func TestWriteDBInfoSidecarRecreatedOnSteadyStateStart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	cfg := &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}

	db, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	sidecarPath := filepath.Join(dataDir, dbinfo.FileName)
	_, err = os.Stat(sidecarPath)
	require.NoError(t, err, "sidecar must exist after the first start")
	require.NoError(t, os.Remove(sidecarPath))

	// Reopen with the identical config: every gate already matches what is
	// persisted, so this start has nothing to write to node_settings_gate.
	reopened, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(reopened))

	info, err := dbinfo.Read(dataDir)
	require.NoError(
		t,
		err,
		"a deleted sidecar must be recreated even when no gate needed writing",
	)
	require.Equal(t, "sqlite", info.MetadataPlugin)
}

// sidecarTrapPath returns a path that behaves like an unusable data
// directory for dbinfo.Read/Write specifically: a plain file, not a
// directory, so any attempt to read or create a file "inside" it fails with
// ENOTDIR. Config.DataDir is independent of the metadata/blob stores'
// actual directories (newTestDatabaseAt resolves those separately), so this
// lets a test break only the sidecar path without touching the real store.
func sidecarTrapPath(t *testing.T) string {
	t.Helper()
	trap := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(trap, []byte("x"), 0o600))
	return trap
}

// TestNewDatabaseFailsWhenSidecarCannotBeEstablished pins Finding 3's fix:
// for a brand-new database, the sidecar is the only thing that will later
// stop a mistyped provider from silently creating a second, empty database
// beside the real one (there is no metadata_plugin gate row yet for
// settingsresolve to compare against -- this open is what creates it), so
// failing to establish it here must fail the open instead of warning and
// continuing.
func TestNewDatabaseFailsWhenSidecarCannotBeEstablished(t *testing.T) {
	t.Parallel()

	metaDir := t.TempDir()
	blobDir := t.TempDir()

	_, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        sidecarTrapPath(t),
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "dbinfo sidecar")
}

func TestSidecarFailureDoesNotLatchMetadataPluginGate(t *testing.T) {
	t.Parallel()

	metaDir := t.TempDir()
	blobDir := t.TempDir()
	trapDataDir := sidecarTrapPath(t)

	_, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        trapDataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.Error(t, err)
	func() {
		host := plugin.NewHost()
		require.NoError(t, sqlite.RegisterProvider(host))
		defer func() { require.NoError(t, host.Stop(context.Background())) }()
		store, resolveErr := plugin.Resolve[metadata.MetadataStore](
			context.Background(), host,
			plugin.CapabilityStorageMetadata, sqlite.ProviderName, nil,
			metadata.ProviderDependencies{DataDir: metaDir},
		)
		require.NoError(t, resolveErr)
		gates, getErr := store.GetNodeSettingsGates()
		require.NoError(t, getErr)
		_, hasMetadataPluginGate := gates["metadata_plugin"]
		require.False(t, hasMetadataPluginGate)
	}()

	dataDir := t.TempDir()
	db, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: sqlite.ProviderName,
	})
	require.NoError(t, err)

	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, sqlite.ProviderName, gates["metadata_plugin"])
}

// TestExistingDatabaseSidecarFailureIsNonFatal pins the other half of
// Finding 3: an already-established database (one with a prior
// writeGateValues call, so a metadata_plugin gate row already exists)
// backfilling a lost or never-written sidecar on a later gate write must
// still warn and continue, not fail the open -- node_settings_gate's own
// metadata_plugin gate is already the real enforcement for it by then.
func TestExistingDatabaseSidecarFailureIsNonFatal(t *testing.T) {
	t.Parallel()

	metaDir := t.TempDir()
	blobDir := t.TempDir()
	realDataDir := t.TempDir()

	db, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        realDataDir,
		StorageMode:    "api",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	// Reopen the same stores with a gate that still needs a genuine write
	// (storage_mode's permitted api -> core move) so this reaches
	// writeGateValues again, but point Config.DataDir at a sidecar trap
	// this time. legacy is already non-nil from the open above, so this is
	// not a new database.
	reopened, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        sidecarTrapPath(t),
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(
		t,
		err,
		"a sidecar failure while backfilling an already-established "+
			"database must not fail the open",
	)
	require.NoError(t, closeTestDatabase(reopened))
}

// domainAccessors pairs each narrowing accessor with the interface it is
// supposed to hand out. Method expressions rather than method values: these
// need no receiver, so they cannot be read as a nil dereference.
var domainAccessors = []struct {
	name     string
	accessor any
	want     reflect.Type
}{
	{
		"certificateStore",
		(*Database).certificateStore,
		reflect.TypeFor[metadata.CertificateStore](),
	},
	{
		"epochStore",
		(*Database).epochStore,
		reflect.TypeFor[metadata.EpochStore](),
	},
	{
		"governanceStore",
		(*Database).governanceStore,
		reflect.TypeFor[metadata.GovernanceStore](),
	},
	{
		"stakeSnapshotStore",
		(*Database).stakeSnapshotStore,
		reflect.TypeFor[metadata.StakeSnapshotStore](),
	},
	{
		"transactionStore",
		(*Database).transactionStore,
		reflect.TypeFor[metadata.TransactionStore](),
	},
	{
		"utxoStore",
		(*Database).utxoStore,
		reflect.TypeFor[metadata.UtxoStore](),
	},
}

// TestFacadesDependOnNarrowStores is the "used by callers" half of the
// domain split. Facade methods reach their backend through these accessors
// rather than through d.metadata, so the compiler -- not review -- is what
// stops a method in one domain from reaching into another.
//
// Asserting on each accessor's declared return type rather than on the
// value it returns is deliberate: widening one back to
// metadata.MetadataStore would keep every call site compiling and silently
// undo the narrowing, and a returned *sqlstore.Store satisfies the narrow
// interface either way.
func TestFacadesDependOnNarrowStores(t *testing.T) {
	t.Parallel()

	for _, a := range domainAccessors {
		t.Run(a.name, func(t *testing.T) {
			typ := reflect.TypeOf(a.accessor)
			require.Equal(t, 1, typ.NumOut())
			require.Equalf(
				t,
				a.want,
				typ.Out(0),
				"%s() must return its narrow domain interface; returning "+
					"MetadataStore re-widens every call site it serves",
				a.name,
			)
		})
	}
}

// TestDomainAccessorsReturnBackingStore checks the accessors hand back the
// store the Database was configured with.
//
// Identity rather than non-nilness: an accessor that ignored d.metadata and
// returned some other value would still be non-nil, so a NotNil assertion
// passes on exactly the bug worth catching here -- an accessor wired to the
// wrong source. Comparing against the pointer that was installed is what
// makes that a failure.
func TestDomainAccessorsReturnBackingStore(t *testing.T) {
	t.Parallel()

	backing := &stubDomainMetadataStore{}
	d := &Database{metadata: backing}

	require.Same(t, backing, d.certificateStore())
	require.Same(t, backing, d.epochStore())
	require.Same(t, backing, d.governanceStore())
	require.Same(t, backing, d.stakeSnapshotStore())
	require.Same(t, backing, d.transactionStore())
	require.Same(t, backing, d.utxoStore())
}

// stubDomainMetadataStore is a nil-method placeholder: the tests only need a
// distinguishable value of the composed interface type, never a call. It is
// used through a pointer so the assertions above can compare identity.
type stubDomainMetadataStore struct {
	metadata.MetadataStore
}

func TestGenesisTransactionMetadataErrorWithShortHashes(t *testing.T) {
	db := newTestDB(t)
	for _, length := range []int{0, 1, 7, 8, 32} {
		for _, shortTx := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("length_%d/short_tx_%t", length, shortTx),
				func(t *testing.T) {
					txHash := bytes.Repeat([]byte{0xab}, 32)
					blockHash := bytes.Repeat([]byte{0xcd}, 32)
					if shortTx {
						txHash = txHash[:length:length]
					} else {
						blockHash = blockHash[:length:length]
					}
					txn := db.Transaction(context.Background(), true)
					defer txn.Rollback() //nolint:errcheck
					// Keep the blob handle live while the metadata handle fails. This
					// reaches the genesis metadata error wrapper without writing outputs.
					require.NoError(t, txn.Metadata().Rollback())
					var err error
					require.NotPanics(t, func() {
						err = db.SetGenesisTransaction(
							context.Background(),
							txHash,
							blockHash,
							nil,
							nil,
							txn,
						)
					})
					require.ErrorIs(t, err, types.ErrNilTxn)
					require.ErrorContains(
						t,
						err,
						"SetGenesisTransaction failed for tx",
					)
				},
			)
		}
	}
}

// TestSetTransactionBatched_SameBatchProducerSpentViaInFlight ingests a
// producer and the transaction that spends its output into the SAME batch
// accumulator before any flush. When the consumer is processed the output it
// spends exists only in the in-flight accumulator — it has never been written
// to the metadata store. Correct provenance (the produced row ends up present
// and marked spent) proves the in-flight lookup carries same-batch provenance
// without depending on blob/metadata recovery.
func TestSetTransactionBatched_SameBatchProducerSpentViaInFlight(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		context.Background(),
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		context.Background(),
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	utxo, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(
		t,
		utxo,
		"same-batch produced output must be created at flush",
	)
	require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
	require.Equal(
		t,
		candidate.consumerTx.Hash().Bytes(),
		[]byte(utxo.SpentAtTxId),
	)
}

// TestSetTransactionBatched_CrossBatchProducerResolvesFromDB flushes the
// producer in one batch, then ingests the consumer in a second batch whose
// accumulator does not contain the producer. The spend must resolve through
// the metadata-store fallthrough, exactly as before this optimisation.
func TestSetTransactionBatched_CrossBatchProducerResolvesFromDB(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

	// Batch 1: ingest and flush the producer so its output is committed.
	acc1 := db.NewBatchAccumulator()
	txn1 := db.Transaction(context.Background(), true)
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		context.Background(),
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc1,
		txn1,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc1, txn1))
	require.NoError(t, txn1.Commit())
	txn1.Release()

	// The producer output is now a committed, live row — not in-flight.
	pre, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, pre)
	require.Zero(
		t,
		pre.DeletedSlot,
		"producer output must be live before the consumer batch",
	)

	// Batch 2: a fresh accumulator (empty in-flight index) ingests the
	// consumer; the spend must resolve via the metadata store.
	acc2 := db.NewBatchAccumulator()
	txn2 := db.Transaction(context.Background(), true)
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		context.Background(),
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc2,
		txn2,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc2, txn2))
	require.NoError(t, txn2.Commit())
	txn2.Release()

	post, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, post)
	require.Equal(t, candidate.consumerPoint.Slot, post.DeletedSlot)
	require.Equal(
		t,
		candidate.consumerTx.Hash().Bytes(),
		[]byte(post.SpentAtTxId),
	)
}

// TestSetTransactionBatched_MissingProducerNotFabricated ingests only the
// consumer, with no producer either in-flight or committed. A genuinely
// missing historical producer must not be hidden or fabricated: the in-flight
// optimisation only short-circuits real same-batch producers.
func TestSetTransactionBatched_MissingProducerNotFabricated(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		context.Background(),
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	utxo, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.Nil(
		t,
		utxo,
		"genuinely missing producer must not be hidden or fabricated",
	)
}

// ingestSameBatchProducerConsumer ingests the producer and consumer of the
// candidate through the batched path into one accumulator and flushes.
func ingestSameBatchProducerConsumer(
	t *testing.T,
	db *Database,
	candidate batchedCrossBlockSpendCandidate,
) {
	t.Helper()
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		context.Background(),
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0, nil, nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc, txn, BatchedTxIngestOpts{},
	))
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		context.Background(),
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0, nil, nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc, txn, BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
}

// TestSetTransactionBatched_InFlightDoesNotSkipExistingRowRepair guards the
// resumed-backfill case: when the consumed output already exists in metadata
// as a partially-written spent row (DeletedSlot == consumer slot, SpentAtTxId
// == nil from a prior partial run) and the producer is re-ingested into the
// same batch (so it is also "in-flight"), the consumer must still backfill the
// spender link. The in-flight short-circuit must run after the existing-row
// repair: the flush's batchSpendUtxos only updates rows where deleted_slot = 0
// and could not fix this row later.
//
// Two passes make the simulation faithful: the first pass ingests normally
// (creating the consumer transaction row that spent_at_tx_id references), then
// spent_at_tx_id is cleared to mimic the partial-write state, and the second
// pass must repair it.
func TestSetTransactionBatched_InFlightDoesNotSkipExistingRowRepair(
	t *testing.T,
) {
	t.Parallel()

	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

	// Pass 1: ingest normally so the output is spent and the consumer tx row
	// exists.
	ingestSameBatchProducerConsumer(t, db, candidate)

	// Mimic a partial prior run: the row stays deleted at the consumer slot
	// but loses its spender hash.
	raw := rawSQLiteMetadataFixture(t, db)
	_, err := raw.Exec(`
UPDATE utxo SET spent_at_tx_id = NULL
WHERE tx_id = ? AND output_idx = ?`,
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
	)
	require.NoError(t, err)

	pre, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(), candidate.input.Index(), nil,
	)
	require.NoError(t, err)
	require.NotNil(t, pre)
	require.Equal(t, candidate.consumerPoint.Slot, pre.DeletedSlot)
	require.Nil(t, pre.SpentAtTxId, "precondition: spender hash cleared")

	// Pass 2: re-ingest. The producer is in-flight again, but because the row
	// already exists the consumer must repair the spender link rather than
	// short-circuit on the in-flight lookup.
	ingestSameBatchProducerConsumer(t, db, candidate)

	post, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(), candidate.input.Index(), nil,
	)
	require.NoError(t, err)
	require.NotNil(t, post)
	require.Equal(t, candidate.consumerPoint.Slot, post.DeletedSlot)
	require.Equal(
		t,
		candidate.consumerTx.Hash().Bytes(),
		[]byte(post.SpentAtTxId),
		"spender link must be backfilled for a pre-existing same-slot row",
	)
}

// TestSetGapBlockTransactionPersistsPositionedCertificates verifies that the
// gap-block ingestion path writes the same certificate provenance consumed by
// transaction hydration and account history readers. Both transactions share
// a slot so the history query must use block_index as its tie-breaker, and the
// second write is repeated to cover the certificate upsert path.
func TestSetGapBlockTransactionPersistsPositionedCertificates(t *testing.T) {
	db := openTestDB(t)

	stakeKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x31}, 28))
	credential := lcommon.Credential{
		CredType:   0,
		Credential: stakeKey,
	}

	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)

	buildTransaction := func(seed byte, poolSeed byte) lcommon.Transaction {
		t.Helper()
		input, inputErr := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{seed + 0x10}, 32),
			0,
		)
		require.NoError(t, inputErr)
		poolKey := lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{poolSeed}, 28)),
		)
		builder := mockledger.NewTransactionBuilder()
		builder.WithId(bytes.Repeat([]byte{seed}, 32))
		builder.WithInputs(input)
		builder.WithOutputs(output)
		builder.WithCertificates(&lcommon.StakeDelegationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeDelegation),
			StakeCredential: &credential,
			PoolKeyHash:     poolKey,
		})
		transaction, buildErr := builder.Build()
		require.NoError(t, buildErr)
		return transaction
	}

	const slot = uint64(42_000)
	point := ocommon.Point{
		Slot: slot,
		Hash: bytes.Repeat([]byte{0xA1}, 32),
	}
	late := buildTransaction(0x41, 0x51)
	early := buildTransaction(0x42, 0x52)
	seedInputs := make([]models.Utxo, 0, 2)
	for _, transaction := range []lcommon.Transaction{late, early} {
		input := transaction.Consumed()[0]
		seedInputs = append(seedInputs, models.Utxo{
			TxId:      input.Id().Bytes(),
			OutputIdx: input.Index(),
			AddedSlot: 1,
			Amount:    1_000_000,
		})
	}
	seedTxn := db.MetadataTxn(context.Background(), true)
	require.NoError(t, seedTxn.Do(func(txn *Txn) error {
		return db.Metadata().ImportUtxos(seedInputs, txn.Metadata())
	}))
	seedTxn.Release()

	gapOffsets := func(transaction lcommon.Transaction) *BlockIngestionResult {
		t.Helper()
		var blockHash [32]byte
		copy(blockHash[:], point.Hash)
		var txHash [32]byte
		copy(txHash[:], transaction.Hash().Bytes())
		return &BlockIngestionResult{
			TxOffsets: map[[32]byte]CborOffset{
				txHash: {
					BlockSlot:  slot,
					BlockHash:  blockHash,
					ByteLength: 1,
				},
			},
			UtxoOffsets: map[UtxoRef]CborOffset{
				{TxId: txHash, OutputIdx: 0}: {
					BlockSlot:  slot,
					BlockHash:  blockHash,
					ByteLength: 1,
				},
			},
		}
	}

	// Deliberately ingest the higher block index first. The reader's ordering
	// must come from persisted transaction/certificate positions, not insertion
	// order or SQLite row IDs.
	require.NoError(t, db.SetGapBlockTransaction(
		context.Background(),
		late, point, 5, nil, gapOffsets(late), nil,
	))
	require.NoError(t, db.SetGapBlockTransaction(
		context.Background(),
		early, point, 2, nil, gapOffsets(early), nil,
	))
	// Reprocessing the same gap transaction must not duplicate its certificate.
	require.NoError(t, db.SetGapBlockTransaction(
		context.Background(),
		late, point, 5, nil, gapOffsets(late), nil,
	))

	gotTx, err := db.Metadata().GetTransactionByHash(late.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, gotTx)
	require.Len(t, gotTx.Certificates, 1)
	require.Equal(t, uint(0), gotTx.Certificates[0].CertIndex)
	require.Equal(t, uint(lcommon.CertificateTypeStakeDelegation), gotTx.Certificates[0].CertType)

	history, err := db.GetAccountDelegationHistoryByCredential(
		context.Background(),
		0,
		stakeKey.Bytes(),
		0,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, uint32(2), history[0].BlockIndex)
	require.Equal(t, uint32(5), history[1].BlockIndex)
	require.Equal(t, uint32(0), history[0].CertIndex)
	require.Equal(t, uint32(0), history[1].CertIndex)
	require.Equal(t, slot, history[0].AddedSlot)
	require.Equal(t, slot, history[1].AddedSlot)
}

type gapRollbackCandidate struct {
	consumerBlock  models.Block
	consumerPoint  ocommon.Point
	consumerTx     lcommon.Transaction
	producerBlocks []models.Block
}

// gapProducerTx identifies one of the transactions that produced an
// input consumed by a gap-block test candidate. It keeps the producer
// tx and its block point so the test can feed both halves of the
// produce→consume pair through SetGapBlockTransaction.
type gapProducerTx struct {
	block models.Block
	point ocommon.Point
	tx    lcommon.Transaction
}

type gapConsumeCandidate struct {
	consumerBlock models.Block
	consumerPoint ocommon.Point
	consumerTx    lcommon.Transaction
	producers     []gapProducerTx
}

type batchedCrossBlockSpendCandidate struct {
	producerBlock models.Block
	producerPoint ocommon.Point
	producerTx    lcommon.Transaction
	producerIdx   uint32
	consumerBlock models.Block
	consumerPoint ocommon.Point
	consumerTx    lcommon.Transaction
	consumerIdx   uint32
	input         lcommon.TransactionInput
}

func TestSetGapBlockTransactionRestoresConsumedInputsOnRollback(
	t *testing.T,
) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findGapRollbackCandidate(t)
	// The fixture contains three stake registrations and three pool
	// registrations. Preserve their protocol deposits instead of recording
	// an authoritative zero for the first certificate only.
	certificateDeposits := map[int]uint64{
		0: 2_000_000,
		1: 500_000_000,
		3: 2_000_000,
		4: 500_000_000,
		6: 2_000_000,
		7: 500_000_000,
	}
	for _, block := range candidate.producerBlocks {
		storeBlockOffsetsOnly(t, db, block)
	}
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	require.NoError(
		t,
		db.SetGapBlockTransaction(
			context.Background(),
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			certificateDeposits,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		),
	)

	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected gap-consumed input %s to be present for rollback",
			input.String(),
		)
		require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
		require.Equal(
			t,
			candidate.consumerTx.Hash().Bytes(),
			[]byte(utxo.SpentAtTxId),
		)
	}

	txn := db.MetadataTxn(context.Background(), true)
	require.NoError(
		t,
		txn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteTransactionsAfterSlot(
				candidate.consumerPoint.Slot-1,
				txn.Metadata(),
			)
		}),
	)
	txn.Release()

	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxo(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected rollback to restore gap-consumed input %s",
			input.String(),
		)
		require.Zero(t, utxo.DeletedSlot)
		require.Nil(t, utxo.SpentAtTxId)
	}
}

func TestSetTransactionRecoversMissingConsumedInputsFromBlob(
	t *testing.T,
) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findGapRollbackCandidateWithoutCertificates(t)
	for _, block := range candidate.producerBlocks {
		storeBlockOffsetsOnly(t, db, block)
	}
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	require.NoError(
		t,
		db.SetTransaction(
			context.Background(),
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		),
	)

	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected SetTransaction to recover consumed input %s",
			input.String(),
		)
		require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
		require.Equal(
			t,
			candidate.consumerTx.Hash().Bytes(),
			[]byte(utxo.SpentAtTxId),
		)
	}
}

func TestSetTransactionBatchedSpendsPreviousBlockOutputInSameBatch(
	t *testing.T,
) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findBatchedCrossBlockSpendCandidate(t)
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(
		t,
		db.SetTransactionBatched(
			context.Background(),
			candidate.producerTx,
			candidate.producerPoint,
			candidate.producerIdx,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.producerBlock),
			acc,
			txn,
		),
	)
	require.NoError(
		t,
		db.SetTransactionBatched(
			context.Background(),
			candidate.consumerTx,
			candidate.consumerPoint,
			candidate.consumerIdx,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			acc,
			txn,
		),
	)
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	utxo, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, utxo)
	require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
	require.Equal(
		t,
		candidate.consumerTx.Hash().Bytes(),
		[]byte(utxo.SpentAtTxId),
	)
}

func findGapRollbackCandidate(t *testing.T) gapRollbackCandidate {
	return findGapRollbackCandidateMatching(t, false)
}

func findGapRollbackCandidateWithoutCertificates(
	t *testing.T,
) gapRollbackCandidate {
	return findGapRollbackCandidateMatching(t, true)
}

func findGapRollbackCandidateMatching(
	t *testing.T,
	requireNoCertificates bool,
) gapRollbackCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	seenOutputs := make(map[string]models.Block)
	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(
			immBlock.Type,
			immBlock.Cbor,
		)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		for _, tx := range block.Transactions() {
			if requireNoCertificates && len(tx.Certificates()) > 0 {
				for _, utxo := range tx.Produced() {
					seenOutputs[inputRefKey(
						utxo.Id.Id().Bytes(),
						utxo.Id.Index(),
					)] = blockModel
				}
				continue
			}
			consumed := tx.Consumed()
			if len(consumed) > 0 {
				producerByPoint := make(map[string]models.Block)
				allEarlier := true
				for _, input := range consumed {
					producerBlock, ok := seenOutputs[inputRefKey(
						input.Id().Bytes(),
						input.Index(),
					)]
					if !ok || producerBlock.Slot >= blockModel.Slot {
						allEarlier = false
						break
					}
					pointKey := fmt.Sprintf(
						"%d:%x",
						producerBlock.Slot,
						producerBlock.Hash,
					)
					producerByPoint[pointKey] = producerBlock
				}
				if allEarlier {
					producerBlocks := make(
						[]models.Block,
						0,
						len(producerByPoint),
					)
					for _, producerBlock := range producerByPoint {
						producerBlocks = append(
							producerBlocks,
							producerBlock,
						)
					}
					return gapRollbackCandidate{
						consumerBlock: blockModel,
						consumerPoint: ocommon.Point{
							Slot: blockModel.Slot,
							Hash: blockModel.Hash,
						},
						consumerTx:     tx,
						producerBlocks: producerBlocks,
					}
				}
			}
			for _, utxo := range tx.Produced() {
				seenOutputs[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = blockModel
			}
		}
	}

	t.Fatal("failed to find rollback candidate in immutable testdata")
	return gapRollbackCandidate{}
}

func storeBlockOffsetsOnly(t *testing.T, db *Database, block models.Block) {
	t.Helper()

	offsets := mustBlockOffsets(t, block)
	txn := db.Transaction(context.Background(), true)
	require.NoError(
		t,
		txn.Do(func(txn *Txn) error {
			if err := db.BlockCreate(block, txn); err != nil {
				return err
			}
			blob := txn.DB().Blob()
			for txHash, offset := range offsets.TxOffsets {
				if err := blob.SetTx(
					txn.Blob(),
					txHash[:],
					EncodeTxOffset(&offset),
				); err != nil {
					return err
				}
			}
			for ref, offset := range offsets.UtxoOffsets {
				if err := blob.SetUtxo(
					txn.Blob(),
					ref.TxId[:],
					ref.OutputIdx,
					EncodeUtxoOffset(&offset),
				); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	txn.Release()
}

func mustBlockOffsets(
	t *testing.T,
	block models.Block,
) *BlockIngestionResult {
	t.Helper()

	decodedBlock, err := gledger.NewBlockFromCbor(
		block.Type,
		block.Cbor,
	)
	require.NoError(t, err)
	indexer := NewBlockIndexer(block.Slot, block.Hash)
	offsets, err := indexer.ComputeOffsets(block.Cbor, decodedBlock)
	require.NoError(t, err)
	return &BlockIngestionResult{
		TxOffsets:   offsets.TxOffsets,
		UtxoOffsets: offsets.UtxoOffsets,
	}
}

func inputRefKey(txId []byte, outputIdx uint32) string {
	return fmt.Sprintf("%x:%d", txId, outputIdx)
}

// findGapConsumeCandidate walks the immutable testdata and returns a
// produce-then-consume pair where both halves are separately
// addressable transactions: the producer tx(s) that created the
// consumed outputs plus the consumer tx that later spent them in a
// different (later) block. Useful for exercising the full gap-block
// ingestion path through two SetGapBlockTransaction calls.
func findGapConsumeCandidate(t *testing.T) gapConsumeCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type producedEntry struct {
		block models.Block
		point ocommon.Point
		tx    lcommon.Transaction
	}
	produced := make(map[string]producedEntry)

	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		blockPoint := ocommon.Point{
			Slot: blockModel.Slot,
			Hash: blockModel.Hash,
		}
		for _, tx := range block.Transactions() {
			consumed := tx.Consumed()
			if len(consumed) > 0 {
				producerByKey := make(
					map[string]gapProducerTx,
				)
				allFound := true
				for _, input := range consumed {
					key := inputRefKey(
						input.Id().Bytes(),
						input.Index(),
					)
					entry, ok := produced[key]
					if !ok || entry.block.Slot >= blockModel.Slot {
						allFound = false
						break
					}
					pKey := fmt.Sprintf(
						"%d:%x:%x",
						entry.block.Slot,
						entry.block.Hash,
						entry.tx.Hash().Bytes(),
					)
					producerByKey[pKey] = gapProducerTx{
						block: entry.block,
						point: entry.point,
						tx:    entry.tx,
					}
				}
				if allFound {
					producers := make(
						[]gapProducerTx,
						0,
						len(producerByKey),
					)
					for _, p := range producerByKey {
						producers = append(producers, p)
					}
					return gapConsumeCandidate{
						consumerBlock: blockModel,
						consumerPoint: blockPoint,
						consumerTx:    tx,
						producers:     producers,
					}
				}
			}
			for _, utxo := range tx.Produced() {
				produced[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = producedEntry{
					block: blockModel,
					point: blockPoint,
					tx:    tx,
				}
			}
		}
	}

	t.Fatal("failed to find gap consume candidate in immutable testdata")
	return gapConsumeCandidate{}
}

// findGapConsumeCandidateWithoutCertificates is the cert-free variant
// of findGapConsumeCandidate. It walks the immutable testdata for the
// same produce-then-consume pair, but skips any transaction (in either
// position) that carries certificates so callers can drive the pair
// through SetTransaction with no certDeposits map.
func findGapConsumeCandidateWithoutCertificates(
	t *testing.T,
) gapConsumeCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type producedEntry struct {
		block models.Block
		point ocommon.Point
		tx    lcommon.Transaction
	}
	produced := make(map[string]producedEntry)

	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		blockPoint := ocommon.Point{
			Slot: blockModel.Slot,
			Hash: blockModel.Hash,
		}
		for _, tx := range block.Transactions() {
			if len(tx.Certificates()) > 0 {
				// A producer with certs would carry deposit-bearing
				// state we don't want to fixture-up; drop it from
				// both sides by not tracking its outputs and not
				// matching it as a consumer.
				continue
			}
			consumed := tx.Consumed()
			if len(consumed) > 0 {
				producerByKey := make(map[string]gapProducerTx)
				allFound := true
				for _, input := range consumed {
					key := inputRefKey(
						input.Id().Bytes(),
						input.Index(),
					)
					entry, ok := produced[key]
					if !ok || entry.block.Slot >= blockModel.Slot {
						allFound = false
						break
					}
					pKey := fmt.Sprintf(
						"%d:%x:%x",
						entry.block.Slot,
						entry.block.Hash,
						entry.tx.Hash().Bytes(),
					)
					producerByKey[pKey] = gapProducerTx{
						block: entry.block,
						point: entry.point,
						tx:    entry.tx,
					}
				}
				if allFound {
					producers := make(
						[]gapProducerTx,
						0,
						len(producerByKey),
					)
					for _, p := range producerByKey {
						producers = append(producers, p)
					}
					return gapConsumeCandidate{
						consumerBlock: blockModel,
						consumerPoint: blockPoint,
						consumerTx:    tx,
						producers:     producers,
					}
				}
			}
			for _, utxo := range tx.Produced() {
				produced[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = producedEntry{
					block: blockModel,
					point: blockPoint,
					tx:    tx,
				}
			}
		}
	}

	t.Fatal(
		"failed to find cert-free gap consume candidate in immutable testdata",
	)
	return gapConsumeCandidate{}
}

func findBatchedCrossBlockSpendCandidate(
	t *testing.T,
) batchedCrossBlockSpendCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type producedEntry struct {
		block models.Block
		point ocommon.Point
		tx    lcommon.Transaction
		idx   uint32
	}
	prevProduced := make(map[string]producedEntry)

	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		blockPoint := ocommon.Point{
			Slot: blockModel.Slot,
			Hash: blockModel.Hash,
		}

		for idx, tx := range block.Transactions() {
			if len(tx.Certificates()) > 0 {
				continue
			}
			for _, input := range tx.Consumed() {
				entry, ok := prevProduced[inputRefKey(
					input.Id().Bytes(),
					input.Index(),
				)]
				if !ok {
					continue
				}
				return batchedCrossBlockSpendCandidate{
					producerBlock: entry.block,
					producerPoint: entry.point,
					producerTx:    entry.tx,
					producerIdx:   entry.idx,
					consumerBlock: blockModel,
					consumerPoint: blockPoint,
					consumerTx:    tx,
					consumerIdx:   uint32(idx),
					input:         input,
				}
			}
		}

		prevProduced = make(map[string]producedEntry)
		for idx, tx := range block.Transactions() {
			if len(tx.Certificates()) > 0 {
				continue
			}
			for _, utxo := range tx.Produced() {
				prevProduced[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = producedEntry{
					block: blockModel,
					point: blockPoint,
					tx:    tx,
					idx:   uint32(idx),
				}
			}
		}
	}

	t.Fatal(
		"failed to find previous-block spend candidate in immutable testdata",
	)
	return batchedCrossBlockSpendCandidate{}
}

// TestSetGapBlockTransactionSpendsLiveProducedInputs verifies that when
// a gap-block transaction consumes a UTxO produced by an earlier
// gap-block transaction (both ingested through SetGapBlockTransaction),
// the produced UTxO row is marked as spent with both deleted_slot and
// spent_at_tx_id populated — matching the normal SetTransaction path.
//
// Regression test for a bug where ensureGapConsumedUtxos unconditionally
// skipped existing UTxO rows, leaving outputs produced by earlier gap
// blocks live forever even after a later gap block consumed them.
func TestSetGapBlockTransactionSpendsLiveProducedInputs(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findGapConsumeCandidate(t)
	certificateDeposits := map[int]uint64{
		0: 2_000_000,
		1: 500_000_000,
		3: 2_000_000,
		4: 500_000_000,
		6: 2_000_000,
		7: 500_000_000,
	}

	// Store blob offsets for all producer blocks plus the consumer
	// block so SetGapBlockTransaction can look up and persist
	// produced UTxO offset references.
	storedBlocks := make(map[string]struct{})
	for _, producer := range candidate.producers {
		key := fmt.Sprintf("%d:%x", producer.block.Slot, producer.block.Hash)
		if _, ok := storedBlocks[key]; ok {
			continue
		}
		storedBlocks[key] = struct{}{}
		storeBlockOffsetsOnly(t, db, producer.block)
	}
	consumerKey := fmt.Sprintf(
		"%d:%x",
		candidate.consumerBlock.Slot,
		candidate.consumerBlock.Hash,
	)
	if _, ok := storedBlocks[consumerKey]; !ok {
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
	}

	// Seed each producer tx's own consumed inputs as already-spent
	// rows (without a SpentAtTxId, to avoid needing a parent
	// transaction row for the Inputs FK) so the producer's
	// gap-block ingestion does not try to recover them from a blob
	// store that lacks the predecessor blocks. This mimics the
	// Mithril snapshot state where inputs from before the gap
	// window are already recorded as spent.
	//
	// Contract for the gap path: AddedSlot = DeletedSlot = seedSlot
	// with seedSlot != point.Slot drives ensureGapConsumedUtxos into
	// its "already spent by a different tx" branch (SpentAtTxId == nil
	// AND DeletedSlot != 0 AND DeletedSlot != point.Slot), so the
	// existing row is left untouched. Future changes to the gap-path
	// branch conditions must keep this skip path reachable or update
	// this seed accordingly.
	seedSlot := uint64(1)
	for _, producer := range candidate.producers {
		seeded := make([]models.Utxo, 0, len(producer.tx.Consumed()))
		for _, in := range producer.tx.Consumed() {
			seeded = append(seeded, models.Utxo{
				TxId:        in.Id().Bytes(),
				OutputIdx:   in.Index(),
				AddedSlot:   seedSlot,
				DeletedSlot: seedSlot,
			})
		}
		if len(seeded) > 0 {
			txn := db.MetadataTxn(context.Background(), true)
			require.NoError(
				t,
				txn.Do(func(txn *Txn) error {
					return db.Metadata().ImportUtxos(
						seeded,
						txn.Metadata(),
					)
				}),
			)
			txn.Release()
		}
	}

	// Ingest every producer tx through the gap path first, so the
	// consumed inputs exist as live UTxO rows when the consumer tx
	// arrives.
	for _, producer := range candidate.producers {
		require.NoError(
			t,
			db.SetGapBlockTransaction(
				context.Background(),
				producer.tx,
				producer.point,
				0,
				nil,
				mustBlockOffsets(t, producer.block),
				nil,
			),
		)
	}

	// Sanity: every consumed input is live before the consumer runs.
	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"producer gap tx did not persist input %s",
			input.String(),
		)
		require.Zero(
			t,
			utxo.DeletedSlot,
			"input %s should be live before consumer gap tx runs",
			input.String(),
		)
		require.Nil(
			t,
			utxo.SpentAtTxId,
			"input %s should be unspent before consumer gap tx runs",
			input.String(),
		)
	}

	// Now ingest the consumer gap tx. This must mark the produced
	// UTxOs as spent rather than silently skipping them.
	require.NoError(
		t,
		db.SetGapBlockTransaction(
			context.Background(),
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			certificateDeposits,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		),
	)

	spenderTxHash := candidate.consumerTx.Hash().Bytes()
	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected consumed input %s to remain in metadata",
			input.String(),
		)
		require.Equal(
			t,
			candidate.consumerPoint.Slot,
			utxo.DeletedSlot,
			"input %s deleted_slot not set to consumer slot",
			input.String(),
		)
		require.Equal(
			t,
			spenderTxHash,
			[]byte(utxo.SpentAtTxId),
			"input %s spent_at_tx_id not set to consumer tx hash",
			input.String(),
		)
	}
}

func TestPhase1PersistsNetworkMagicOnFirstStart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:      dataDir,
		StorageMode:  "core",
		Network:      "preprod",
		NetworkMagic: 1,
	})
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "1", gates["network_magic"])
	require.NoError(t, closeTestDatabase(db))
}

func TestPhase1RejectsNetworkMagicChange(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:      dataDir,
		StorageMode:  "core",
		Network:      "preprod",
		NetworkMagic: 1,
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	_, err = newTestDatabase(t, &Config{
		DataDir:      dataDir,
		StorageMode:  "core",
		Network:      "preprod",
		NetworkMagic: 2,
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
	require.Contains(t, settingsErr.Error(), "network magic")
}

// TestPhase1RecordsNoStartEraAndRejectsLaterDijkstra pins the fix for the
// common case a database that ran with no start era override previously
// recorded nothing at all for the gate (phase1GateValues emitted "" and
// FrozenFillOnce's first-start rule skips writing an empty configured
// value), so a later --start-era dijkstra against that same database was
// silently accepted as a first-time fill instead of rejected as the
// consensus-affecting flip the gate exists to freeze. A full caller (one
// that sets MetadataPlugin, distinguishing it from mithril/sync.go's and
// restore.go's partial reopen) must now persist nodesettings.NoStartEra
// instead, so the comparison actually happens on the next open.
func TestPhase1RecordsNoStartEraAndRejectsLaterDijkstra(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, nodesettings.NoStartEra, gates["start_era"])
	require.NoError(t, closeTestDatabase(db))

	_, err = newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		StartEra:       "dijkstra",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
	require.Contains(t, settingsErr.Error(), "dijkstra")
}

func TestPhase1RejectsCoreToAPI(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	_, err = newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
		Network:     "preprod",
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
}

// TestPhase1AllowsAPIToCore pins the round-3 fix for a latch write that
// was silently discarded: reopened.StorageMode() alone (the original
// assertion here) reads back d.config, not the persisted row, so it
// passes vacuously even when the write never reached the store -- which is
// exactly what happened before node_settings_gate became authoritative
// for storage_mode (see persistedGateValues's doc comment). This asserts
// the actual persisted gate value instead, and additionally that the
// latch really did latch: a further reopen as "api" must now be rejected,
// which is the behavior the whole test is meant to pin and which nothing
// here previously asserted.
func TestPhase1AllowsAPIToCore(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	gates, err := reopened.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "core", gates["storage_mode"])
	require.NoError(t, closeTestDatabase(reopened))

	_, err = newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
		Network:     "preprod",
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
}

// TestPhase1LatchAndNetworkFillTogether pins Critical 2 from round 3: when
// storage_mode latches (api -> core) and network is filled for the first
// time on the very same open, both gates must land in node_settings_gate,
// and the legacy node_settings row's network backfill (best-effort, for
// older tooling -- see writeGateValues's doc comment) must also succeed.
// The bug this guards was using the new, not-yet-persisted effective
// storage_mode ("core") as the backfill's WHERE match key while the row's
// actual physical storage_mode column was still "api" from first insert:
// the match always missed, and network was silently never recorded either
// in the legacy row or (in an earlier version of this fix) in
// node_settings_gate.
func TestPhase1LatchAndNetworkFillTogether(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	gates, err := reopened.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "core", gates["storage_mode"])
	require.Equal(t, "preprod", gates["network"])
	legacy, err := reopened.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.NotNil(t, legacy)
	require.Equal(t, "preprod", legacy.Network)
	require.NoError(t, closeTestDatabase(reopened))
}

// TestPhase1SkipsPartialConfigWithoutTripping guards the regression found
// while auditing mithril/sync.go and database/lifecycle/restore.go: both
// reopen an existing database with only DataDir/Logger/StorageMode/Network
// set, since their config types (mithril.SyncConfig, the restore Manifest)
// carry nothing else. A database first opened with a fuller config must
// still be reopenable that way -- every gate the partial reopen cannot
// supply (NetworkMagic, BlobPlugin, MetadataPlugin, all opt-in-absent) or
// cannot express as "unknown" via its own zero value (StartEra, whose
// FrozenFillOnce class treats an empty configured value as "not known on
// this path") must be skipped rather than compared, not silently rejected.
// This is also why no bool-derived gate -- the two validation taints, and
// history_expiry_active -- lives in phase1GateValues: a bool has no such
// "unknown" state, so they all belong to phase 2 instead (see
// TestPhase1SkipsHistoryExpiryGateOnPartialReopen for the regression this
// specifically guards for history_expiry_active).
func TestPhase1SkipsPartialConfigWithoutTripping(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:              dataDir,
		StorageMode:          "core",
		Network:              "preprod",
		NetworkMagic:         1,
		StartEra:             "dijkstra",
		StrictUtxoValidation: true,
		BlobPlugin:           "badger",
		MetadataPlugin:       "sqlite",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(reopened))
}

// TestPhase1SkipsHistoryExpiryGateOnPartialReopen pins the regression found
// while auditing mithril/sync.go and database/lifecycle/restore.go for
// history_expiry_active specifically: database.Config has no
// HistoryExpiryActive field (its LatchBool gate is phase 2's
// responsibility, written by EnforceNodeSettings once a full node has
// started with expiry on), so a database that persisted
// history_expiry_active = "on" from an earlier full-config open must still
// be reopenable through a partial Config carrying only
// DataDir/Logger/StorageMode/Network -- the shape mithril/sync.go:1200 and
// database/lifecycle/restore.go:609 use. Before the fix, computing "off"
// from the field's zero value on that reopen would trip LatchBool's
// "cannot be turned off once enabled" mismatch, which is exactly the
// regression this guards.
func TestPhase1SkipsHistoryExpiryGateOnPartialReopen(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	// Simulate a database that previously ran with history expiry enabled:
	// there is no phase-1 Config field to drive this through, so persist the
	// gate directly the way phase 2's write path eventually will.
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		nodesettings.Values{"history_expiry_active": nodesettings.LatchOn},
		0,
		0,
	))
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(reopened))
}

// openForRecoveryTest opens a database directly through New, the same way
// node.go does, rather than through newTestDatabase: newTestDatabase
// discards the returned *Database on any error, but node.go's
// dbNeedsRecovery path -- and this test -- specifically needs the
// *Database New still returns alongside a CommitTimestampError, since a
// database on that path is available for recovery rather than closed. See
// newTestDatabaseWithHost's doc comment (tests_test.go) for the
// keepOnError contract this delegates to.
func openForRecoveryTest(
	tb testing.TB,
	config *Config,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHost(tb, config, true)
}

// TestPhase1SkippedOnRecoveryPathButCatchesMismatchOnceReCheckable pins the
// P1 fix: database.New returns a CommitTimestampError before it ever calls
// CheckNodeSettings (checkCommitTimestamp runs first in init and returns
// immediately on failure), so phase 1 -- and the gates only it validates,
// like blob_plugin -- goes completely unchecked for that entire open. This
// reproduces that gap directly (a commit-timestamp mismatch combined with a
// blob_plugin change reports only CommitTimestampError, never the gate
// mismatch), then proves the fix: calling the now-exported
// CheckNodeSettings on the *Database New still returned -- exactly what
// node.go's dbNeedsRecovery path does once RecoverCommitTimestampConflict
// succeeds -- does catch it.
func TestPhase1SkippedOnRecoveryPathButCatchesMismatchOnceReCheckable(
	t *testing.T,
) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "badger", gates["blob_plugin"])

	// Induce a commit-timestamp mismatch the same way
	// TestCheckCommitTimestamp_MetadataOnly does: give metadata a commit
	// timestamp with none on the blob side.
	metaTxn := db.Metadata().Transaction(t.Context())
	require.NoError(t, db.Metadata().SetCommitTimestamp(123456789, metaTxn))
	require.NoError(t, metaTxn.Commit())
	require.NoError(t, closeTestDatabase(db))

	// Reopen with a changed blob_plugin. On a healthy reopen this alone
	// would be a NodeSettingsError from phase 1. Here, the commit-timestamp
	// mismatch above makes checkCommitTimestamp fail first, and init
	// returns immediately without ever reaching CheckNodeSettings.
	reopened, reopenErr := openForRecoveryTest(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "gcs",
		MetadataPlugin: "sqlite",
	})
	require.Error(t, reopenErr)
	var cte CommitTimestampError
	require.ErrorAs(
		t,
		reopenErr,
		&cte,
		"phase 1 must not run before the commit-timestamp conflict is "+
			"resolved, so the error on this open must be exactly "+
			"CommitTimestampError, not a NodeSettingsError from the "+
			"blob_plugin change",
	)
	var settingsErr NodeSettingsError
	require.False(
		t,
		errors.As(reopenErr, &settingsErr),
		"the blob_plugin mismatch must not have been reported yet -- "+
			"phase 1 has not run on this open at all",
	)
	require.NotNil(
		t,
		reopened,
		"New must still return the *Database on a CommitTimestampError so "+
			"the caller can recover it, per node.go's dbNeedsRecovery path",
	)

	// This is the fix: node.go calls CheckNodeSettings explicitly once
	// RecoverCommitTimestampConflict succeeds. Simulate that here directly
	// against the *Database New returned above, without needing a real
	// ledgerState-driven recovery run (recovery repairs the commit
	// timestamp, an orthogonal concern from the blob_plugin gate this
	// checks).
	checkErr := reopened.CheckNodeSettings(context.Background())
	require.True(
		t,
		errors.As(checkErr, &settingsErr),
		"re-invoking CheckNodeSettings after recovery must catch the "+
			"blob_plugin change phase 1 never got to see: got %v",
		checkErr,
	)
	require.Contains(t, settingsErr.Error(), "blob plugin")
}

// TestPhase1ConcurrentFirstOpenOneWinnerOneMismatch pins Finding 2's fix: a
// gate written for the first time ever can race against a second opener
// doing the same first-ever write. Two *Database instances race to open
// against the SAME metadata directory concurrently -- each with its own
// separate blob store directory, so badger's exclusive per-directory lock
// (which rules this race out for a real shared node) never enters into it,
// isolating the metadata-side race this fix targets -- with different
// NetworkMagic. Before the fix, node_settings_gate's plain upsert let
// whichever opener wrote last silently overwrite the other with no record
// a collision happened. After it, exactly one opener may win the first-ever
// write; the other must observe the winner's value and fail loudly on its
// own now-conflicting configuration, never silently adopt or overwrite it.
func TestPhase1ConcurrentFirstOpenOneWinnerOneMismatch(t *testing.T) {
	t.Parallel()

	for i := range 5 {
		t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
			metaDir := t.TempDir()
			blobDirA := t.TempDir()
			blobDirB := t.TempDir()

			var wg sync.WaitGroup
			var errA, errB error
			wg.Add(2)
			go func() {
				defer wg.Done()
				_, errA = newTestDatabaseAt(t, metaDir, blobDirA, &Config{
					DataDir:      metaDir,
					StorageMode:  "core",
					Network:      "preprod",
					NetworkMagic: 1,
				})
			}()
			go func() {
				defer wg.Done()
				_, errB = newTestDatabaseAt(t, metaDir, blobDirB, &Config{
					DataDir:      metaDir,
					StorageMode:  "core",
					Network:      "preprod",
					NetworkMagic: 2,
				})
			}()
			wg.Wait()

			// Exactly one opener must win: the other's differing
			// NetworkMagic must be rejected as a mismatch against whatever
			// the winner actually persisted, never silently accepted or
			// silently overwritten.
			require.True(
				t,
				(errA == nil) != (errB == nil),
				"exactly one concurrent first-open must succeed: errA=%v errB=%v",
				errA,
				errB,
			)
			var settingsErr NodeSettingsError
			if errA != nil {
				require.True(t, errors.As(errA, &settingsErr))
				require.Contains(t, settingsErr.Error(), "network magic")
			} else {
				require.True(t, errors.As(errB, &settingsErr))
				require.Contains(t, settingsErr.Error(), "network magic")
			}
		})
	}
}

// A proposal for epoch 0, made during epoch 0 before its slot of no return, is
// a current proposal the reference adopts at the boundary into epoch 1. Both
// transaction ingestion paths must store it.
func TestSetTransactionStoresEpochZeroProposals(t *testing.T) {
	t.Parallel()
	updateCbor, err := cbor.Encode(map[uint64]any{0: 200})
	require.NoError(t, err)
	var update shelley.ShelleyProtocolParameterUpdate
	_, err = cbor.Decode(updateCbor, &update)
	require.NoError(t, err)
	genesis := lcommon.Blake2b224{0x42}
	updates := map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate{
		genesis: update,
	}
	requireStored := func(t *testing.T, db *Database) {
		t.Helper()
		rows, err := db.Metadata().GetPParamUpdates(0, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, genesis.Bytes(), rows[0].GenesisHash)
		require.Equal(t, uint64(0), rows[0].Epoch)
		require.Equal(t, update.Cbor(), rows[0].Cbor)
	}

	t.Run("block", func(t *testing.T) {
		t.Parallel()
		db := openTestDB(t)
		candidate := findGapRollbackCandidateWithoutCertificates(t)
		for _, block := range candidate.producerBlocks {
			storeBlockOffsetsOnly(t, db, block)
		}
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
		require.True(t, candidate.consumerTx.IsValid())
		require.NoError(t, db.SetTransaction(
			context.Background(),
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			0,
			updates,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		))
		requireStored(t, db)
	})

	t.Run("batch", func(t *testing.T) {
		t.Parallel()
		db := openTestDB(t)
		candidate := findBatchedCrossBlockSpendCandidate(t)
		stagedProducer(t, db, candidate)
		require.True(t, candidate.producerTx.IsValid())
		acc := db.NewBatchAccumulator()
		txn := db.Transaction(context.Background(), true)
		defer txn.Release()
		defer txn.Rollback() //nolint:errcheck
		require.NoError(
			t,
			db.SetTransactionBatchedWithOpts(
				context.Background(),
				candidate.producerTx,
				candidate.producerPoint,
				candidate.producerIdx,
				0,
				updates,
				nil,
				mustBlockOffsets(t, candidate.producerBlock),
				acc,
				txn,
				BatchedTxIngestOpts{},
			),
		)
		require.NoError(t, db.FlushBatch(acc, txn))
		require.NoError(t, txn.Commit())
		requireStored(t, db)
	})
}

// rawSQLiteMetadataFixture is intentionally test-only. Production callers use
// the metadata contract; tests that must seed impossible/interrupted states
// can inspect the database without requiring a DB() escape hatch on Store.
func rawSQLiteMetadataFixture(
	t *testing.T,
	db *Database,
) *sql.DB {
	t.Helper()
	raw, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(db.DataDir(), "metadata.sqlite")+
			"?_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)"+
			"&_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, raw.Close())
	})
	return raw
}

func newSettingsTestDB(
	t *testing.T,
	dataDir, storageMode, network string,
) (*Database, error) {
	t.Helper()
	return newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: storageMode,
		Network:     network,
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
}

func TestNodeSettingsPersistence(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	// First open: persists settings
	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "preview", s.Network)
	require.NoError(t, closeTestDatabase(db))

	// Reopen with same settings: succeeds
	db, err = newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))
}

func TestNodeSettingsRejectStorageModeChange(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	// Change storage mode → error
	db, err = newSettingsTestDB(t, dataDir, "api", "preview")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 1)
	require.Contains(t, nsErr.Mismatches[0], "storage mode")
	if db != nil {
		require.NoError(t, closeTestDatabase(db))
	}
}

func TestNodeSettingsRejectNetworkChange(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	// Change network → error
	db, err = newSettingsTestDB(t, dataDir, "core", "mainnet")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 1)
	require.Contains(t, nsErr.Mismatches[0], "network")
	if db != nil {
		require.NoError(t, closeTestDatabase(db))
	}
}

func TestNodeSettingsAllowOpenWithoutConfiguredNetwork(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	db, err = newSettingsTestDB(t, dataDir, "core", "")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))
}

func TestNodeSettingsAllowDeferredNetworkInitialization(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "")
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "", s.Network)
	require.NoError(t, closeTestDatabase(db))

	db, err = newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)

	s, err = db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "preview", s.Network)
	require.NoError(t, closeTestDatabase(db))
}

func TestNodeSettingsRejectStorageModeChangeWhenNetworkUnset(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	db, err = newSettingsTestDB(t, dataDir, "api", "")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 1)
	require.Contains(t, nsErr.Mismatches[0], "storage mode")
	if db != nil {
		require.NoError(t, closeTestDatabase(db))
	}
}

func TestNodeSettingsRejectBothChanged(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	// Change both → error with 2 mismatches
	db, err = newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 2)
	if db != nil {
		require.NoError(t, closeTestDatabase(db))
	}
}

func TestNodeSettingsAPIMode(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	// First open with "api" + "mainnet"
	db, err := newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "api", s.StorageMode)
	require.Equal(t, "mainnet", s.Network)
	require.NoError(t, closeTestDatabase(db))

	// Reopen: succeeds
	db, err = newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	// Downgrade to core is a permitted one-way latch
	db, err = newSettingsTestDB(t, dataDir, "core", "mainnet")
	require.NoError(t, err)
	require.Equal(t, "core", db.StorageMode())
	require.NoError(t, closeTestDatabase(db))
}

func TestNodeSettingsMetadataSetDoesNotOverwrite(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)

	err = db.Metadata().SetNodeSettings(&types.NodeSettings{
		StorageMode: "api",
		Network:     "mainnet",
	})
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "preview", s.Network)
	require.NoError(t, closeTestDatabase(db))

	db, err = newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	db, err = newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	if db != nil {
		require.NoError(t, closeTestDatabase(db))
	}
}

// TestListSyncStateKeysByPrefix covers the byte-prefix scan used to repopulate
// the deferred-header retention set after a restart. The match
// must be an exact BYTE prefix on every backend, so this asserts cases a
// collation-sensitive SQL range or LIKE could get wrong: uppercase/mixed-case
// variants a case-insensitive column collation would fold in, a sibling prefix
// one byte away, and a non-ASCII neighbor a synthesized range upper bound could
// mishandle. The prefix also contains a LIKE wildcard ('_') to prove no
// wildcard escaping is needed.
func TestListSyncStateKeysByPrefix(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	const prefix = "deferred_header_validation:"
	want := []string{
		prefix + "10:aa",
		prefix + "13:bb",
		prefix + "21:cc",
	}
	for _, k := range want {
		require.NoError(t, db.SetSyncState(k, "true", nil))
	}
	// Decoys that must NOT match under an exact byte-prefix filter:
	decoys := []string{
		// Shorter than the prefix.
		"deferred_header_validation",
		// A sibling prefix differing only in the last byte.
		"deferred_header_validatioo:zz",
		// Unrelated key.
		"other_key",
		// Uppercased prefix: a case-INSENSITIVE column collation on
		// MySQL/Postgres would wrongly fold this into the match; a byte prefix
		// must exclude it.
		"DEFERRED_HEADER_VALIDATION:99",
		// Mixed case variant, same hazard.
		"Deferred_Header_Validation:88",
		// A non-ASCII key adjacent in Unicode: proves the match does not depend
		// on a synthesized range upper bound (which a non-ASCII prefix could
		// make invalid) and is not confused by locale collation ordering.
		"deferred_header_validationé:77",
	}
	for _, k := range decoys {
		require.NoError(t, db.SetSyncState(k, "x", nil))
	}

	got, err := db.ListSyncStateKeysByPrefix(prefix, nil)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		want,
		got,
		"only exact byte-prefix keys must match (no collation folding)",
	)

	// A non-ASCII prefix must scan safely and match exactly by bytes. The
	// "...é:77" decoy above shares this UTF-8 prefix, so both it and the key
	// added here must come back (and nothing ASCII-only).
	const utf8Prefix = "deferred_header_validationé:"
	require.NoError(t, db.SetSyncState(utf8Prefix+"aa", "true", nil))
	utf8Got, err := db.ListSyncStateKeysByPrefix(utf8Prefix, nil)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]string{utf8Prefix + "77", utf8Prefix + "aa"},
		utf8Got,
	)

	// Empty prefix returns everything.
	all, err := db.ListSyncStateKeysByPrefix("", nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(all), len(want)+len(decoys))

	// A prefix matching nothing returns empty, not an error.
	none, err := db.ListSyncStateKeysByPrefix("zzz_no_such_prefix:", nil)
	require.NoError(t, err)
	require.Empty(t, none)
}

var testDatabaseHosts sync.Map

func newTestDatabase(
	tb testing.TB,
	config *Config,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHost(tb, config, false)
}

// newTestDatabaseWithRunMode builds a test database whose blob store is
// resolved with the given run mode. It exists because the badger plugin
// switches block metadata to a compact binary encoding for run mode
// "serve" or "leios" with storage mode "core", and nothing else in these
// tests reaches that encoding.
func newTestDatabaseWithRunMode(
	tb testing.TB,
	config *Config,
	runMode string,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHostRunMode(tb, config, false, runMode)
}

// newTestDatabaseWithHost is the shared body behind newTestDatabase and
// openForRecoveryTest (database/tests_test.go): register the
// badger and sqlite providers, resolve the blob and metadata stores from
// config, and call New. The two callers differ only in keepOnError: New can
// return a non-nil *Database alongside an error on a CommitTimestampError,
// since that database is available for recovery rather than closed.
// newTestDatabase passes false -- none of its many callers need the failed
// handle, so it is closed (via a host stop) and discarded like any other
// open error. openForRecoveryTest passes true, matching node.go's
// dbNeedsRecovery path, which specifically needs the *Database New still
// returns alongside the error.
func newTestDatabaseWithHost(
	tb testing.TB,
	config *Config,
	keepOnError bool,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHostRunMode(tb, config, keepOnError, "")
}

func newTestDatabaseWithHostRunMode(
	tb testing.TB,
	config *Config,
	keepOnError bool,
	runMode string,
) (*Database, error) {
	tb.Helper()
	if config == nil {
		config = DefaultConfig
	}
	host := plugin.NewHost()
	if err := badger.RegisterProvider(host); err != nil {
		return nil, err
	}
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, err
	}
	blobStore, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", testutil.BadgerBlobConfig(),
		blob.ProviderDependencies{
			DataDir: config.DataDir, StorageMode: config.StorageMode,
			RunMode: runMode,
			Logger:  config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		return nil, err
	}
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{
			DataDir: config.DataDir, StorageMode: config.StorageMode,
			Logger: config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	db, dbErr := New(
		context.Background(),
		config,
		Stores{Blob: blobStore, Metadata: metadataStore},
	)
	if db == nil || (dbErr != nil && !keepOnError) {
		_ = host.Stop(context.Background())
		return nil, dbErr
	}
	testDatabaseHosts.Store(db, host)
	tb.Cleanup(func() {
		if closeErr := closeTestDatabase(db); closeErr != nil {
			tb.Errorf("close test database runtime: %v", closeErr)
		}
	})
	return db, dbErr
}

func closeTestDatabase(db *Database) error {
	if db == nil {
		return nil
	}
	err := db.Close()
	if hostValue, ok := testDatabaseHosts.LoadAndDelete(db); ok {
		err = errors.Join(
			err,
			hostValue.(*plugin.Host).Stop(context.Background()),
		)
	}
	return err
}

// erroringMetadata wraps a real MetadataStore and returns injectErr from
// SetTransaction and SetTransactionBatched. Every other method delegates
// to the embedded real store unchanged (via interface embedding), so the
// production paths' pre-metadata prerequisites (blob writes, offset
// lookups, consumed-input recovery, batch accumulator lifecycle) work
// exactly as they do at runtime — only the final metadata-write step is
// forced to fail with a known inner error.
//
// This is the harness that turns this test file into an integration test
// of the three real wrap sites, rather than a same-file duplication of
// the format strings.
type erroringMetadata struct {
	metadata.MetadataStore
	injectErr error
}

func (e *erroringMetadata) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
) error {
	return e.injectErr
}

func (e *erroringMetadata) SetTransactionLeiosClosure(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
) error {
	return e.injectErr
}

func (e *erroringMetadata) SetTransactionBatched(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	acc types.MetadataBatchAccumulator,
	txn types.Txn,
) error {
	return e.injectErr
}

// TestSetTransactionMetadataErrorWrap_ProductionPaths exercises the three
// public entry points whose error wraps this commit changed:
//
//   - database.SetTransactionBatched → wrap at batch.go
//     ("set transaction metadata for tx %s (batch idx %d, slot %d): %w")
//   - database.SetTransaction → wrap at transaction.go
//     ("set transaction metadata for tx %s (block idx %d, slot %d): %w")
//   - database.SetTransactionMetadataOnly → wrap at transaction.go
//     ("set transaction metadata only for tx %s (block idx %d, slot %d): %w")
//
// The metadata plugin is a real SQLite store from openTestDB, then wrapped
// with erroringMetadata so the final metadata write returns a known inner
// error. Everything upstream of the metadata call — blob offset writes,
// consumed-input recovery, batch accumulator lifecycle — runs against the
// real code as it does in production.
//
// If any production wrap loses the tx hash, index, slot, inner-error text,
// or the errors.Is chain, this test fails loudly. If a production message
// drifts (e.g. "batch idx" → "batch index"), this test fails loudly.
func TestSetTransactionMetadataErrorWrap_ProductionPaths(t *testing.T) {
	t.Parallel()

	// Inner error mimics the real failure that motivated the wrap.
	inner := errors.New(
		"pool reward account: pool cert reward_account: got 2 bytes, want 29",
	)

	db := openTestDB(t)
	// Swap in the erroring wrapper. Same-package access to the unexported
	// `metadata` field is intentional and follows the pattern used by
	// other database/*_test.go files (e.g. batch_test.go) that
	// prod internal state directly.
	db.metadata = &erroringMetadata{
		MetadataStore: db.metadata,
		injectErr:     inner,
	}

	candidate := findBatchedCrossBlockSpendCandidate(t)
	// %s renders Blake2b256 exactly as the production sites will
	// (all three wraps use `tx.Hash()` as a `%s` arg).
	txHashStr := fmt.Sprintf("%s", candidate.producerTx.Hash())
	require.GreaterOrEqual(
		t,
		len(txHashStr),
		8,
		"Blake2b256 %%s output too short to be a valid hash: %q",
		txHashStr,
	)
	idx := candidate.producerIdx
	slot := candidate.producerPoint.Slot
	idxStr := fmt.Sprint(idx)
	slotStr := fmt.Sprint(slot)

	// --- Path 1: batch (batch.go: SetTransactionBatched) ---
	// The batched path stages the producer's UTxOs so that a real accumulator
	// can be built and the consumed-input step passes. Only the terminal
	// metadata.SetTransactionBatched call is forced to fail.
	_ = stagedProducer(t, db, candidate)
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(context.Background(), true)
	batchErr := db.SetTransactionBatched(
		context.Background(),
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,   // updateEpoch
		nil, // pparamUpdates
		nil, // certDeposits
		mustBlockOffsets(t, candidate.producerBlock),
		acc,
		txn,
	)
	_ = txn.Rollback()
	txn.Release()
	assertProductionWrap(
		t,
		batchErr,
		inner,
		"batch idx",
		idxStr,
		slotStr,
		txHashStr,
	)

	// --- Path 2: block (transaction.go: SetTransaction) ---
	// Non-batched form uses the same setup, but the wrap-site prefix is
	// "block idx" instead of "batch idx".
	txn2 := db.Transaction(context.Background(), true)
	blockErr := db.SetTransaction(
		context.Background(),
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,   // updateEpoch
		nil, // pparamUpdates
		nil, // certDeposits
		mustBlockOffsets(t, candidate.producerBlock),
		txn2,
	)
	_ = txn2.Rollback()
	txn2.Release()
	assertProductionWrap(
		t,
		blockErr,
		inner,
		"block idx",
		idxStr,
		slotStr,
		txHashStr,
	)

	// --- Path 3: metadata-only (transaction.go: SetTransactionMetadataOnly) ---
	// Simpler path — no offsets or consumed-input recovery — but must still
	// produce the "metadata only" phrasing plus tx hash / block idx / slot.
	txn3 := db.Transaction(context.Background(), true)
	metaErr := db.SetTransactionMetadataOnly(
		context.Background(),
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		nil, // certDeposits
		txn3,
	)
	_ = txn3.Rollback()
	txn3.Release()
	require.Error(t, metaErr)
	assertProductionWrap(
		t,
		metaErr,
		inner,
		"block idx",
		idxStr,
		slotStr,
		txHashStr,
	)
	// The "only" wrap has a distinguishing marker in addition to the shared
	// tx/idx/slot fields — pin it too so the two block-idx sites can't
	// collapse into the same wording without failing this test.
	require.Contains(
		t,
		metaErr.Error(),
		"metadata only for tx",
		"metadata-only wrap must contain 'metadata only for tx' marker; got %q",
		metaErr.Error(),
	)
}

// assertProductionWrap validates a wrapped error from one of the three
// production wrap sites: (a) errors.Is unwrap chain preserved,
// (b) tx hash rendered via %s appears, (c) idx label ("batch idx" or
// "block idx") and its numeric value appear, (d) slot decimal appears,
// (e) inner error text preserved. It intentionally does NOT pin the
// entire format string so minor wording refinements (e.g. reordering)
// don't require churn — only field drift fails.
func assertProductionWrap(
	t *testing.T,
	wrapped, inner error,
	idxLabel, idxStr, slotStr, txHashStr string,
) {
	t.Helper()
	require.Error(
		t,
		wrapped,
		"expected non-nil error from production wrap site",
	)
	require.Truef(
		t,
		errors.Is(wrapped, inner),
		"wrap chain broken: errors.Is(wrapped, inner) == false; wrapped=%q",
		wrapped.Error(),
	)
	msg := wrapped.Error()
	require.Contains(
		t,
		msg,
		txHashStr,
		"wrap missing tx hash %q; got %q",
		txHashStr,
		msg,
	)
	require.Contains(
		t,
		msg,
		idxLabel,
		"wrap missing %q label; got %q",
		idxLabel,
		msg,
	)
	require.Contains(t, msg, idxLabel+" "+idxStr,
		"wrap missing %q with numeric value %q; got %q", idxLabel, idxStr, msg)
	require.Contains(t, msg, "slot "+slotStr,
		"wrap missing slot decimal %q; got %q", "slot "+slotStr, msg)
	require.Contains(t, msg, inner.Error(),
		"wrap dropped inner error text %q; got %q", inner.Error(), msg)
	// Also require the shared prefix to keep the two blocks-idx sites
	// grepable by operators; the exact phrase is the invariant.
	require.True(
		t,
		strings.Contains(msg, "set transaction metadata for tx ") ||
			strings.Contains(msg, "set transaction metadata only for tx "),
		"wrap missing 'set transaction metadata[ only] for tx ' prefix; got %q",
		msg,
	)
}

// collateralReturnAddress is an arbitrary mainnet payment address; the
// collateral-return fixtures only need a decodable one.
const collateralReturnAddress = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"

// noOutputsTx is the legal zero-output transaction shape: a valid
// transaction that declares no outputs, so Produced() is empty too. On chain
// this is e.g. a stake registration that spends its whole input on the deposit
// plus the fee and returns no change.
type noOutputsTx struct {
	lcommon.Transaction
}

func (t noOutputsTx) Outputs() []lcommon.TransactionOutput { return nil }

func (t noOutputsTx) Produced() []lcommon.Utxo { return nil }

// droppedOutputsTx is the unexpected shape: a valid transaction that declares
// outputs but produces no UTxOs. Produced() maps one-to-one onto Outputs() for
// a valid transaction, so this cannot occur on chain -- it would mean outputs
// were dropped before storage.
type droppedOutputsTx struct {
	lcommon.Transaction
}

func (t droppedOutputsTx) Produced() []lcommon.Utxo { return nil }

// invalidTx is the phase-2 failure shape, where Produced() is the collateral
// return alone. A nil collateralReturn is the legal zero-output case; a
// non-nil one that still produces nothing has lost the collateral return.
type invalidTx struct {
	lcommon.Transaction
	collateralReturn lcommon.TransactionOutput
}

func (t invalidTx) IsValid() bool { return false }

func (t invalidTx) CollateralReturn() lcommon.TransactionOutput {
	return t.collateralReturn
}

func (t invalidTx) Produced() []lcommon.Utxo { return nil }

// collateralReturnTx is the shape a real phase-2 failure takes in Babbage and
// later: invalid, a non-nil collateral return, and a Produced() that carries
// that return at index len(Outputs()). It is the true negative for the
// collateral branch of the warning -- the only shape where a non-nil
// CollateralReturn() coexists with a correctly stored UTxO, so it is the shape
// that would expose the branch firing on declaration alone rather than on loss.
type collateralReturnTx struct {
	lcommon.Transaction
	collateralReturn lcommon.TransactionOutput
}

func (t collateralReturnTx) IsValid() bool { return false }

func (t collateralReturnTx) CollateralReturn() lcommon.TransactionOutput {
	return t.collateralReturn
}

func (t collateralReturnTx) Produced() []lcommon.Utxo {
	return []lcommon.Utxo{
		{
			Id: shelley.NewShelleyTransactionInput(
				t.Hash().String(),
				len(t.Outputs()),
			),
			Output: t.collateralReturn,
		},
	}
}

// TestSetTransactionZeroProducedOutputsLogging covers the log emitted when a
// transaction stores no UTxOs. A zero-output transaction is a legal shape and
// must not warn; only losing declared outputs is worth an operator's
// attention.
func TestSetTransactionZeroProducedOutputsLogging(t *testing.T) {
	t.Parallel()

	candidate := findGapConsumeCandidateWithoutCertificates(t)

	// newStagedDB stages the consumer's producers so the consumed inputs
	// resolve, leaving the produced-side logging as the only thing under test.
	newStagedDB := func(t *testing.T, logs *bytes.Buffer) *Database {
		t.Helper()
		db, err := newTestDatabase(t, &Config{
			DataDir: t.TempDir(),
			Logger: slog.New(slog.NewJSONHandler(
				logs,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			)),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		for _, p := range candidate.producers {
			storeBlockOffsetsOnly(t, db, p.block)
			metaTxn := db.MetadataTxn(context.Background(), true)
			producer := p
			require.NoError(t, metaTxn.Do(func(txn *Txn) error {
				return db.Metadata().SetGapBlockTransaction(
					producer.tx,
					producer.point,
					0,
					nil,
					txn.Metadata(),
				)
			}))
			metaTxn.Release()
		}
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
		return db
	}

	// setTx discards everything newStagedDB logged before calling the function
	// under test, so an assertion sees only that call's output. Staging writes
	// blocks and producer transactions, and a warning from that setup would
	// otherwise decide a "does not warn" subtest.
	setTx := func(
		t *testing.T,
		db *Database,
		logs *bytes.Buffer,
		tx lcommon.Transaction,
		withOffsets ...func(*BlockIngestionResult),
	) string {
		t.Helper()
		offsets := mustBlockOffsets(t, candidate.consumerBlock)
		for _, fn := range withOffsets {
			fn(offsets)
		}
		logs.Reset()
		require.NoError(t, db.SetTransactionWithOpts(
			context.Background(),
			tx,
			candidate.consumerPoint,
			0,
			0,
			nil,
			nil,
			offsets,
			nil,
			BatchedTxIngestOpts{},
		))
		return logs.String()
	}

	t.Run("zero-output transaction does not warn", func(t *testing.T) {
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(
			t, db, &logs,
			noOutputsTx{Transaction: candidate.consumerTx},
		)
		require.NotContains(t, out, `"level":"WARN"`)
	})

	t.Run("transaction with outputs does not warn", func(t *testing.T) {
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(t, db, &logs, candidate.consumerTx)
		require.NotContains(t, out, `"level":"WARN"`)
	})

	t.Run("dropped outputs warn", func(t *testing.T) {
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(
			t, db, &logs,
			droppedOutputsTx{Transaction: candidate.consumerTx},
		)
		require.Contains(t, out, `"level":"WARN"`)
		require.Contains(
			t,
			out,
			"valid transaction produced no UTxOs despite declaring outputs",
		)
		// The count names what was dropped, so it must be the declared
		// outputs rather than the (empty) produced set.
		require.Contains(t, out, fmt.Sprintf(
			`"outputs":%d`,
			len(candidate.consumerTx.Outputs()),
		))
	})

	t.Run(
		"invalid transaction without collateral return does not warn",
		func(t *testing.T) {
			var logs bytes.Buffer
			db := newStagedDB(t, &logs)
			out := setTx(
				t, db, &logs,
				invalidTx{Transaction: candidate.consumerTx},
			)
			require.NotContains(t, out, `"level":"WARN"`)
		},
	)

	t.Run("dropped collateral return warns", func(t *testing.T) {
		const collateralLovelace = 1_000_000
		collateralReturn, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(collateralReturnAddress).
			WithLovelace(collateralLovelace).
			Build()
		require.NoError(t, err)
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(t, db, &logs, invalidTx{
			Transaction:      candidate.consumerTx,
			collateralReturn: collateralReturn,
		})
		require.Contains(t, out, `"level":"WARN"`)
		// The dropped declaration here is the collateral return, so the
		// message and the attribute must name it. Reporting the transaction's
		// outputs instead would point an operator at a field that is not what
		// went missing.
		require.Contains(
			t,
			out,
			"invalid transaction produced no UTxOs despite declaring "+
				"a collateral return",
		)
		require.Contains(t, out, fmt.Sprintf(
			`"collateralReturnLovelace":"%d"`,
			collateralLovelace,
		))
		require.NotContains(t, out, `"outputs":`)
		require.NotContains(t, out, "despite declaring outputs")
	})

	t.Run(
		"invalid transaction keeping its collateral return does not warn",
		func(t *testing.T) {
			collateralReturn, err := mockledger.NewTransactionOutputBuilder().
				WithAddress(collateralReturnAddress).
				WithLovelace(1_000_000).
				Build()
			require.NoError(t, err)
			var logs bytes.Buffer
			db := newStagedDB(t, &logs)
			var txHash [32]byte
			copy(txHash[:], ledgerHashBytes(candidate.consumerTx.Hash()))
			collateralIdx := uint32(len(candidate.consumerTx.Outputs()))
			out := setTx(
				t, db, &logs,
				collateralReturnTx{
					Transaction:      candidate.consumerTx,
					collateralReturn: collateralReturn,
				},
				func(offsets *BlockIngestionResult) {
					// The indexer only emits offsets for the outputs the
					// transaction declares, so the collateral return's index
					// has none and SetTransactionWithOpts would fail before
					// reaching the log. Nothing on this path decodes the
					// offset, so output 0's span stands in for it.
					offsets.UtxoOffsets[UtxoRef{
						TxId:      txHash,
						OutputIdx: collateralIdx,
					}] = offsets.UtxoOffsets[UtxoRef{
						TxId:      txHash,
						OutputIdx: 0,
					}]
				},
			)
			require.NotContains(t, out, `"level":"WARN"`)
		},
	)
}
