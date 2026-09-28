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

package storagetest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// RunMetadataStoreConformance exercises the dialect-neutral contract
// documented on metadata.SettingsStore, metadata.TxnStore,
// metadata.SlotRangeStore, and metadata.GovernanceStore against newStore().
// newStore is called once; the returned store is reused across every
// subtest.
//
// The suite is deliberately scoped to that shared, capability-level surface
// rather than the ~150 domain methods MetadataStore composes on top of it:
// sqlite, mysql, and postgres are thin driver shims around the one shared
// database/plugin/metadata/sqlstore.Store implementation, so the domain
// methods have no per-dialect business logic to differentiate here --
// database/plugin/metadata/sqlstore/dialect_integration_test.go already
// exercises that shared implementation against real Postgres/MySQL.
func RunMetadataStoreConformance(
	t *testing.T,
	newStore func(t *testing.T) metadata.MetadataStore,
) {
	t.Helper()
	store := newStore(t)

	t.Run("CommitTimestampRoundTrip", func(t *testing.T) {
		txn := store.Transaction(t.Context())
		require.NoError(t, store.SetCommitTimestamp(555, txn))
		require.NoError(t, txn.Commit())

		got, err := store.GetCommitTimestamp()
		require.NoError(t, err)
		require.Equal(t, int64(555), got)
	})

	t.Run("NodeSettingsFirstWriteWins", func(t *testing.T) {
		require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
			StorageMode: types.StorageModeCore,
			Network:     "conformance-first",
		}))
		require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
			StorageMode: types.StorageModeCore,
			Network:     "conformance-second",
		}))

		got, err := store.GetNodeSettings()
		require.NoError(t, err)
		require.Equal(t, "conformance-first", got.Network)
	})

	t.Run("NodeSettingsGatesRoundTrip", func(t *testing.T) {
		name := conformanceGateName(t, "gate")
		gates := nodesettings.Values{name: "enabled"}
		require.NoError(t, store.SetNodeSettingsGates(gates, 3, 30))

		got, err := store.GetNodeSettingsGates()
		require.NoError(t, err)
		require.Equal(t, "enabled", got[name])
	})

	t.Run("SetNodeSettingsGatesOverwritesOnSecondCall", func(t *testing.T) {
		// Unlike SetNodeSettings (NodeSettingsFirstWriteWins above), gates
		// are documented to overwrite: "a later call overwrites an earlier
		// value for the same name" (metadata.SettingsStore doc comment).
		name := conformanceGateName(t, "update-gate")
		require.NoError(t, store.SetNodeSettingsGates(
			nodesettings.Values{name: "first-value"},
			1,
			10,
		))
		require.NoError(t, store.SetNodeSettingsGates(
			nodesettings.Values{name: "second-value"},
			2,
			20,
		))

		got, err := store.GetNodeSettingsGates()
		require.NoError(t, err)
		require.Equal(t, "second-value", got[name])
	})

	t.Run("SetNodeSettingsGatesNilIsNoop", func(t *testing.T) {
		require.NoError(t, store.SetNodeSettingsGates(nil, 0, 0))
	})

	t.Run("InsertNodeSettingsGateIfAbsentIsFirstWriteWins", func(t *testing.T) {
		name := conformanceGateName(t, "insert-if-absent")
		const writers = 8
		var wg sync.WaitGroup
		inserted := make([]bool, writers)
		errs := make([]error, writers)
		for i := range writers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ok, err := store.InsertNodeSettingsGateIfAbsent(
					name,
					fmt.Sprintf("value-%d", i),
					1,
					10,
				)
				inserted[i], errs[i] = ok, err
			}(i)
		}
		wg.Wait()

		winners := 0
		for i, err := range errs {
			require.NoErrorf(t, err, "writer %d", i)
			if inserted[i] {
				winners++
			}
		}
		require.Equal(t, 1, winners)

		got, err := store.GetNodeSettingsGates()
		require.NoError(t, err)
		require.Contains(t, got, name)
	})

	t.Run("TransactionCommitPersists", func(t *testing.T) {
		txn := store.Transaction(t.Context())
		require.NoError(t, store.SetCommitTimestamp(777, txn))
		require.NoError(t, txn.Commit())

		got, err := store.GetCommitTimestamp()
		require.NoError(t, err)
		require.Equal(t, int64(777), got)
	})

	t.Run("TransactionRollbackDiscardsWrites", func(t *testing.T) {
		baseline, err := store.GetCommitTimestamp()
		require.NoError(t, err)

		txn := store.Transaction(t.Context())
		require.NoError(t, store.SetCommitTimestamp(baseline+1, txn))
		require.NoError(t, txn.Rollback())

		got, err := store.GetCommitTimestamp()
		require.NoError(t, err)
		require.Equal(t, baseline, got)
	})

	t.Run("ReadTransactionSucceeds", func(t *testing.T) {
		txn := store.ReadTransaction(t.Context())
		require.NoError(t, txn.Rollback())
	})

	// SlotRangeStore is not part of MetadataStore -- it is a narrower
	// capability that database/plugin/metadata/sqlstore.Store additionally
	// implements for API adapters. Check it opportunistically so the suite
	// still covers it for the three shared-sqlstore-backed plugins without
	// forcing every metadata.MetadataStore implementation to satisfy it.
	if slotRangeStore, ok := store.(metadata.SlotRangeStore); ok {
		t.Run("SlotRangeStatsOnUnknownRange", func(t *testing.T) {
			// Read through a real ReadTransaction rather than nil: the
			// SettingsStore/TxnStore doc comments specifically call
			// out ReadTransaction as the read connection pool a caller
			// should use for exactly this kind of query, so the combination
			// -- not just each half in isolation -- needs to actually work.
			txn := store.ReadTransaction(t.Context())
			defer func() { require.NoError(t, txn.Rollback()) }()

			count, err := slotRangeStore.CountTransactionsInSlotRange(
				1_900_000_000,
				1_900_000_100,
				txn,
			)
			require.NoError(t, err)
			require.Equal(t, 0, count)

			stats, err := slotRangeStore.GetBlockSlotRangeStats(
				1_900_000_000,
				1_900_000_100,
				txn,
			)
			require.NoError(t, err)
			require.Equal(t, 0, stats.Count)
			require.Zero(t, stats.FirstSlot)
			require.Zero(t, stats.LastSlot)
		})
	}

	// Unlike the SlotRangeStore block above these need no runtime check:
	// MetadataStore composes each domain, so the conversions are static.
	// Naming the narrow handles is what makes the subtests below exercise
	// each domain the way a single-domain caller would, against a real
	// database, on every backend that runs this suite.
	var (
		certificateStore   metadata.CertificateStore   = store
		epochStore         metadata.EpochStore         = store
		governanceStore    metadata.GovernanceStore    = store
		stakeSnapshotStore metadata.StakeSnapshotStore = store
		transactionStore   metadata.TransactionStore   = store
		utxoStore          metadata.UtxoStore          = store
	)

	t.Run("GovernanceReadsOnEmptyState", func(t *testing.T) {
		// Reading through the narrowed handle, not through store: a
		// GovernanceStore-only caller has no ReadTransaction of its own,
		// so the combination it will actually use in production is a
		// transaction handed in from outside.
		txn := store.ReadTransaction(t.Context())
		defer func() { require.NoError(t, txn.Rollback()) }()

		proposals, err := governanceStore.GetActiveGovernanceProposals(
			1, txn,
		)
		require.NoError(t, err)
		require.Empty(t, proposals)

		ratified, err := governanceStore.GetRatifiedGovernanceProposals(txn)
		require.NoError(t, err)
		require.Empty(t, ratified)

		// Documented on GovernanceStore as (nil, nil) before any
		// UpdateCommittee has been enacted, rather than a not-found error.
		quorum, err := governanceStore.GetCommitteeQuorum(txn)
		require.NoError(t, err)
		require.Nil(t, quorum)

		count, err := governanceStore.GetCommitteeActiveCount(txn)
		require.NoError(t, err)
		require.Zero(t, count)

		dreps, err := governanceStore.GetActiveDreps(txn)
		require.NoError(t, err)
		require.Empty(t, dreps)
	})

	t.Run(
		"GovernanceRatificationHistoryRestoresRepeatedCycles",
		func(t *testing.T) {
			proposal := &models.GovernanceProposal{
				TxHash:        []byte(conformanceGateName(t, "proposal")),
				ActionIndex:   0,
				ActionType:    6,
				ProposedEpoch: 1,
				ExpiresEpoch:  100,
				AnchorURL:     "https://example.invalid/governance",
				AnchorHash:    []byte("conformance-governance-anchor"),
				ReturnAddress: []byte("conformance-return-address"),
				AddedSlot:     500,
			}
			setRatification := func(epoch, slot uint64) {
				proposal.RatifiedEpoch = &epoch
				proposal.RatifiedSlot = &slot
				write := store.Transaction(t.Context())
				defer func() { require.NoError(t, write.Rollback()) }()
				require.NoError(
					t,
					governanceStore.SetGovernanceProposal(proposal, write),
				)
				require.NoError(t, write.Commit())
			}
			clearRatification := func(slot uint64) {
				write := store.Transaction(t.Context())
				defer func() { require.NoError(t, write.Rollback()) }()
				require.NoError(
					t,
					governanceStore.ClearGovernanceProposalRatification(
						proposal.TxHash,
						proposal.ActionIndex,
						slot,
						write,
					),
				)
				require.NoError(t, write.Commit())
				proposal.RatifiedEpoch = nil
				proposal.RatifiedSlot = nil
			}
			rollback := func(slot uint64) {
				write := store.Transaction(t.Context())
				defer func() { require.NoError(t, write.Rollback()) }()
				require.NoError(
					t,
					governanceStore.DeleteGovernanceProposalsAfterSlot(
						slot,
						write,
					),
				)
				require.NoError(t, write.Commit())
			}
			readMarker := func() (*uint64, *uint64) {
				read := store.ReadTransaction(t.Context())
				defer func() { require.NoError(t, read.Rollback()) }()
				got, err := governanceStore.GetGovernanceProposal(
					proposal.TxHash,
					proposal.ActionIndex,
					read,
				)
				require.NoError(t, err)
				require.NotNil(t, got)
				return got.RatifiedEpoch, got.RatifiedSlot
			}

			setRatification(5, 550)
			clearRatification(600)
			setRatification(7, 700)
			clearRatification(800)
			epoch, slot := readMarker()
			require.Nil(t, epoch)
			require.Nil(t, slot)

			rollback(700)
			epoch, slot = readMarker()
			require.NotNil(t, epoch)
			require.NotNil(t, slot)
			require.Equal(t, uint64(7), *epoch)
			require.Equal(t, uint64(700), *slot)

			rollback(600)
			epoch, slot = readMarker()
			require.Nil(t, epoch)
			require.Nil(t, slot)

			rollback(599)
			epoch, slot = readMarker()
			require.NotNil(t, epoch)
			require.NotNil(t, slot)
			require.Equal(t, uint64(5), *epoch)
			require.Equal(t, uint64(550), *slot)

			rollback(549)
			epoch, slot = readMarker()
			require.Nil(t, epoch)
			require.Nil(t, slot)
		},
	)

	t.Run("ConstitutionRoundTripThroughNarrowStore", func(t *testing.T) {
		// A write as well as a read: a domain interface that can only be
		// read from would still compile at every call site the split
		// moved over, so the round trip is what proves the narrowing is
		// usable rather than merely type-correct.
		write := store.Transaction(t.Context())
		// Registered before the write, not after it: require.NoError
		// stops the subtest on failure, so a SetConstitution error would
		// otherwise leave this transaction holding its connection for the
		// rest of the suite. Rollback after a successful Commit is a
		// no-op -- sqlTxn returns nil once the transaction is finished.
		defer func() { require.NoError(t, write.Rollback()) }()
		require.NoError(t, governanceStore.SetConstitution(
			&models.Constitution{
				AnchorURL:  "https://example.invalid/constitution",
				AnchorHash: []byte("conformance-anchor-hash"),
				PolicyHash: []byte("conformance-policy-hash"),
				AddedSlot:  42,
			},
			write,
		))
		require.NoError(t, write.Commit())

		read := store.ReadTransaction(t.Context())
		defer func() { require.NoError(t, read.Rollback()) }()

		got, err := governanceStore.GetConstitution(read)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(
			t, "https://example.invalid/constitution", got.AnchorURL,
		)
		require.Equal(t, uint64(42), got.AddedSlot)
	})

	t.Run("DomainReadsOnEmptyState", func(t *testing.T) {
		// One cheap read per extracted domain, through that domain's
		// narrow handle. This is not coverage of the domains themselves --
		// the shared sqlstore implementation is tested elsewhere -- it is
		// evidence that each newly split interface is wired to a working
		// backend on this dialect, which a compile-time assertion cannot
		// show.
		txn := store.ReadTransaction(t.Context())
		defer func() { require.NoError(t, txn.Rollback()) }()

		utxo, err := utxoStore.GetUtxo(
			make([]byte, 32), 0, txn,
		)
		require.NoError(t, err)
		require.Nil(t, utxo)

		hashes, err := transactionStore.GetTransactionHashesAfterSlot(
			1_900_000_000, txn,
		)
		require.NoError(t, err)
		require.Empty(t, hashes)

		epochs, err := epochStore.GetEpochs(txn)
		require.NoError(t, err)
		require.Empty(t, epochs)

		snapshots, err := stakeSnapshotStore.GetPoolStakeSnapshotsByEpoch(
			1, models.PoolStakeSnapshotTypeMark, txn,
		)
		require.NoError(t, err)
		require.Empty(t, snapshots)

		mirs, err := certificateStore.GetMIRCertsInSlotRange(
			1_900_000_000, 1_900_000_100, txn,
		)
		require.NoError(t, err)
		require.Empty(t, mirs)
	})

	t.Run("OperationsCompleteWithinTimeout", func(t *testing.T) {
		// Not a benchmark: a generous bound that only catches a genuine
		// hang (a leaked lock, a stuck connection-pool wait, a network call
		// with no deadline) rather than measuring throughput.
		const bound = 10 * time.Second
		start := time.Now()

		txn := store.Transaction(t.Context())
		require.NoError(t, store.SetCommitTimestamp(1, txn))
		require.NoError(t, txn.Commit())
		_, err := store.GetCommitTimestamp()
		require.NoError(t, err)

		require.Less(
			t,
			time.Since(start),
			bound,
			"a commit+read pair took longer than %s; likely a hang rather "+
				"than a slow backend",
			bound,
		)
	})
}

// conformanceGateName derives a gate name unique to t's subtest name and
// label so concurrently running conformance suites never collide on the
// same persisted row.
func conformanceGateName(t *testing.T, label string) string {
	t.Helper()
	return "storagetest:" + t.Name() + ":" + label
}
