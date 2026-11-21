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
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/stretchr/testify/require"
)

const (
	noFamilyRows   = "reads no row of a drainable family"
	databaseServes = "the database answers a drained family from its source, or has nothing to do"
	writeSkips     = "a write that skips the rows of a drained family"
	loaderOnly     = "read only while the set loads or verifies, before its family drains"
	metadataOnly   = "reached only by replay or recovery that runs on the metadata store alone"
)

// stagingPromoted names every ledgerdb.LedgerDB method stagingLedgerDB does
// not override, so the metadata store's implementation answers it, and why
// that is safe once a ledger-state family is drained: the metadata store holds
// no rows of a drained family, and a method that read them would refuse or
// answer empty. A new LedgerDB method, or one removed from stagingLedgerDB,
// must be placed here deliberately.
var stagingPromoted = map[string]string{
	// The transaction and lifecycle methods of the interface itself.
	"Begin": noFamilyRows, "BeginLedgerOnly": noFamilyRows, "Run": noFamilyRows,
	"StorageMode": noFamilyRows, "Logger": noFamilyRows,
	"BeginDestructiveTransitionContext":       noFamilyRows,
	"BlockAtOrAfterIndex":                     noFamilyRows,
	"BlockBatchTransactionContext":            noFamilyRows,
	"BlockBatchTxnContext":                    noFamilyRows,
	"BlockByIndex":                            noFamilyRows,
	"BlockCreate":                             noFamilyRows,
	"BlockPointByIndex":                       noFamilyRows,
	"CountUtxosConsumedUpTo":                  noFamilyRows,
	"DeleteDeferredHeaderMarker":              noFamilyRows,
	"EnableBatchUtxoOverlay":                  noFamilyRows,
	"EnsureBlockInventory":                    noFamilyRows,
	"FirstBlockAtOrAfterSlot":                 noFamilyRows,
	"GetDeferredHeaderMarkerValue":            noFamilyRows,
	"GetLeiosEBManifest":                      noFamilyRows,
	"GetLeiosEBTxs":                           noFamilyRows,
	"GetLiveUtxoLovelace":                     noFamilyRows,
	"GetTip":                                  noFamilyRows,
	"GetTransactionByHash":                    noFamilyRows,
	"GetTransactionsByHashes":                 noFamilyRows,
	"HasAnyGenesisCbor":                       noFamilyRows,
	"HasGenesisCbor":                          noFamilyRows,
	"IterateLiveUtxoRefs":                     noFamilyRows,
	"IterateLiveUtxos":                        noFamilyRows,
	"IterateUtxoRefsAsOf":                     noFamilyRows,
	"IterateUtxoRefsAsOfPaged":                noFamilyRows,
	"ListDeferredHeaderMarkerValues":          noFamilyRows,
	"MaxLeiosEBSlot":                          noFamilyRows,
	"MaxLeiosEBSlotContext":                   noFamilyRows,
	"NewBatchAccumulator":                     noFamilyRows,
	"NewLeiosClosureBatchAccumulator":         noFamilyRows,
	"NewValidatedBlockBatchAccumulator":       noFamilyRows,
	"OldestRetainedSlot":                      noFamilyRows,
	"PersistTransactionOffsets":               noFamilyRows,
	"ResolveTxCbor":                           noFamilyRows,
	"ResolveUtxoCborWithRecovery":             noFamilyRows,
	"SetDeferredHeaderMarkerWithValue":        noFamilyRows,
	"SetGenesisCbor":                          noFamilyRows,
	"SetLeiosEB":                              noFamilyRows,
	"SetTip":                                  noFamilyRows,
	"SumTransactionFeesInSlotRange":           noFamilyRows,
	"UtxoByRef":                               noFamilyRows,
	"UtxoByRefIncludingSpent":                 noFamilyRows,
	"UtxoExists":                              noFamilyRows,
	"UtxosByAddress":                          noFamilyRows,
	"UtxosByAddressAsOf":                      noFamilyRows,
	"UtxosByRefs":                             noFamilyRows,
	"UtxosByRefsAsOf":                         noFamilyRows,
	"UtxosByRefsBounded":                      noFamilyRows,
	"UtxosDeleteSpentInBatch":                 noFamilyRows,
	"UtxosDropSpentInBatchBlobs":              noFamilyRows,
	"GetPoolStakeSnapshotsByEpochBounded":     noFamilyRows,
	"GetSyncStateBounded":                     noFamilyRows,
	"ListSyncStateKeysByPrefix":               noFamilyRows,
	"AccountsWitnessedAfterSlot":              databaseServes,
	"AccountInactivityActivationMembership":   databaseServes,
	"ConsumedUtxoPruneFloor":                  databaseServes,
	"ImportedLedgerStateBoundarySlotStrict":   databaseServes,
	"UtxosDeleteConsumed":                     databaseServes,
	"GetPParamsRows":                          databaseServes,
	"GetRewardLikelihoodStateAtSlot":          noFamilyRows,
	"GetRewardLikelihoodStateBeforeEpoch":     noFamilyRows,
	"GetRewardPoolLikelihoods":                noFamilyRows,
	"SaveRewardLikelihoodState":               noFamilyRows,
	"DeleteRewardAccountOutputsBeforeEpoch":   writeSkips,
	"FlushBatch":                              writeSkips,
	"FlushBatchStakeDeltas":                   writeSkips,
	"MarkUtxosDeletedAtSlot":                  writeSkips,
	"PrepareLeiosClosureBatch":                writeSkips,
	"RebuildRewardLiveStake":                  writeSkips,
	"RefreshRewardLiveStakeRefs":              writeSkips,
	"SetGenesisTransaction":                   writeSkips,
	"TruncateAfterSlot":                       writeSkips,
	"IterateAccountImportBaselines":           loaderOnly,
	"IterateAccountWitnessEffectiveSlots":     loaderOnly,
	"IterateAccountWithdrawalEffectiveSlots":  loaderOnly,
	"IterateAccountWitnessSlots":              loaderOnly,
	"IterateAccounts":                         loaderOnly,
	"IterateDreps":                            loaderOnly,
	"IterateLeiosClosureUtxoEffects":          loaderOnly,
	"IterateLiveDrepDelegators":               loaderOnly,
	"IterateStakeDelegationEvents":            loaderOnly,
	"IterateUnfoldedRewardAccountOutputs":     loaderOnly,
	"LatestPoolOpCertCounters":                loaderOnly,
	"ListPParamUpdates":                       loaderOnly,
	"GetImportedPoolBlockCounts":              loaderOnly,
	"MergeImportedPoolBlockCounts":            loaderOnly,
	"CountPoolBlocksInSlotRange":              loaderOnly,
	"GetPoolBlockIssuersInSlotRange":          loaderOnly,
	"GetExpiredDReps":                         metadataOnly,
	"RecordDRepActivityEpoch":                 metadataOnly,
	"ReconcileAlonzoPParamsUnitAfterRecovery": metadataOnly,
}

func TestStagingLedgerDBPromotesOnlyMethodsThatNeedNoDrainedRows(t *testing.T) {
	t.Parallel()
	iface := reflect.TypeFor[ledgerdb.LedgerDB]()
	staging := reflect.TypeFor[stagingLedgerDB]()
	var promoted []string
	for method := range iface.Methods() {
		name := method.Name
		m, ok := staging.MethodByName(name)
		require.True(t, ok, "stagingLedgerDB lacks %s", name)
		fn := runtime.FuncForPC(m.Func.Pointer())
		// A method promoted from the embedded LedgerDB is a compiler-generated
		// wrapper, which has no source file.
		file, _ := fn.FileLine(fn.Entry())
		if strings.Contains(file, "autogenerated") {
			promoted = append(promoted, name)
		}
	}
	slices.Sort(promoted)
	var listed []string
	for name := range stagingPromoted {
		listed = append(listed, name)
	}
	slices.Sort(listed)
	require.Equal(t, listed, promoted,
		"stagingLedgerDB overrides a method listed as promoted, or promotes one that is not listed")
}
