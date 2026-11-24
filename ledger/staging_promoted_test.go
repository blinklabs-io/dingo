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
	chainStore = "chain data the SQL chain store holds: blocks, tip, deferred " +
		"header markers, genesis CBOR, Leios endorser blocks, the Mithril " +
		"trust boundary, and the transactions over them"
	txIndex = "the transaction and UTxO index the SQL chain store holds, " +
		"and the batch plumbing that writes it"
	loaderOnly = "read only by the one-way load of a v0.94.2 store into the " +
		"LedgerDB set, or by its verification"
	notLedger = "never called through the ledger's LedgerDB; another " +
		"component calls it on its own adapter"
)

// stagingPromoted names every ledgerdb.LedgerDB method stagingLedgerDB does
// not override, so the SQL store answers it. Ledger state is LedgerDB's
// alone, so each one must be chain data, the transaction index, or a read
// of the v0.94.2 load. A new LedgerDB method, or one removed from
// stagingLedgerDB, must be placed here deliberately.
var stagingPromoted = map[string]string{
	"Begin":                                  chainStore,
	"BeginDestructiveTransitionContext":      chainStore,
	"BeginLedgerOnly":                        chainStore,
	"BeginStaged":                            chainStore,
	"BlockAtOrAfterIndex":                    chainStore,
	"BlockBatchTransactionContext":           chainStore,
	"BlockBatchTxnContext":                   chainStore,
	"BlockByIndex":                           chainStore,
	"BlockCreate":                            chainStore,
	"BlockPointByIndex":                      chainStore,
	"DeferredHeaderFloor":                    chainStore,
	"DeleteDeferredHeaderMarker":             chainStore,
	"EnsureBlockInventory":                   chainStore,
	"FirstBlockAtOrAfterSlot":                chainStore,
	"GetDeferredHeaderMarkerValue":           chainStore,
	"GetLeiosEBManifest":                     chainStore,
	"GetLeiosEBTxs":                          chainStore,
	"GetTip":                                 chainStore,
	"HasAnyGenesisCbor":                      chainStore,
	"HasGenesisCbor":                         chainStore,
	"ListDeferredHeaderMarkerValues":         chainStore,
	"Logger":                                 chainStore,
	"MaxLeiosEBSlot":                         chainStore,
	"MaxLeiosEBSlotContext":                  chainStore,
	"ImportedLedgerStateBoundaryHash":        chainStore,
	"ImportedLedgerStateBoundarySlotStrict":  chainStore,
	"OldestRetainedSlot":                     chainStore,
	"Run":                                    chainStore,
	"SetDeferredHeaderFloor":                 chainStore,
	"SetDeferredHeaderMarkerWithValue":       chainStore,
	"SetGenesisCbor":                         chainStore,
	"SetLeiosEB":                             chainStore,
	"SetTip":                                 chainStore,
	"StorageMode":                            chainStore,
	"TruncateAfterSlot":                      chainStore,
	"GetLiveUtxoLovelace":                    txIndex,
	"IterateLiveUtxoRefs":                    txIndex,
	"IterateLiveUtxos":                       txIndex,
	"PersistTransactionOffsets":              txIndex,
	"ResolveTxCbor":                          txIndex,
	"ResolveUtxoCborWithRecovery":            txIndex,
	"SumTransactionFeesInSlotRange":          txIndex,
	"UtxoByRef":                              txIndex,
	"UtxoByRefIncludingSpent":                txIndex,
	"UtxoExists":                             txIndex,
	"UtxosByAddress":                         txIndex,
	"UtxosByRefs":                            txIndex,
	"UtxosByRefsBounded":                     txIndex,
	"CountPoolBlocksInSlotRange":             loaderOnly,
	"GetImportedPoolBlockCounts":             loaderOnly,
	"GetPoolBlockIssuersInSlotRange":         loaderOnly,
	"IterateAccountImportBaselines":          loaderOnly,
	"IterateAccountWithdrawalEffectiveSlots": loaderOnly,
	"IterateAccountWitnessEffectiveSlots":    loaderOnly,
	"IterateAccountWitnessSlots":             loaderOnly,
	"IterateAccounts":                        loaderOnly,
	"IterateAppliedTransactions":             loaderOnly,
	"IterateDreps":                           loaderOnly,
	"IterateLeiosClosureUtxoEffects":         loaderOnly,
	"IterateLiveDrepDelegators":              loaderOnly,
	"IterateStakeDelegationEvents":           loaderOnly,
	"IterateUnfoldedRewardAccountOutputs":    loaderOnly,
	"LatestPoolOpCertCounters":               loaderOnly,
	"ListPParamUpdates":                      loaderOnly,
	"MergeImportedPoolBlockCounts":           loaderOnly,
	"GetPParamsRows":                         notLedger,
}

func TestStagingLedgerDBPromotesOnlyChainStoreMethods(t *testing.T) {
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
