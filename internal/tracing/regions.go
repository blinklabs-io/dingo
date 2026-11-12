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

package tracing

// Region names for runtime/trace user regions. They mark stage boundaries
// that an execution trace cannot otherwise attribute: a goroutine blocked on
// a mutex shows the waiter but not what the holder was doing. Regions are
// recorded only while an execution trace is being captured; with tracing off
// runtime/trace.StartRegion returns after one atomic load.
const (
	// RegionChainSelectionEvaluate covers the chain selector's candidate
	// evaluation while it holds the selector mutex.
	RegionChainSelectionEvaluate = "dingo.chainselection.evaluate"
	// RegionChainSelectionAdmission covers peer tip admission while it
	// holds the selector mutex: recording an observed tip and committing a
	// staged admission.
	RegionChainSelectionAdmission = "dingo.chainselection.admission"
	// RegionChainSelectionGSMStep covers one Genesis state machine step,
	// whether it tries to leave or to re-enter Genesis mode.
	RegionChainSelectionGSMStep = "dingo.chainselection.gsm_step"
	// RegionChainsyncHeaderSettleWait covers a chainsync header waiting for
	// the ledger to settle header verification context.
	RegionChainsyncHeaderSettleWait = "dingo.chainsync.header_settle_wait"
	// RegionLedgerBatchApplyCommit covers one ledger block batch: delta
	// apply and database commit, on the database worker goroutine that runs
	// them.
	RegionLedgerBatchApplyCommit = "dingo.ledger.batch_apply_commit"
)
