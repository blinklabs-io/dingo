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

package conformance

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// vectorEntryPointEvidence records one vector's trip through the production
// entry points, preserving per-vector identity so an aggregate cannot hide a
// vector whose validation path never ran.
type vectorEntryPointEvidence struct {
	// Err is a replay failure (decode, initial state, epoch boundary). It is
	// a test failure: it means the vector produced no entry-point evidence.
	Err error

	Path     string
	Title    string
	TxEvents int
	Routings []entryPointRouting
}

// replayEntryPoints replays the corpus against sm, routing every transaction
// event through the production era entry point resolved from the vector's own
// protocol parameters.
//
// It is a separate replay from the shared harness's, because the harness has
// no hook for a caller-supplied validator and never reaches Dingo's entry
// points. State advancement mirrors the harness: successful transactions are
// applied, epoch events cross the boundary, and a rollback event restores the
// initial state and re-applies the journaled transactions at or below the
// target slot. The one modelled difference is that only transactions are
// journaled, not epoch events, so a rollback that follows an epoch boundary
// is reported as an error rather than replayed -- no corpus vector does that
// today.
func replayEntryPoints(
	sm *DingoStateManager,
	testdataRoot string,
	entries []eraEntryPoint,
) ([]vectorEntryPointEvidence, error) {
	paths, err := collectEntryPointVectors(testdataRoot)
	if err != nil {
		return nil, err
	}
	loader := conformance.NewPParamsLoaderFromTestdata(testdataRoot)
	provider := NewDingoStateProvider(sm)
	observer := newObservedLedgerState(provider)

	evidence := make([]vectorEntryPointEvidence, 0, len(paths))
	for _, path := range paths {
		ev := replayVectorEntryPoints(sm, loader, observer, entries, path)
		// The corpus is extracted to a fresh temp directory per process, so
		// the absolute path is not a stable subtest name. Report the path
		// relative to the corpus root instead.
		if rel, err := filepath.Rel(testdataRoot, path); err == nil {
			ev.Path = rel
		}
		evidence = append(evidence, ev)
	}
	return evidence, nil
}

// appliedTx is a journaled transaction, retained so a rollback event can
// re-apply the transactions at or below its target slot.
type appliedTx struct {
	tx   common.Transaction
	slot uint64
}

func replayVectorEntryPoints(
	sm *DingoStateManager,
	loader *conformance.PParamsLoader,
	observer *observedLedgerState,
	entries []eraEntryPoint,
	path string,
) vectorEntryPointEvidence {
	ev := vectorEntryPointEvidence{Path: path}

	vector, err := conformance.DecodeTestVector(path)
	if err != nil {
		ev.Err = fmt.Errorf("decode vector: %w", err)
		return ev
	}
	ev.Title = vector.Title

	initialState, err := conformance.ParseInitialState(vector.InitialState)
	if err != nil {
		ev.Err = fmt.Errorf("parse initial state: %w", err)
		return ev
	}
	pp, err := loader.LoadForVector(vector, initialState)
	if err != nil {
		ev.Err = fmt.Errorf("load protocol parameters: %w", err)
		return ev
	}
	if err := sm.Reset(); err != nil {
		ev.Err = fmt.Errorf("reset state: %w", err)
		return ev
	}
	if err := sm.LoadInitialState(initialState, pp); err != nil {
		ev.Err = fmt.Errorf("load initial state: %w", err)
		return ev
	}

	epoch := initialState.CurrentEpoch
	var applied []appliedTx
	var epochCrossed bool

	for idx, event := range vector.Events {
		switch event.Type {
		case conformance.EventTypeTransaction:
			ev.TxEvents++
			tx, err := decodeVectorTransaction(event.TxBytes)
			if err != nil {
				// The harness tolerates a decode failure on an
				// expected-failure event; so does this pass, but the event is
				// not counted as one that reached an entry point.
				if event.Success {
					ev.Err = fmt.Errorf(
						"event %d: decode transaction: %w",
						idx,
						err,
					)
					return ev
				}
				ev.TxEvents--
				continue
			}
			routing, err := routeVectorTransaction(
				entries, observer, tx, event.Slot, pp, idx,
			)
			if err != nil {
				ev.Err = err
				return ev
			}
			ev.Routings = append(ev.Routings, routing)
			if event.Success {
				if err := sm.ApplyTransaction(tx, event.Slot); err != nil {
					ev.Err = fmt.Errorf("event %d: apply: %w", idx, err)
					return ev
				}
				applied = append(applied, appliedTx{tx: tx, slot: event.Slot})
			}
		case conformance.EventTypePassEpoch:
			epoch += event.EpochDelta
			if err := sm.ProcessEpochBoundary(epoch); err != nil {
				ev.Err = fmt.Errorf("event %d: epoch boundary: %w", idx, err)
				return ev
			}
			pp = sm.GetProtocolParameters()
			epochCrossed = true
		case conformance.EventTypeRollback:
			if epochCrossed {
				// The harness restores initialProtocolParams and replays its
				// journaled epoch events on rollback. This pass journals only
				// transactions, so it can neither undo an enacted parameter
				// change nor re-cross a boundary. No vector in the corpus
				// rolls back after an epoch event, so rather than model a
				// path nothing exercises -- and silently route later
				// transactions through an era selected from stale parameters
				// -- fail loudly if one ever appears.
				ev.Err = fmt.Errorf(
					"event %d: rollback after an epoch boundary is not modelled by this replay; journal epoch events and restore the vector's initial protocol parameters before relying on it",
					idx,
				)
				return ev
			}
			retained, err := rollbackEntryPointReplay(
				sm, initialState, pp, applied, event.RollbackSlot,
			)
			if err != nil {
				ev.Err = fmt.Errorf("event %d: rollback: %w", idx, err)
				return ev
			}
			applied = retained
			epoch = initialState.CurrentEpoch
		case conformance.EventTypePassTick:
			// No state effect; the harness only advances its slot cursor.
		}
	}
	return ev
}

// routeVectorTransaction resolves the era entry point from the active
// protocol parameters and routes tx through it.
func routeVectorTransaction(
	entries []eraEntryPoint,
	observer *observedLedgerState,
	tx common.Transaction,
	slot uint64,
	pp common.ProtocolParameters,
	eventIndex int,
) (entryPointRouting, error) {
	major, err := protocolMajorVersion(pp)
	if err != nil {
		return entryPointRouting{}, fmt.Errorf(
			"event %d: resolve protocol major version: %w",
			eventIndex,
			err,
		)
	}
	entry, ok := entryPointForProtocolVersion(entries, major)
	if !ok {
		return entryPointRouting{}, fmt.Errorf(
			"event %d: no era covers protocol major version %d",
			eventIndex,
			major,
		)
	}
	if entry.Name != entryPointCorpusDecodeEra {
		// decodeVectorTransaction decodes the corpus as Conway. A vector whose
		// parameters place it in another era would be handed to that era's
		// entry point as a Conway transaction, so fail loudly instead of
		// reporting coverage the run does not have.
		return entryPointRouting{}, fmt.Errorf(
			"event %d: protocol major version %d selects era %s but the corpus is decoded as %s; add a decoder for %s before claiming its entry point is covered",
			eventIndex,
			major,
			entry.Name,
			entryPointCorpusDecodeEra,
			entry.Name,
		)
	}
	return routeTransaction(
		entry,
		entryPointFuncName(entry),
		tx,
		slot,
		observer,
		pp,
		eventIndex,
	), nil
}

// rollbackEntryPointReplay mirrors the shared harness's rollback: reset,
// reload the vector's initial state, and re-apply the journaled transactions
// at or below the target slot. Re-applied transactions are not routed again;
// they already produced their evidence on first execution.
//
// pp is the vector's initial protocol parameters. replayVectorEntryPoints
// refuses a rollback that follows an epoch boundary, so the parameters still
// active here are the ones LoadForVector produced, which is what the harness
// restores explicitly from its own initialProtocolParams.
func rollbackEntryPointReplay(
	sm *DingoStateManager,
	initialState *conformance.ParsedInitialState,
	pp common.ProtocolParameters,
	applied []appliedTx,
	targetSlot uint64,
) ([]appliedTx, error) {
	retained := make([]appliedTx, 0, len(applied))
	for _, entry := range applied {
		if entry.slot <= targetSlot {
			retained = append(retained, entry)
		}
	}
	if err := sm.Reset(); err != nil {
		return nil, fmt.Errorf("reset: %w", err)
	}
	if err := sm.LoadInitialState(initialState, pp); err != nil {
		return nil, fmt.Errorf("reload initial state: %w", err)
	}
	for _, entry := range retained {
		if err := sm.ApplyTransaction(entry.tx, entry.slot); err != nil {
			return nil, fmt.Errorf("replay slot %d: %w", entry.slot, err)
		}
	}
	return retained, nil
}

// entryPointCorpusRun is the memoized entry-point replay. It is a second
// replay of the corpus, separate from sqliteCorpusResults: the shared
// ouroboros-mock harness validates with its own upstream rule list and offers
// no hook for a caller-supplied validator, so there is no way to observe
// Dingo's entry points from inside the harness pass. tests_cc29688c_test.go's
// "replay once per backend" reasoning still holds for storage-dialect
// coverage; what this pass buys is different, and is not obtainable from the
// harness replay at any count.
//
// The cost is the state replay, not the validation. Running this pass with
// the entry-point call removed takes the same wall clock as running it with
// the call in place, so eras.ValidateTx* is free at this corpus size; what
// the pass pays for is a second Reset/LoadInitialState/ApplyTransaction pass
// over the corpus, which is roughly what the harness replay itself costs.
// Measured on one machine, the package went from 585s to 891s under -race.
// It runs against SQLite only -- the Postgres and MySQL replays exist for
// storage-dialect coverage, and the entry points do not vary by backend.
//
// It replays once per process, like sqliteCorpusResults, so a build that adds
// more consumers of this evidence does not add more replays.
type entryPointCorpusRun struct {
	err      error
	entries  []eraEntryPoint
	evidence []vectorEntryPointEvidence
}

var (
	entryPointCorpusOnce sync.Once
	entryPointCorpusData entryPointCorpusRun
)

func entryPointCorpusEvidence(t *testing.T) entryPointCorpusRun {
	t.Helper()
	entryPointCorpusOnce.Do(func() {
		entries, err := dingoEraEntryPoints(entryPointEraList())
		if err != nil {
			entryPointCorpusData = entryPointCorpusRun{err: err}
			return
		}
		root, err := corpusTestdataRoot()
		if err != nil {
			entryPointCorpusData = entryPointCorpusRun{err: err}
			return
		}
		sm, err := NewDingoStateManager()
		if err != nil {
			entryPointCorpusData = entryPointCorpusRun{
				err: fmt.Errorf("new sqlite state manager: %w", err),
			}
			return
		}
		defer sm.Close()
		evidence, err := replayEntryPoints(sm, root, entries)
		entryPointCorpusData = entryPointCorpusRun{
			err:      err,
			entries:  entries,
			evidence: evidence,
		}
	})
	require.NoError(t, entryPointCorpusData.err, "entry point corpus replay")
	return entryPointCorpusData
}

// TestDingoEraRegistryExposesValidationEntryPoints fails when any era's
// production transaction-validation entry point is missing from the registry.
//
// A nil ValidateTxFunc is the cheapest way to bypass validation for an era,
// and nothing else in the conformance package notices: the shared harness
// never reads the registry.
func TestDingoEraRegistryExposesValidationEntryPoints(t *testing.T) {
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)
	require.Len(t, entries, len(entryPointEraList()))

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name)
	}
	t.Logf("era validation entry points: %v", names)

	// The registry must cover every protocol major version the corpus can
	// select, without a gap between adjacent eras.
	for i := 1; i < len(entries); i++ {
		require.Equal(
			t,
			entries[i-1].MaxMajorVersion+1,
			entries[i].MinMajorVersion,
			"protocol major version gap between %s and %s leaves versions with no validation entry point",
			entries[i-1].Name,
			entries[i].Name,
		)
	}
}

// TestDingoEraEntryPointsReportsMissingValidator proves the registry check
// above detects the state it exists to catch. Without it,
// TestDingoEraRegistryExposesValidationEntryPoints would assert something
// that is true of any table, including one whose entry points were removed.
func TestDingoEraEntryPointsReportsMissingValidator(t *testing.T) {
	eraList := entryPointEraList()
	require.NotEmpty(t, eraList)

	bypassed := make([]eras.EraDesc, len(eraList))
	copy(bypassed, eraList)
	bypassed[len(bypassed)-1].ValidateTxFunc = nil

	_, err := dingoEraEntryPoints(bypassed)
	require.ErrorContains(
		t,
		err,
		"no production validation entry point",
		"an era with no validation entry point must be reported",
	)
	require.ErrorContains(t, err, eraList[len(eraList)-1].Name)

	_, err = dingoEraEntryPoints(nil)
	require.Error(t, err, "an empty era registry must be reported")
}

// TestConformanceVectorsExerciseDingoEraEntryPoints routes every corpus
// vector through Dingo's production validation entry point for its era and
// asserts, per vector, that the entry point actually executed against ledger
// state derived from that vector's transactions.
//
// TestRulesConformanceVectors cannot make this assertion: it reports the
// shared harness's verdict, which is produced by upstream gouroboros rules
// and stays green with ValidateTxConway stubbed out entirely.
func TestConformanceVectorsExerciseDingoEraEntryPoints(t *testing.T) {
	run := entryPointCorpusEvidence(t)
	require.NotEmpty(
		t,
		run.evidence,
		"corpus replay produced no vectors; an empty corpus would otherwise "+
			"report as full entry-point coverage",
	)

	reportEntryPointCoverage(t, run)

	var routedVectors int
	for _, ev := range run.evidence {
		t.Run(ev.Path, func(t *testing.T) {
			require.NoError(t, ev.Err, "vector %s: %s", ev.Path, ev.Title)
			require.Len(
				t,
				ev.Routings,
				ev.TxEvents,
				"vector %s routed %d of %d transaction events through a "+
					"production entry point",
				ev.Path,
				len(ev.Routings),
				ev.TxEvents,
			)
			for _, routing := range ev.Routings {
				require.NoErrorf(
					t,
					entryPointExecutionFault(routing),
					"vector %s (%s) event %d",
					ev.Path,
					ev.Title,
					routing.EventIndex,
				)
			}
		})
		if len(ev.Routings) > 0 {
			routedVectors++
		}
	}

	require.Positive(
		t,
		routedVectors,
		"no vector routed a transaction through a production validation "+
			"entry point",
	)
}

// reportEntryPointCoverage logs which eras the corpus actually reached, so an
// aggregate pass cannot be read as covering eras the corpus never touches.
func reportEntryPointCoverage(t *testing.T, run entryPointCorpusRun) {
	t.Helper()
	perEra := make(map[string]int)
	var routings int
	for _, ev := range run.evidence {
		for _, routing := range ev.Routings {
			perEra[routing.EntryPoint]++
			routings++
		}
	}
	eraNames := make([]string, 0, len(perEra))
	for name := range perEra {
		eraNames = append(eraNames, name)
	}
	sort.Strings(eraNames)

	t.Logf("Dingo validation entry point coverage (sqlite):")
	t.Logf("  Vectors replayed: %d", len(run.evidence))
	t.Logf("  Transactions routed: %d", routings)
	for _, name := range eraNames {
		t.Logf("  %s: %d transactions", name, perEra[name])
	}
	for _, entry := range run.entries {
		if perEra[entryPointFuncName(entry)] == 0 {
			t.Logf(
				"  %s: 0 transactions (no %s vectors in this corpus; covered "+
					"by TestDingoEraEntryPointsRejectInputlessTransaction only)",
				entryPointFuncName(entry),
				entry.Name,
			)
		}
	}
}
