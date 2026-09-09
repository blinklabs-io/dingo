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
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// entryPointEraList is the era table these tests cover. Dijkstra is included
// deliberately: it is off by default at runtime but its ValidateTxFunc is a
// production entry point, and per-era rule duplication means an entry point
// that is only covered for Conway proves nothing about the others.
func entryPointEraList() []eras.EraDesc {
	return eras.ActiveEras(true)
}

// entryPointCorpusRun is the memoized entry-point replay. It is a second
// replay of the corpus, separate from sqliteCorpusResults: the shared
// ouroboros-mock harness validates with its own upstream rule list and offers
// no hook for a caller-supplied validator, so there is no way to observe
// Dingo's entry points from inside the harness pass. corpus_test.go's
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

// TestEntryPointExecutionFaultDetectsBypassedValidator proves the detector
// used by TestConformanceVectorsExerciseDingoEraEntryPoints actually
// discriminates: it accepts the production entry point and rejects both a
// no-op validator and one that returns the vector fixture's own verdict
// without consulting ledger state.
//
// Without this, the coverage assertion above would be unfalsifiable, which is
// the same failure mode as the aggregate pass rate it exists to backstop.
func TestEntryPointExecutionFaultDetectsBypassedValidator(t *testing.T) {
	root, err := corpusTestdataRoot()
	require.NoError(t, err)
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)

	sm, err := NewDingoStateManager()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	probe := loadEntryPointProbeTransaction(t, root, sm)
	observer := newObservedLedgerState(NewDingoStateProvider(sm))
	major, err := protocolMajorVersion(probe.pp)
	require.NoError(t, err)
	production, ok := entryPointForProtocolVersion(entries, major)
	require.True(t, ok, "no era covers protocol major version %d", major)

	route := func(validate validateTxFunc) entryPointRouting {
		entry := production
		entry.Validate = validate
		return routeTransaction(
			entry,
			entryPointFuncName(production),
			probe.tx,
			probe.slot,
			observer,
			probe.pp,
			0,
		)
	}

	t.Run("production entry point is accepted", func(t *testing.T) {
		routing := route(production.Validate)
		require.NoError(t, entryPointExecutionFault(routing))
		require.Positive(
			t,
			routing.LookedUpInputs,
			"%s must resolve the transaction's declared inputs",
			entryPointFuncName(production),
		)
	})

	t.Run("no-op validator is detected", func(t *testing.T) {
		routing := route(func(
			common.Transaction,
			uint64,
			common.LedgerState,
			common.ProtocolParameters,
		) error {
			return nil
		})
		require.Error(
			t,
			entryPointExecutionFault(routing),
			"a validator that accepts everything without reading state must "+
				"be reported as a bypassed validation path",
		)
	})

	t.Run("fixture-only verdict is detected", func(t *testing.T) {
		// The worst case for an outcome-based check: a validator that returns
		// exactly the verdict the vector fixture declares. Every accept/reject
		// comparison against the corpus would agree with it.
		routing := route(func(
			common.Transaction,
			uint64,
			common.LedgerState,
			common.ProtocolParameters,
		) error {
			if probe.expectSuccess {
				return nil
			}
			return errors.New("vector fixture says this transaction fails")
		})
		require.Error(
			t,
			entryPointExecutionFault(routing),
			"a verdict copied from the vector fixture must be reported as a "+
				"bypassed validation path",
		)
	})

	t.Run("rejecting validator that reads no state is detected", func(t *testing.T) {
		routing := route(func(
			common.Transaction,
			uint64,
			common.LedgerState,
			common.ProtocolParameters,
		) error {
			return errors.New("rejected without looking")
		})
		require.Error(
			t,
			entryPointExecutionFault(routing),
			"returning an error is not evidence the validation path ran",
		)
	})
}

// entryPointProbe is a single real corpus transaction plus the state it was
// loaded against, used to exercise the detector.
type entryPointProbe struct {
	tx            common.Transaction
	pp            common.ProtocolParameters
	path          string
	slot          uint64
	expectSuccess bool
}

// loadEntryPointProbeTransaction loads the first corpus vector carrying a
// transaction with at least one declared input, and leaves sm holding that
// vector's initial state. Using real vector data rather than a constructed
// transaction is deliberate: the detector must be shown to work on the same
// input the coverage assertion runs on.
func loadEntryPointProbeTransaction(
	t *testing.T,
	root string,
	sm *DingoStateManager,
) entryPointProbe {
	t.Helper()
	paths, err := collectEntryPointVectors(root)
	require.NoError(t, err)
	loader := conformance.NewPParamsLoaderFromTestdata(root)

	for _, path := range paths {
		vector, err := conformance.DecodeTestVector(path)
		if err != nil {
			continue
		}
		initialState, err := conformance.ParseInitialState(vector.InitialState)
		if err != nil {
			continue
		}
		pp, err := loader.LoadForVector(vector, initialState)
		if err != nil {
			continue
		}
		for _, event := range vector.Events {
			if event.Type != conformance.EventTypeTransaction {
				continue
			}
			tx, err := decodeVectorTransaction(event.TxBytes)
			if err != nil || len(tx.Inputs()) == 0 {
				continue
			}
			require.NoError(t, sm.Reset())
			require.NoError(t, sm.LoadInitialState(initialState, pp))
			return entryPointProbe{
				tx:            tx,
				pp:            pp,
				path:          filepath.Base(path),
				slot:          event.Slot,
				expectSuccess: event.Success,
			}
		}
	}
	t.Fatal("no corpus vector carries a transaction with declared inputs")
	return entryPointProbe{}
}

// TestDingoEraEntryPointsRejectInputlessTransaction covers every era in the
// registry, not just the era the corpus happens to contain.
//
// The corpus is Conway-only, and validation rules are duplicated per era, so
// Conway coverage says nothing about ValidateTxShelley or ValidateTxDijkstra.
// A transaction with no inputs is invalid in every era (Byron's own
// InputSetEmpty rule, and UtxoValidateInputSetEmptyUtxo from Shelley onward),
// which makes it a rule the whole table can be held to. The paired no-op
// assertion is what makes this a detector rather than a restatement: the same
// input is accepted by a validator that does nothing.
func TestDingoEraEntryPointsRejectInputlessTransaction(t *testing.T) {
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)

	sm, err := NewDingoStateManager()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	observer := newObservedLedgerState(NewDingoStateProvider(sm))

	probes := inputlessEraProbes()
	for _, entry := range entries {
		t.Run(entry.Name, func(t *testing.T) {
			probe, ok := probes[entry.Name]
			require.Truef(
				t,
				ok,
				"era %s has no input-less transaction probe; add one so its "+
					"production entry point is covered",
				entry.Name,
			)
			require.Empty(t, probe.tx.Inputs())

			routing := routeTransaction(
				entry,
				entryPointFuncName(entry),
				probe.tx,
				0,
				observer,
				probe.pp,
				0,
			)
			require.NoErrorf(
				t,
				entryPointExecutionFault(routing),
				"%s accepted a transaction with no inputs",
				entryPointFuncName(entry),
			)

			bypassed := entry
			bypassed.Validate = func(
				common.Transaction,
				uint64,
				common.LedgerState,
				common.ProtocolParameters,
			) error {
				return nil
			}
			bypassedRouting := routeTransaction(
				bypassed,
				entryPointFuncName(entry),
				probe.tx,
				0,
				observer,
				probe.pp,
				0,
			)
			require.Errorf(
				t,
				entryPointExecutionFault(bypassedRouting),
				"a no-op replacement for %s must be detected",
				entryPointFuncName(entry),
			)
		})
	}
}

// eraProbe is an era-appropriate transaction and the protocol parameters its
// entry point requires.
type eraProbe struct {
	tx common.Transaction
	pp common.ProtocolParameters
}

// inputlessEraProbes returns one input-less transaction per era, keyed by era
// name.
//
// From Shelley onward each entry point type-asserts its own parameter type,
// so the parameters have to match or the entry point returns
// eras.ErrIncompatibleProtocolParams before reaching any rule -- which would
// satisfy the input-less assertion for the wrong reason.
//
// Byron is the exception and carries nil: ValidateTxByron never asserts on
// pp, and gouroboros has no Byron protocol-parameters type to supply. It runs
// its structural rules unconditionally (byronValidateInputsNotEmpty is what
// rejects the probe) and its UTxO-aware rules whenever a ledger state is
// given, passing pp through to rules that ignore it.
func inputlessEraProbes() map[string]eraProbe {
	return map[string]eraProbe{
		byron.EraNameByron: {
			tx: &byron.ByronTransaction{},
			pp: nil,
		},
		shelley.EraNameShelley: {
			tx: &shelley.ShelleyTransaction{},
			pp: &shelley.ShelleyProtocolParameters{},
		},
		allegra.EraNameAllegra: {
			tx: &allegra.AllegraTransaction{},
			pp: &allegra.AllegraProtocolParameters{},
		},
		mary.EraNameMary: {
			tx: &mary.MaryTransaction{},
			pp: &mary.MaryProtocolParameters{},
		},
		alonzo.EraNameAlonzo: {
			tx: &alonzo.AlonzoTransaction{},
			pp: &alonzo.AlonzoProtocolParameters{},
		},
		babbage.EraNameBabbage: {
			tx: &babbage.BabbageTransaction{},
			pp: &babbage.BabbageProtocolParameters{},
		},
		conway.EraNameConway: {
			tx: &conway.ConwayTransaction{},
			pp: &conway.ConwayProtocolParameters{},
		},
		dijkstra.EraNameDijkstra: {
			tx: &dijkstra.DijkstraTransaction{},
			pp: &dijkstra.DijkstraProtocolParameters{},
		},
	}
}
