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

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
)

// This file is the entry-point replay machinery; entry_points_test.go holds
// the assertions. It is a _test.go file because every identifier in it is
// test-only and unexported, and .golangci.yml sets run.tests: false, so the
// same code in a plain .go file is reported as unused by the linter while
// still being exercised by the package's tests.
//
// The shared ouroboros-mock harness validates each vector transaction with
// common.VerifyTransaction over conformance.ConformanceValidationRules -- a
// list of upstream gouroboros rule functions. Nothing in that path reaches
// Dingo's own era validation entry points (eras.EraDesc.ValidateTxFunc, i.e.
// ValidateTxByron .. ValidateTxDijkstra), which are what the node actually
// runs against live transactions and which differ from the upstream list:
// Conway and Dijkstra substitute Dingo implementations for the committee
// certificate, unknown voter, Plutus, fee and PlutusV1/V2 feature rules, and
// the pre-Alonzo eras replace the upstream fee and max-size rules outright.
//
// The consequence is that the corpus pass rate is independent of those entry
// points: with ValidateTxConway stubbed to `return nil`, all 315 vectors still
// report as passing. This file routes every corpus vector transaction through
// the production entry point for its era as well, and records evidence that
// the entry point actually consulted ledger state derived from the
// transaction, so a bypassed or fixture-only validator cannot read as a pass.

// validateTxFunc is eras.EraDesc.ValidateTxFunc's signature.
type validateTxFunc = func(
	common.Transaction,
	uint64,
	common.LedgerState,
	common.ProtocolParameters,
) error

// eraEntryPoint is one era's production transaction-validation entry point,
// read from Dingo's era registry rather than restated here. Restating the
// list would defeat the purpose: an era whose ValidateTxFunc is dropped from
// the registry has to show up as a missing entry point, not as a local copy
// that keeps working.
type eraEntryPoint struct {
	Validate        validateTxFunc
	Name            string
	Id              uint
	MinMajorVersion uint
	MaxMajorVersion uint
}

// dingoEraEntryPoints reads the production validation entry point out of each
// era descriptor. An era with no ValidateTxFunc is reported rather than
// skipped: a nil entry point is exactly the "validation path bypassed" state
// these tests exist to catch.
func dingoEraEntryPoints(eraList []eras.EraDesc) ([]eraEntryPoint, error) {
	if len(eraList) == 0 {
		return nil, errors.New("era registry is empty")
	}
	entries := make([]eraEntryPoint, 0, len(eraList))
	var missing []string
	for _, era := range eraList {
		if era.ValidateTxFunc == nil {
			missing = append(missing, era.Name)
			continue
		}
		entries = append(entries, eraEntryPoint{
			Validate:        era.ValidateTxFunc,
			Name:            era.Name,
			Id:              era.Id,
			MinMajorVersion: era.MinMajorVersion,
			MaxMajorVersion: era.MaxMajorVersion,
		})
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"eras with no production validation entry point: %v",
			missing,
		)
	}
	return entries, nil
}

// entryPointForProtocolVersion resolves the era entry point covering a
// protocol major version, mirroring eras.EraForVersionIn.
func entryPointForProtocolVersion(
	entries []eraEntryPoint,
	majorVersion uint,
) (eraEntryPoint, bool) {
	for _, entry := range entries {
		if majorVersion >= entry.MinMajorVersion &&
			majorVersion <= entry.MaxMajorVersion {
			return entry, true
		}
	}
	return eraEntryPoint{}, false
}

// protocolMajorVersion reports the protocol major version carried by pp. It
// is what selects the era, and therefore which production entry point a
// vector's transactions belong to.
//
// Every parameter type from Shelley onward implements
// common.PoolRuleProtocolParameters; the Utxorpc projection is the fallback
// for anything that does not.
func protocolMajorVersion(pp common.ProtocolParameters) (uint, error) {
	if pp == nil {
		return 0, errors.New("nil protocol parameters")
	}
	if versioned, ok := pp.(common.PoolRuleProtocolParameters); ok {
		return versioned.ProtocolMajorVersion(), nil
	}
	upp, err := pp.Utxorpc()
	if err != nil {
		return 0, fmt.Errorf("project protocol parameters: %w", err)
	}
	version := upp.GetProtocolVersion()
	if version == nil {
		return 0, errors.New("protocol parameters carry no protocol version")
	}
	return uint(version.GetMajor()), nil
}

// observedLedgerState wraps the real DingoStateProvider and records the reads
// a validation entry point performs through it.
//
// It embeds the concrete provider rather than the common.LedgerState
// interface so that the optional capabilities Dingo's era validation asserts
// for -- eras.CommitteeCredentialState among them -- keep resolving. Wrapping
// the interface would silently strip them and change what the entry point
// validates.
type observedLedgerState struct {
	*DingoStateProvider
	utxoLookups map[string]struct{}
	reads       int
}

// The wrapper must keep satisfying everything the real provider satisfies,
// including the optional capability Dingo's Conway and Dijkstra validation
// type-asserts for. Losing it here would change what the entry point
// validates while still reporting as "routed".
var (
	_ common.LedgerState            = (*observedLedgerState)(nil)
	_ eras.CommitteeCredentialState = (*observedLedgerState)(nil)
)

func newObservedLedgerState(
	provider *DingoStateProvider,
) *observedLedgerState {
	return &observedLedgerState{
		DingoStateProvider: provider,
		utxoLookups:        make(map[string]struct{}),
	}
}

// reset clears the recorded reads so one observer can serve consecutive
// routings without attributing an earlier transaction's lookups to a later
// one.
func (o *observedLedgerState) reset() {
	clear(o.utxoLookups)
	o.reads = 0
}

// UtxoById records the input reference before delegating. The lookup is
// recorded even when it fails: an entry point that asked for an input it
// could not resolve still executed, which is what is being observed.
func (o *observedLedgerState) UtxoById(
	id common.TransactionInput,
) (common.Utxo, error) {
	o.reads++
	if id != nil {
		o.utxoLookups[utxoLookupKey(id)] = struct{}{}
	}
	return o.DingoStateProvider.UtxoById(id)
}

// NetworkId records the read performed by the network-id rules.
func (o *observedLedgerState) NetworkId() uint {
	o.reads++
	return o.DingoStateProvider.NetworkId()
}

// CostModels records the read performed by the script rules.
func (o *observedLedgerState) CostModels() map[common.PlutusLanguage]common.CostModel {
	o.reads++
	return o.DingoStateProvider.CostModels()
}

// utxoLookupKey is the canonical identity of a transaction input, used to
// match what an entry point looked up against what the transaction declared.
// It deliberately does not use TransactionInput.String(), whose format is not
// part of any contract.
func utxoLookupKey(id common.TransactionInput) string {
	txId := id.Id()
	return fmt.Sprintf("%x#%d", txId[:], id.Index())
}

// entryPointRouting is the evidence produced by routing one vector
// transaction through a production era validation entry point.
type entryPointRouting struct {
	// Err is what the entry point returned. A non-nil Err is not a test
	// failure: Dingo's rule set is a strict superset of the corpus rule set
	// (it keeps the fee and max-size rules the corpus excludes because the
	// vectors carry Haskell-computed values), so a vector the corpus accepts
	// may still be rejected here. What is asserted is that the entry point
	// ran and read transaction-derived state, not what it decided.
	Err error

	// EraName is the era whose entry point was used.
	EraName string

	// EntryPoint identifies the production function that ran.
	EntryPoint string

	// EventIndex is the transaction event's index within the vector.
	EventIndex int

	// DeclaredInputs is the number of inputs the transaction declares.
	DeclaredInputs int

	// LookedUpInputs is how many of those declared inputs the entry point
	// resolved through the ledger state. This is the transaction-derived
	// signal: a no-op, bypassed, or fixture-only validator resolves none.
	LookedUpInputs int

	// StateReads is the total number of observed ledger-state reads.
	StateReads int
}

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

// routeTransaction runs one transaction through a production era validation
// entry point against the observed ledger state and returns the evidence.
func routeTransaction(
	entry eraEntryPoint,
	entryPointName string,
	tx common.Transaction,
	slot uint64,
	ls *observedLedgerState,
	pp common.ProtocolParameters,
	eventIndex int,
) entryPointRouting {
	inputs := tx.Inputs()
	declared := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil {
			continue
		}
		declared[utxoLookupKey(input)] = struct{}{}
	}

	ls.reset()
	err := entry.Validate(tx, slot, ls, pp)

	lookedUp := 0
	for key := range declared {
		if _, ok := ls.utxoLookups[key]; ok {
			lookedUp++
		}
	}

	return entryPointRouting{
		Err:            err,
		EraName:        entry.Name,
		EntryPoint:     entryPointName,
		EventIndex:     eventIndex,
		DeclaredInputs: len(declared),
		LookedUpInputs: lookedUp,
		StateReads:     ls.reads,
	}
}

// entryPointExecutionFault reports why a routing fails to prove that the
// production validation path executed, or nil when it does prove it.
//
// The predicate is deliberately independent of the validation verdict. It is
// satisfied only by evidence the entry point could not have produced without
// looking at this transaction: the ledger-state lookups of the inputs the
// transaction itself declares. A validator that returns a canned verdict --
// nil, an error, or a value copied from the vector fixture -- performs none
// of those lookups and is reported here.
func entryPointExecutionFault(routing entryPointRouting) error {
	if routing.EraName == "" || routing.EntryPoint == "" {
		return errors.New(
			"no production era validation entry point was resolved for this transaction",
		)
	}
	if routing.DeclaredInputs == 0 {
		// A transaction with no inputs is invalid in every era (Byron's
		// InputSetEmpty rule and the UtxoValidateInputSetEmptyUtxo rule from
		// Shelley onward), so there is nothing to look up and acceptance is
		// itself proof the path did not run.
		if routing.Err == nil {
			return fmt.Errorf(
				"%s accepted a transaction with no inputs; a validating entry point must reject one",
				routing.EntryPoint,
			)
		}
		return nil
	}
	if routing.LookedUpInputs == 0 {
		return fmt.Errorf(
			"%s resolved none of the transaction's %d declared inputs through the ledger state (%d total state reads); the production validation path did not run",
			routing.EntryPoint,
			routing.DeclaredInputs,
			routing.StateReads,
		)
	}
	return nil
}

// entryPointFuncName is the reporting name of an era's production entry
// point, e.g. "eras.ValidateTxConway".
func entryPointFuncName(entry eraEntryPoint) string {
	return "eras.ValidateTx" + entry.Name
}

// collectEntryPointVectors walks the same corpus roots the shared harness
// walks, so this pass and the harness pass see the same vector set.
func collectEntryPointVectors(testdataRoot string) ([]string, error) {
	var all []string
	for _, sub := range []string{"eras", "synthetic"} {
		root := filepath.Join(testdataRoot, sub)
		paths, err := conformance.CollectVectorFiles(root)
		if err != nil {
			if sub == "synthetic" {
				continue
			}
			return nil, fmt.Errorf("collect %s vectors: %w", sub, err)
		}
		all = append(all, paths...)
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no vectors found under %s", testdataRoot)
	}
	sort.Strings(all)
	return all, nil
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

// entryPointCorpusDecodeEra is the era decodeVectorTransaction decodes as.
const entryPointCorpusDecodeEra = conway.EraNameConway

// decodeVectorTransaction decodes a vector transaction. The corpus is Conway,
// and the shared harness decodes it the same way; routeVectorTransaction
// cross-checks the decoded era against the era the vector's protocol
// parameters select, so a future corpus in another era cannot pass unnoticed.
func decodeVectorTransaction(txBytes []byte) (common.Transaction, error) {
	tx := &conway.ConwayTransaction{}
	if _, err := cbor.Decode(txBytes, tx); err != nil {
		return nil, err
	}
	return tx, nil
}
