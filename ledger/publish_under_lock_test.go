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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// guardedMutexes are the LedgerState locks that an EventBus subscriber
// handler acquires. Publishing while holding one of these can deadlock the
// node, so no function may do both.
//
// Both are listed because RecoverAfterLocalRollback -- the subscriber that
// closes the cycle -- takes chainsyncMutex and then nests
// chainsyncBlockfetchMutex inside it via startQueuedBlockfetchLocked.
// Holding either one while publishing is therefore enough to deadlock;
// guarding only the outer mutex would miss every path that runs under the
// blockfetch lock alone, such as handleEventBlockfetch's.
var guardedMutexes = []string{
	"chainsyncMutex",
	"chainsyncBlockfetchMutex",
}

// TestNoEventBusPublishWhileHoldingChainsyncMutex enforces that nothing in
// this package publishes to the EventBus while holding a mutex that an
// EventBus subscriber needs.
//
// EventBus delivery blocks when a subscriber's buffer is full — deliberate,
// since the bus backpressures rather than dropping events — and
// ChainsyncResyncEventType's subscriber calls RecoverAfterLocalRollback,
// which takes chainsyncMutex. Publish under that lock and the two can wait
// on each other forever: the subscriber wants the lock the publisher holds,
// the publisher wants the buffer capacity the subscriber would free.
//
// It does not stay contained. handleConnectionClosedEvent takes the same
// mutex, so ledger.conn_closed stops draining; node.go's handler
// translating connmanager.conn_closed into ledger.conn_closed then blocks
// inside its own callback, which stops connmanager.conn_closed draining,
// and every subsequent connection close parks another publisher goroutine.
// A DevNet run reproduced exactly that: ~217k "event delivery stalled:
// subscriber not draining type=connmanager.conn_closed" warnings in five
// minutes, the mempool component silent, and the node still forging but no
// longer completing Node-to-Node handshakes.
//
// This covers Publish, PublishBlocking and PublishAsync alike. All three
// wait for capacity rather than dropping, so all three can close the
// cycle; PublishAsync merely does it through the shared async queue and
// its worker pool instead of a single subscriber's buffer.
//
// Queue the event with pendingPublishes and flush it after the unlock
// instead.
//
// The check is intra-procedural, which is not the whole story: a lock
// holder can also reach a publish through a helper. Those paths are
// enumerated by TestChainsyncResyncPublishPathsUnderLock rather than left
// to be assumed safe.
func TestNoEventBusPublishWhileHoldingChainsyncMutex(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fset := token.NewFileSet()
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(
			fset, filepath.Join(".", name), nil, parser.ParseComments,
		)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			checked++
			for _, v := range violations(fn) {
				t.Errorf(
					"%s: %s publishes to the EventBus while holding %s;"+
						" queue it with pendingPublishes and flush after"+
						" the unlock (see pending_publish.go)",
					fset.Position(v.pos), fn.Name.Name, v.mutex,
				)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no functions inspected; the scan is not working")
	}
}

type violation struct {
	pos   token.Pos
	mutex string
}

type lockEvent struct {
	pos   token.Pos
	kind  string // "lock", "unlock", "deferUnlock", "publish"
	mutex string
}

// violations walks a function in source order, tracking which guarded
// mutexes are held, and reports publishes made while one is.
//
// Source order is a good enough model of control flow for this code: these
// functions either hold a mutex for their whole body via defer, or take and
// release it around a specific region. A deferred unlock keeps the mutex
// held to the end of the function, which is what makes a publish anywhere
// after the Lock unsafe.
func violations(fn *ast.FuncDecl) []violation {
	deferred := map[token.Pos]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if d, ok := n.(*ast.DeferStmt); ok && d.Call != nil {
			deferred[d.Call.Pos()] = true
		}
		return true
	})

	var events []lockEvent
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "Lock", "Unlock":
			if !slices.Contains(guardedMutexes, inner.Sel.Name) {
				return true
			}
			kind := "lock"
			if sel.Sel.Name == "Unlock" {
				kind = "unlock"
				if deferred[call.Pos()] {
					kind = "deferUnlock"
				}
			}
			events = append(events, lockEvent{
				pos: call.Pos(), kind: kind, mutex: inner.Sel.Name,
			})
		case "Publish", "PublishBlocking", "PublishAsync":
			// Only EventBus publishes; other types have Publish methods.
			// PublishAsync is included: it does not park on a
			// subscriber's buffer, but it does wait for room in the
			// shared async queue rather than dropping the event, and that
			// queue is drained by a worker pool whose workers run
			// subscriber handlers. A handler that needs the publisher's
			// mutex parks a worker, the queue fills, and the same cycle
			// closes.
			if inner.Sel.Name != "EventBus" {
				return true
			}
			events = append(events, lockEvent{
				pos: call.Pos(), kind: "publish",
			})
		}
		return true
	})

	slices.SortFunc(events, func(a, b lockEvent) int {
		return int(a.pos - b.pos)
	})

	held := map[string]bool{}
	heldToEnd := map[string]bool{}
	var found []violation
	for _, ev := range events {
		switch ev.kind {
		case "lock":
			held[ev.mutex] = true
		case "unlock":
			if !heldToEnd[ev.mutex] {
				held[ev.mutex] = false
			}
		case "deferUnlock":
			// Released only when the function returns.
			heldToEnd[ev.mutex] = true
		case "publish":
			for mu, isHeld := range held {
				if isHeld {
					found = append(found, violation{pos: ev.pos, mutex: mu})
				}
			}
		}
	}
	return found
}

// knownResyncPublishPathsUnderLock records every helper that can publish
// ChainsyncResyncEventType while one of guardedMutexes is held.
//
// That event is the dangerous one: its subscriber calls
// RecoverAfterLocalRollback, which takes the same mutex, so a publish
// reaching it from under the lock is the deadlock pendingPublishes
// exists to break. handleChainSwitchEvent was converted because a review
// demonstrated it concretely; the rest are pre-existing paths that
// predate this change.
//
// Converting them means threading a queue through roughly ten functions
// in the hottest consensus path, which belongs in its own change with its
// own DevNet and soak validation rather than riding along here. This list
// is the honest alternative to silence: the guard above is
// intra-procedural, so without it the remaining exposure would look like
// it had been handled.
var knownResyncPublishPathsUnderLock = []string{
	"handleBlockfetchTimeoutLocked",
	"handleEventBlockfetchBatchDone",
	"handleEventChainsyncRollback",
	"handoffPipelineOnSwitchLocked",
	"replayBufferedHeaderEvents",
	"restartQueuedBlockfetchAfterForkLocked",
	"startQueuedBlockfetchLocked",
}

// TestChainsyncResyncPublishPathsUnderLock pins the set of helpers that
// can publish ChainsyncResyncEventType while the mutex is held.
//
// It fails in both directions on purpose. A new such path is a new
// deadlock and must be converted rather than appended here; a path that
// disappears should be removed, so the list cannot rot into overstating
// the problem.
func TestChainsyncResyncPublishPathsUnderLock(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset, filepath.Join(".", "chainsync.go"), nil, parser.ParseComments,
	)
	if err != nil {
		t.Fatalf("parse chainsync.go: %v", err)
	}

	publishesResync := map[string]bool{}
	queuedResync := map[string]bool{}
	callees := map[string]map[string]bool{}
	nilQueueCalls := map[string]map[string]bool{}
	holdsLock := map[string]bool{}
	var order []string

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		order = append(order, name)
		callees[name] = map[string]bool{}
		nilQueueCalls[name] = map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if node.Sel.Name == "ChainsyncResyncEventType" {
					if usesInlinePublish(fn) {
						publishesResync[name] = true
					} else {
						// Queues instead. Safe only for callers that
						// hand it a queue -- see nilQueueCalls.
						queuedResync[name] = true
					}
				}
			case *ast.CallExpr:
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				// A queue-taking helper called with a nil queue
				// publishes immediately, so the caller is the publisher.
				if ident, ok := sel.X.(*ast.Ident); ok &&
					ident.Name == "ls" {
					for _, arg := range node.Args {
						if id, ok := arg.(*ast.Ident); ok &&
							id.Name == "nil" {
							nilQueueCalls[name][sel.Sel.Name] = true
						}
					}
				}
				if inner, ok := sel.X.(*ast.SelectorExpr); ok &&
					slices.Contains(guardedMutexes, inner.Sel.Name) &&
					sel.Sel.Name == "Lock" {
					holdsLock[name] = true
				}
				if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "ls" {
					callees[name][sel.Sel.Name] = true
				}
			}
			return true
		})
	}

	// A caller that hands nil to a queue-taking resync helper makes that
	// helper publish inline, so the caller owns the publish. Without this
	// the guard silently stopped reporting every requestChainsyncResync
	// route the moment that helper was converted to take a queue.
	for caller, targets := range nilQueueCalls {
		for target := range targets {
			if queuedResync[target] {
				publishesResync[caller] = true
			}
		}
	}

	// Transitive closure: anything reaching an inline resync publish.
	reaches := map[string]bool{}
	for name, ok := range publishesResync {
		if ok {
			reaches[name] = true
		}
	}
	for range order {
		for name, cs := range callees {
			if reaches[name] {
				continue
			}
			for c := range cs {
				if reaches[c] {
					reaches[name] = true
					break
				}
			}
		}
	}

	var found []string
	for _, name := range order {
		if holdsLock[name] || !reaches[name] {
			continue
		}
		// Only helpers a lock holder can actually reach.
		reachedFromLockHolder := false
		for holder, cs := range callees {
			if holdsLock[holder] && cs[name] {
				reachedFromLockHolder = true
				break
			}
		}
		if reachedFromLockHolder && !slices.Contains(found, name) {
			found = append(found, name)
		}
	}
	slices.Sort(found)

	expected := slices.Clone(knownResyncPublishPathsUnderLock)
	slices.Sort(expected)
	// Built from guardedMutexes so the message cannot drift as that list
	// grows -- it named only chainsyncMutex after the blockfetch mutex was
	// added, which would misdirect anyone hitting the failure.
	require.Equal(t, expected, found,
		"the set of helpers publishing ChainsyncResyncEventType while"+
			" holding one of %v changed. A new entry is a new deadlock:"+
			" thread a pendingPublishes queue through it instead of adding"+
			" it here. A missing entry means it was fixed -- drop it from"+
			" knownResyncPublishPathsUnderLock.",
		guardedMutexes)
}

// usesInlinePublish reports whether a function publishes directly rather
// than queueing through pendingPublishes.
func usesInlinePublish(fn *ast.FuncDecl) bool {
	inline := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name != "Publish" && sel.Sel.Name != "PublishBlocking" &&
			sel.Sel.Name != "PublishAsync" {
			return true
		}
		if inner, ok := sel.X.(*ast.SelectorExpr); ok &&
			inner.Sel.Name == "EventBus" {
			inline = true
		}
		return true
	})
	return inline
}
