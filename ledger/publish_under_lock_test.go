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
)

// guardedMutexes are the LedgerState locks that an EventBus subscriber
// handler acquires. Publishing while holding one of these can deadlock the
// node, so no function may do both.
var guardedMutexes = []string{"chainsyncMutex"}

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
