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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReplayRecoveryRollbackLockOrder protects the lock-order contract shared
// with rollbackChainAndStateDeferred. The blockfetch lock must be acquired
// before transactionEventMutex. The old implementation acquired those locks
// in the opposite order, so this test fails against the unfixed code instead
// of merely checking that both mutexes appear somewhere in the function.
func TestReplayRecoveryRollbackLockOrder(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset,
		"replay_recovery.go",
		nil,
		0,
	)
	require.NoError(t, err)

	var rollbackBody *ast.BlockStmt
	ast.Inspect(file, func(node ast.Node) bool {
		function, ok := node.(*ast.FuncDecl)
		if ok && function.Name.Name == "rollbackPrimaryChainInSecurityParamWindows" {
			rollbackBody = function.Body
			return false
		}
		return true
	})
	require.NotNil(t, rollbackBody,
		"replay recovery rollback function must remain present")

	var lockOrder []string
	ast.Inspect(rollbackBody, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Lock" {
			return true
		}
		mutex, ok := selector.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch mutex.Sel.Name {
		case "chainsyncBlockfetchMutex", "transactionEventMutex":
			lockOrder = append(lockOrder, mutex.Sel.Name)
		}
		return true
	})

	require.Equal(
		t,
		[]string{"chainsyncBlockfetchMutex", "transactionEventMutex"},
		lockOrder,
		"replay recovery must use the same lock order as rollbackChainAndStateDeferred",
	)
}
