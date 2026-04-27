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

// TestProcessEpochRollover_OrderingInvariant pins the EPOCH→HARDFORK call
// sequence inside processEpochRollover. The order matters for correctness
// (see the long-form comment at the top of processEpochRollover's body)
// but is otherwise hard to test through observable side effects without
// substantial governance + pparams + cert fixture setup.
//
// This is a structural lock-in test: it parses chainsync.go's AST, locates
// the body of processEpochRollover, and asserts that the named call
// expressions appear in the expected source order. A future refactor that
// reorders these calls — even if all unit tests pass — will fail this
// test loudly. The error message points to the comment block that
// documents the invariant.
//
// Markers are matched by suffix on the call's selector expression so that
// receiver renames (`ls.db` → `ls.metadata`) don't churn the test.
func TestProcessEpochRollover_OrderingInvariant(t *testing.T) {
	const targetFunc = "processEpochRollover"

	// In source order, the calls that must appear inside processEpochRollover.
	// Each entry is the trailing identifier of a SelectorExpr (or a bare
	// Ident for unqualified calls).
	wantOrder := []string{
		"ComputeAndApplyPParamUpdates", // (1) Shelley-style pparam updates
		"ProcessEpoch",                 // (2) Conway-style governance enact
		"SetPParams",                   // (3) persist enacted pparams
		"IsHardForkTransition",         // (4) inter-era boundary detection
		"applyIntraEraHardForkRule",    // (5) per-major-version HARDFORK rule
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "chainsync.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err, "parse chainsync.go")

	var fnDecl *ast.FuncDecl
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fd.Name.Name == targetFunc {
			fnDecl = fd
			break
		}
	}
	require.NotNil(t, fnDecl, "%s not found in chainsync.go", targetFunc)
	require.NotNil(t, fnDecl.Body, "%s has no body", targetFunc)

	// Walk the function body, recording the source position of each
	// call whose final identifier is in our marker set.
	wanted := make(map[string]struct{}, len(wantOrder))
	for _, m := range wantOrder {
		wanted[m] = struct{}{}
	}
	type hit struct {
		marker string
		pos    token.Pos
	}
	var hits []hit
	ast.Inspect(fnDecl.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var name string
		switch fn := call.Fun.(type) {
		case *ast.SelectorExpr:
			name = fn.Sel.Name
		case *ast.Ident:
			name = fn.Name
		default:
			return true
		}
		if _, ok := wanted[name]; ok {
			hits = append(hits, hit{marker: name, pos: call.Pos()})
		}
		return true
	})

	// Build the observed order by first occurrence (a marker can appear
	// in nested branches; we only care about its first appearance).
	seen := make(map[string]bool, len(wantOrder))
	var observed []string
	for _, h := range hits {
		if seen[h.marker] {
			continue
		}
		seen[h.marker] = true
		observed = append(observed, h.marker)
	}

	for _, m := range wantOrder {
		require.True(t, seen[m],
			"marker %q not found in %s body — was it renamed or removed? "+
				"If renamed, update wantOrder. If removed, also revisit "+
				"the EPOCH→HARDFORK ordering comment in chainsync.go.",
			m, targetFunc)
	}

	// observedFiltered is observed restricted to the wanted markers, in
	// observed order. (Equivalent to observed today since wanted gates
	// inclusion above, but keeps the assertion intent explicit.)
	observedFiltered := observed
	require.Equal(t, wantOrder, observedFiltered,
		"call sequence in %s drifted from the EPOCH→HARDFORK ordering "+
			"invariant. Expected %v, observed %v. See the comment block "+
			"at the top of the function body for the rationale; if the "+
			"reorder is intentional, update both the comment and "+
			"wantOrder in this test.",
		targetFunc, wantOrder, observedFiltered)
}
