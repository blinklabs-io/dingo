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

package dingo

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
)

// apiBindCallSites are the two places that hand a bind address to the
// three API provider factories: Run builds them at startup and
// reinitializeAPIServers rebuilds them after a live restore/truncate.
// Both must pass apiBindAddr, and each must do it for all three
// providers, or one of the listeners silently returns to the public
// wildcard bindAddr on one of the two paths (issue #3498).
var apiBindCallSites = map[string]int{
	"node.go":           3,
	"node_lifecycle.go": 3,
}

// apiProviderDependencyTypes are the provider dependency structs whose
// Host field is the address that listener binds. midnightserver.Config
// and the bark client are deliberately absent: they carry their own
// host settings (midnight.host, barkHost) and are not governed by
// apiBindAddr.
var apiProviderDependencyTypes = map[string]bool{
	"blockfrost.ProviderDependencies": true,
	"mesh.ProviderDependencies":       true,
	"utxorpc.ProviderDependencies":    true,
}

// TestAPIProviderDependenciesUseAPIBindAddr asserts both node call sites
// resolve every API listener's Host from n.config.apiBindAddr.
//
// It is a source-level guard rather than a runtime one because neither
// call site is reachable without a started node: Run needs a live
// network stack and reinitializeAPIServers needs an open database, a
// resolved plugin host, and storageMode "api". A guard is still worth
// having, because reverting one Host field back to n.config.bindAddr is
// a one-word edit that no existing test observes -- coverage otherwise
// stops at buildDingoConfig and the provider factories, both of which
// stay green while a node call site hands down the wrong address.
func TestAPIProviderDependenciesUseAPIBindAddr(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	for filename, wantCount := range apiBindCallSites {
		file, err := parser.ParseFile(
			fset, filename, nil, parser.SkipObjectResolution,
		)
		require.NoErrorf(t, err, "parsing %s", filename)

		found := 0
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok || lit.Type == nil {
				return true
			}
			typeName := types.ExprString(lit.Type)
			if !apiProviderDependencyTypes[typeName] {
				return true
			}
			found++
			position := fset.Position(lit.Pos())
			host, ok := compositeLitField(lit, "Host")
			require.Truef(
				t, ok,
				"%s at %s sets no Host field; the listener would bind "+
					"the empty wildcard address",
				typeName, position,
			)
			require.Equalf(
				t,
				"n.config.apiBindAddr",
				types.ExprString(host),
				"%s at %s must bind apiBindAddr; bindAddr is the public "+
					"relay/metrics address and defaults to a wildcard",
				typeName, position,
			)
			return true
		})
		require.Equalf(
			t,
			wantCount,
			found,
			"%s builds %d API provider dependency literals, want %d",
			filename, found, wantCount,
		)
	}
}

// compositeLitField returns the value assigned to name in a keyed
// composite literal.
func compositeLitField(
	lit *ast.CompositeLit,
	name string,
) (ast.Expr, bool) {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != name {
			continue
		}
		return kv.Value, true
	}
	return nil, false
}
