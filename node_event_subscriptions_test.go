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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeEventSubscriptionPoliciesAreExplicit(t *testing.T) {
	t.Parallel()

	type counts struct {
		required               int
		detachable             int
		chainsync              int
		connectionRecycle      int
		ledgerRecycleTranslate int
	}
	want := map[string]map[string]counts{
		"node.go": {
			"Run": {
				required:  3,
				chainsync: 1,
			},
			"subscribeChainsyncClientRemoveRequests": {required: 1},
			"subscribeConnectionEvents": {
				required:               3,
				connectionRecycle:      1,
				ledgerRecycleTranslate: 1,
			},
			"subscribeChainSelectorEvents": {
				required:   8,
				detachable: 1,
			},
		},
		"node_lifecycle.go": {
			"reinitializeNetworkingCore": {
				chainsync:         1,
				connectionRecycle: 1,
			},
		},
		"node_leios.go": {
			"initLeiosVoteManager": {required: 2},
		},
		"node_koiosparity.go": {
			"startKoiosParityObserver": {required: 1},
		},
	}

	policyHelpers := map[string]string{
		"subscribeRequiredEvent":                      "SubscriberBackpressureBlock",
		"subscribeDetachableEvent":                    "SubscriberBackpressureDetach",
		"subscribeConnectionRecycleRequests":          "SubscriberBackpressureBlock",
		"subscribeLedgerConnectionRecycleTranslation": "SubscriberBackpressureBlock",
	}
	seenPolicyHelpers := make(map[string]bool)
	for fileName, functions := range want {
		file, err := parser.ParseFile(
			token.NewFileSet(),
			filepath.Clean(fileName),
			nil,
			parser.SkipObjectResolution,
		)
		require.NoError(t, err)

		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			wantCounts, isRegistration := functions[fn.Name.Name]
			got := counts{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := nodeCallName(call.Fun)
				switch name {
				case "subscribeRequiredEvent":
					got.required++
				case "subscribeDetachableEvent":
					got.detachable++
				case "subscribeChainsyncClientRemoveRequests":
					got.chainsync++
				case "subscribeConnectionRecycleRequests":
					got.connectionRecycle++
				case "subscribeLedgerConnectionRecycleTranslation":
					got.ledgerRecycleTranslate++
				case "SubscribeFunc", "SubscribeFuncWithBuffer", "SubscribeFuncStrict":
					t.Errorf("%s uses unclassified EventBus registration %s", fn.Name.Name, name)
				case "SubscribeFuncWithBufferPolicy":
					policy, allowed := policyHelpers[fn.Name.Name]
					if !allowed {
						t.Errorf("%s registers an EventBus callback outside a policy helper", fn.Name.Name)
						return true
					}
					seenPolicyHelpers[fn.Name.Name] = true
					if len(call.Args) != 4 || nodeCallName(call.Args[2]) != policy {
						t.Errorf("%s must register with %s", fn.Name.Name, policy)
					}
				}
				return true
			})
			if isRegistration {
				require.Equalf(t, wantCounts, got,
					"subscription classification changed in %s:%s", fileName, fn.Name.Name)
			}
		}
	}
	for helper := range policyHelpers {
		require.Truef(t, seenPolicyHelpers[helper],
			"missing explicit policy implementation %s", helper)
	}
}

func nodeCallName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	default:
		return ""
	}
}
