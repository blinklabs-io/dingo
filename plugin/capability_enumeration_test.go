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

package plugin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"testing"
)

// TestAllCapabilitiesCoversEveryDeclaredConstant reads this package's own
// source for Capability constants and requires each one to appear in
// allCapabilities.
//
// Go cannot enumerate the constants of a named string type at run time, so a
// constant added to the block without being added to the slice would be
// rejected by Valid, silently disabled at registration, and absent from every
// caller that iterates AllCapabilities. This is the check that catches it.
func TestAllCapabilitiesCoversEveryDeclaredConstant(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, 0)
	if err != nil {
		t.Fatalf("parse package source: %v", err)
	}
	pkg, ok := pkgs["plugin"]
	if !ok {
		t.Fatal("plugin package source not found")
	}

	declared := map[string]Capability{}
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				ident, ok := value.Type.(*ast.Ident)
				if !ok || ident.Name != "Capability" {
					continue
				}
				for i, name := range value.Names {
					if i >= len(value.Values) {
						continue
					}
					lit, ok := value.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					unquoted, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatalf("unquote %s: %v", name.Name, err)
					}
					declared[name.Name] = Capability(unquoted)
				}
			}
		}
	}

	if len(declared) == 0 {
		t.Fatal("no Capability constants found: the source scan is broken")
	}
	for name, capability := range declared {
		if !slices.Contains(allCapabilities, capability) {
			t.Errorf(
				"%s is declared but missing from allCapabilities, so Valid "+
					"rejects it and no caller iterating AllCapabilities can "+
					"reach it",
				name,
			)
		}
	}
	if len(allCapabilities) != len(declared) {
		t.Errorf(
			"allCapabilities has %d entries for %d declared constants",
			len(allCapabilities),
			len(declared),
		)
	}
}
