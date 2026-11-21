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

package badgerutxo_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// declaredKeys is every key in the keys.go declarations: the one-byte row
// family tags and the marker keys, each by its Go name.
type declaredKeys struct {
	tags    map[string]byte
	markers map[string][]byte
}

// keyDecls reads the key declarations from the source rather than from a
// hand-kept table, so a tag added to keys.go is checked without the test
// being edited. A tag is a const initialised with a character literal; a
// marker is a var initialised with []byte("...").
func keyDecls(t *testing.T, path string) declaredKeys {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err)
	out := declaredKeys{
		tags:    map[string]byte{},
		markers: map[string][]byte{},
	}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, v := range vs.Values {
				if i >= len(vs.Names) {
					break
				}
				name := vs.Names[i].Name
				switch gd.Tok {
				case token.CONST:
					lit, ok := v.(*ast.BasicLit)
					if !ok || lit.Kind != token.CHAR {
						continue
					}
					r, _, _, err := strconv.UnquoteChar(
						strings.Trim(lit.Value, "'"), '\'',
					)
					require.NoError(t, err, name)
					out.tags[name] = byte(r)
				case token.VAR:
					call, ok := v.(*ast.CallExpr)
					if !ok || len(call.Args) != 1 {
						continue
					}
					arr, ok := call.Fun.(*ast.ArrayType)
					if !ok || arr.Len != nil {
						continue
					}
					lit, ok := call.Args[0].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					s, err := strconv.Unquote(lit.Value)
					require.NoError(t, err, name)
					out.markers[name] = []byte(s)
				}
			}
		}
	}
	return out
}

// legacyMarkerPairs are the marker keys that are a prefix of a newer one on
// purpose: each is read by exact Get, never by prefix, and records a store
// written before the newer marker existed.
var legacyMarkerPairs = map[[2]string]bool{
	{"legacyNoncesKey", "noncesKey"}:                           true,
	{"legacyAccountsKey", "accountsKey"}:                       true,
	{"legacyEpochStateKey", "epochStateKey"}:                   true,
	{"legacyGovernanceKey", "governanceKey"}:                   true,
	{"legacyRewardBlocksKey", "rewardBlocksKey"}:               true,
	{"legacyOpCertsKey", "opCertsKey"}:                         true,
	{"legacyWitnessSlotsKey", "witnessSlotsKey"}:               true,
	{"legacyPoolHistoryKey", "poolHistoryKey"}:                 true,
	{"legacyRegistrationHistoryKey", "registrationHistoryKey"}: true,
}

// The store drops and iterates a family by its one-byte tag, and reads a
// marker by exact key, so every tag must be distinct and no key may be a
// prefix of another.
func TestKeyFamiliesDoNotOverlap(t *testing.T) {
	t.Parallel()
	d := keyDecls(t, "keys.go")
	require.NotEmpty(t, d.tags, "no row family tags found in keys.go")
	require.NotEmpty(t, d.markers, "no marker keys found in keys.go")

	// Two families sharing a first byte share every DropPrefix and
	// iteration range.
	byByte := map[byte]string{}
	for name, b := range d.tags {
		if other, dup := byByte[b]; dup {
			t.Errorf("%s and %s share the tag %q", name, other, b)
		}
		byByte[b] = name
	}

	// Every marker is a key of its own, so a tag equal to the marker
	// namespace's first byte would make its DropPrefix cover them.
	require.Contains(t, d.tags, "MarkerPrefix")
	for name, b := range d.tags {
		if name != "MarkerPrefix" && b == d.tags["MarkerPrefix"] {
			t.Errorf("%s tag %q is the marker prefix", name, b)
		}
	}
	for name, k := range d.markers {
		if name == "DormancyKey" {
			continue
		}
		if k[0] != d.tags["MarkerPrefix"] {
			t.Errorf(
				"marker %s %q does not start with the marker prefix",
				name,
				k,
			)
		}
	}

	// DormancyKey is a row of the dormancy family, so the family's tag
	// must be its first byte and no other family may claim it.
	require.Equal(t, d.tags["DormancyTag"], d.markers["DormancyKey"][0])

	// No key may be a prefix of another, except a family's own row and the
	// listed legacy markers: iterating or dropping the shorter key would
	// take in the longer one.
	type key struct {
		name string
		k    []byte
	}
	var keys []key
	for name, b := range d.tags {
		if name == "MarkerPrefix" {
			continue
		}
		keys = append(keys, key{name, []byte{b}})
	}
	for name, k := range d.markers {
		keys = append(keys, key{name, k})
	}
	for _, a := range keys {
		for _, b := range keys {
			if a.name == b.name || !bytes.HasPrefix(b.k, a.k) {
				continue
			}
			if a.name == "DormancyTag" && b.name == "DormancyKey" {
				continue
			}
			if legacyMarkerPairs[[2]string{a.name, b.name}] {
				continue
			}
			t.Errorf("%s %q is a prefix of %s %q", a.name, a.k, b.name, b.k)
		}
	}
}

// A tag or marker declared outside keys.go escapes the checks above, which
// is how two branches took the same tag.
func TestKeysDeclaredOnlyInKeysFile(t *testing.T) {
	t.Parallel()
	dirs := []string{
		".",
		filepath.Join("..", "..", "..", "internal", "utxoset"),
	}
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)
		require.NotEmpty(t, files)
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") ||
				filepath.Base(f) == "keys.go" {
				continue
			}
			d := keyDecls(t, f)
			for name := range d.tags {
				t.Errorf("%s declares the row tag %s outside keys.go", f, name)
			}
			for name := range d.markers {
				t.Errorf("%s declares the key %s outside keys.go", f, name)
			}
		}
	}
}
