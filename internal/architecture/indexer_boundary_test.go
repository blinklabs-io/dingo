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

package architecture_test

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestIndexerWriterDoesNotReachTheLedger keeps the IndexerDB writer off the
// ledger and its stores. The writer derives every fact it holds by replaying
// blocks through the epoch engine over its own rows, so nothing in the ledger
// is retained, awaited or pruned on its behalf.
func TestIndexerWriterDoesNotReachTheLedger(t *testing.T) {
	t.Parallel()

	forbidden := []string{
		modulePath + "/ledger",
		modulePath + "/internal/utxoset",
		modulePath + "/internal/nodestorage",
		modulePath + "/database/ledgerdb/badgerutxo",
	}
	dir := filepath.Join(findRepoRoot(t), "internal", "indexer")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no indexer sources found")
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(
			token.NewFileSet(), file, nil, parser.ImportsOnly,
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			for _, bad := range forbidden {
				if path == bad {
					t.Errorf(
						"%s imports %s",
						filepath.Base(file), path,
					)
				}
			}
		}
	}
}
