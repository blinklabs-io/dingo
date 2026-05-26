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

package ledgerstate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMmapReadOnlyRejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, nil, 0o640); err != nil {
		t.Fatalf("writing empty file: %v", err)
	}

	data, cleanup, err := mmapReadOnly(path)
	if err == nil {
		if cleanup != nil {
			cleanup()
		}
		t.Fatalf("expected error, got data len %d", len(data))
	}
	if !strings.Contains(err.Error(), "empty file") {
		t.Fatalf("expected empty file error, got %v", err)
	}
}

func TestMmapReadOnlyReturnsFileData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	want := []byte("dingo mmap test")
	if err := os.WriteFile(path, want, 0o640); err != nil {
		t.Fatalf("writing data file: %v", err)
	}

	data, cleanup, err := mmapReadOnly(path)
	if err != nil {
		t.Fatalf("mmap read-only: %v", err)
	}
	defer cleanup()

	if !bytes.Equal(data, want) {
		t.Fatalf("mapped data = %q, want %q", data, want)
	}
}
