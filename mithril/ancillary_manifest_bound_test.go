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

package mithril

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReadFileInRejectsOversizedManifest pins the bound on a manifest read out
// of an extracted snapshot. The archive is remote input, so an unbounded read
// lets its producer choose the allocation.
func TestReadFileInRejectsOversizedManifest(t *testing.T) {
	dir := t.TempDir()
	name := "ancillary_manifest.json"
	if err := os.WriteFile(
		filepath.Join(dir, name),
		make([]byte, maxAncillaryManifestBytes+1),
		0o600,
	); err != nil {
		t.Fatalf("write oversized manifest: %v", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	if _, err := readFileIn(root, name); err == nil {
		t.Fatal("expected an oversized manifest to be rejected")
	} else if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected a size error, got %v", err)
	}
}

// TestReadFileInAcceptsManifestAtBound pins that the bound is inclusive, so a
// manifest exactly at the limit is still readable.
func TestReadFileInAcceptsManifestAtBound(t *testing.T) {
	dir := t.TempDir()
	name := "ancillary_manifest.json"
	want := make([]byte, maxAncillaryManifestBytes)
	if err := os.WriteFile(filepath.Join(dir, name), want, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	got, err := readFileIn(root, name)
	if err != nil {
		t.Fatalf("manifest at the bound must be readable: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
}
