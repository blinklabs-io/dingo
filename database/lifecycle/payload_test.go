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

package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestVerifyPayloadsRespectsContextCancellation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	blob := []byte("blob")
	metadata := []byte("metadata")
	if err := os.WriteFile(filepath.Join(dir, BlobBackupFileName), blob, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, MetadataBackupFileName), metadata, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	manifest := Manifest{
		BlobBytes:      int64(len(blob)),
		BlobSHA256:     "declared-digest",
		MetadataBytes:  int64(len(metadata)),
		MetadataSHA256: "declared-digest",
	}
	if err := manifest.verifyPayloads(ctx, dir, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("verifyPayloads() error = %v, want context.Canceled", err)
	}
}

// v094Manifest is the manifest v0.94.2 writes, field for field: its checksum
// covers this encoding.
type v094Manifest struct {
	FormatVersion   int               `json:"formatVersion"`
	ID              string            `json:"id,omitempty"`
	CreatedAt       time.Time         `json:"createdAt"`
	Trigger         string            `json:"trigger"`
	Name            string            `json:"name,omitempty"`
	Description     string            `json:"description,omitempty"`
	StorageMode     string            `json:"storageMode"`
	Network         string            `json:"network"`
	CommitTimestamp int64             `json:"commitTimestamp"`
	TipSlot         uint64            `json:"tipSlot"`
	TipHash         []byte            `json:"tipHash"`
	TipBlockNumber  uint64            `json:"tipBlockNumber"`
	BlobPlugin      string            `json:"blobPlugin"`
	MetadataPlugin  string            `json:"metadataPlugin"`
	Gates           map[string]string `json:"gates,omitempty"`
	DingoVersion    string            `json:"dingoVersion"`
	BlobBytes       int64             `json:"blobBytes"`
	MetadataBytes   int64             `json:"metadataBytes"`
	BlobSHA256      string            `json:"blobSha256,omitempty"`
	MetadataSHA256  string            `json:"metadataSha256,omitempty"`
	Authentication  string            `json:"authentication,omitempty"`
	Checksum        string            `json:"checksum"`
}

func writePayload(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// A snapshot of the ledger's database carries no metadata backup, and its
// payload verifies without one. A v0.94.2 snapshot always carries one, and
// its payload does not verify without it.
func TestVerifyPayloadsNeedsAMetadataBackupOnlyWhereDeclared(t *testing.T) {
	t.Parallel()
	blob := []byte("blob")
	metadata := []byte("metadata")

	ledgerDir := t.TempDir()
	ledger := Manifest{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		BlobBytes:      int64(len(blob)),
		BlobSHA256:     writePayload(t, ledgerDir, BlobBackupFileName, blob),
	}
	if err := ledger.verifyPayloads(t.Context(), ledgerDir, true); err != nil {
		t.Fatalf("ledger snapshot payload: %v", err)
	}

	releasedDir := t.TempDir()
	released := v094Manifest{
		FormatVersion:  2,
		ID:             "snap",
		Trigger:        TriggerManual,
		StorageMode:    "core",
		Network:        "preview",
		TipSlot:        7,
		TipHash:        []byte{1, 2},
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		Gates:          map[string]string{"network": "preview"},
		DingoVersion:   "v0.94.2",
		BlobBytes:      int64(len(blob)),
		MetadataBytes:  int64(len(metadata)),
		BlobSHA256:     writePayload(t, releasedDir, BlobBackupFileName, blob),
		MetadataSHA256: writePayload(
			t, releasedDir, MetadataBackupFileName, metadata,
		),
	}
	encoded, err := json.Marshal(released)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(encoded)
	released.Checksum = hex.EncodeToString(sum[:])
	if encoded, err = json.Marshal(released); err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseManifest(encoded)
	if err != nil {
		t.Fatalf("ParseManifest(v0.94.2 manifest): %v", err)
	}
	if err := parsed.verifyPayloads(t.Context(), releasedDir, true); err != nil {
		t.Fatalf("v0.94.2 snapshot payload: %v", err)
	}
	if err := os.Remove(filepath.Join(releasedDir, MetadataBackupFileName)); err != nil {
		t.Fatal(err)
	}
	if err := parsed.verifyPayloads(t.Context(), releasedDir, true); err == nil {
		t.Fatal("a v0.94.2 snapshot payload verified without its metadata backup")
	}
}
