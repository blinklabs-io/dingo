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
	"errors"
	"os"
	"path/filepath"
	"testing"
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
