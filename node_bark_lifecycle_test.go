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
	"os"
	"path/filepath"
	"testing"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestBarkLifecycleServiceCarriesTrustKey verifies the DatabaseService Bark
// mounts verifies manifests with the node's snapshot trust key. Without it,
// VerifySnapshot accepts a manifest the node itself would refuse to restore.
func TestBarkLifecycleServiceCarriesTrustKey(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "trust.key")
	require.NoError(
		t,
		os.WriteFile(keyFile, []byte("0123456789abcdef0123456789abcdef"), 0o600),
	)
	n := &Node{
		config: Config{
			databaseLifecycle: internalconfig.DatabaseLifecycleConfig{
				SnapshotTrustKeyFile: keyFile,
			},
		},
	}
	opts, err := n.barkLifecycleService().ManifestOptions()
	require.NoError(t, err)
	require.NotEmpty(t, opts, "bark lifecycle service has no manifest trust key")
}
