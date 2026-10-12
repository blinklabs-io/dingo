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

package dblifecycle

import (
	"bytes"
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/config"
)

// minTrustKeyBytes is the shortest shared secret accepted as a manifest trust
// key.
const minTrustKeyBytes = 16

// ManifestOptions returns the manifest options cfg selects: a trust key read
// from SnapshotTrustKeyFile, if one is configured. The file is read on every
// call so a rotated secret takes effect without a restart. A configured file
// that cannot be read, or holds a short secret, is an error rather than a
// silent fall back to unauthenticated manifests.
func ManifestOptions(
	cfg config.DatabaseLifecycleConfig,
) ([]lifecycle.ManifestOption, error) {
	if cfg.SnapshotTrustKeyFile == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(cfg.SnapshotTrustKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read snapshot trust key file: %w", err)
	}
	key := bytes.TrimSpace(raw)
	if len(key) < minTrustKeyBytes {
		return nil, fmt.Errorf(
			"snapshot trust key file holds %d bytes, need at least %d",
			len(key), minTrustKeyBytes,
		)
	}
	return []lifecycle.ManifestOption{lifecycle.WithManifestKey(key)}, nil
}
