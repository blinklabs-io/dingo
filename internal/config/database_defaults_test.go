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

package config

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
)

// TestDatabaseDefaultsMatchInternalConfigDefaults guards against drift
// between the two independent plugin-default sources of truth:
// internal/config.DefaultBlobPlugin/DefaultMetadataPlugin (used to
// resolve config for the CLI path before database.New is ever called)
// and database.DefaultConfig (used by database.New's own defaulting,
// which only matters for direct callers -- library usage and tests --
// that construct a database.Config without going through internal/config
// at all).
func TestDatabaseDefaultsMatchInternalConfigDefaults(t *testing.T) {
	if database.DefaultConfig.BlobPlugin != DefaultBlobPlugin {
		t.Errorf(
			"database.DefaultConfig.BlobPlugin = %q, want %q (internal/config.DefaultBlobPlugin)",
			database.DefaultConfig.BlobPlugin,
			DefaultBlobPlugin,
		)
	}
	if database.DefaultConfig.MetadataPlugin != DefaultMetadataPlugin {
		t.Errorf(
			"database.DefaultConfig.MetadataPlugin = %q, want %q (internal/config.DefaultMetadataPlugin)",
			database.DefaultConfig.MetadataPlugin,
			DefaultMetadataPlugin,
		)
	}
}
