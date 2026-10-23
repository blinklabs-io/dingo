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
	"testing"

	"github.com/blinklabs-io/dingo/database"
)

// This file is compiled only into this package's tests, and exports to the
// external mithril_test package what a test there needs: it can import the
// packages that import mithril (internal/nodemithril), which a test inside
// package mithril cannot.

// TestAggregator is a v2 mock aggregator serving a certified artifact.
type TestAggregator struct {
	URL                    string
	Network                string
	GenesisVerificationKey string
}

// NewRealChunkTestAggregator serves immutable files 0 through last of the
// real ImmutableDB in dir as one certified v2 artifact, with a genuine STM
// certificate chain to a genesis key it reports.
func NewRealChunkTestAggregator(
	t *testing.T,
	dir string,
	last uint64,
) TestAggregator {
	t.Helper()
	f := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: last,
		realImmutableDir:    dir,
	})
	return TestAggregator{
		URL:                    f.server.URL,
		Network:                f.artifact.Network,
		GenesisVerificationKey: f.genesisVKey,
	}
}

// ImmutableImportMarkerForTest reads the stream catch-up's import marker.
func ImmutableImportMarkerForTest(
	db *database.Database,
) (uint64, bool, error) {
	return getImmutableImportMarker(db)
}
