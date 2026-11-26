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

package indexer

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/factledger"
	"github.com/stretchr/testify/require"
)

type storeLedger = factledger.Ledger

func newStoreLedger(t *testing.T, db *database.Database) storeLedger {
	t.Helper()
	return factledger.New(t, db)
}

// newStoreFacts copies the facts seeded in src into dst.
func newStoreFacts(t *testing.T, src, dst *database.Database) *MetadataFacts {
	t.Helper()
	f, err := NewMetadataFacts(newStoreLedger(t, src), dst)
	require.NoError(t, err)
	return f
}
