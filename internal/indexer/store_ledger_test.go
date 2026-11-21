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
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// storeLedger stands in for the ledger in tests of the copy: it serves the
// fact rows, the pending record and the reward cursors a test seeded in db,
// and no state at an anchor.
type storeLedger struct {
	metadata.FactRowReader
	db *database.Database
}

func newStoreLedger(t *testing.T, db *database.Database) storeLedger {
	t.Helper()
	rows, ok := db.Metadata().(metadata.FactRowReader)
	require.True(t, ok)
	return storeLedger{FactRowReader: rows, db: db}
}

func (s storeLedger) RewardStakeInputsPendingState() (string, error) {
	return s.db.Metadata().GetSyncState(
		models.RewardStakeInputsPendingKey, nil,
	)
}

func (s storeLedger) RewardPrecomputeCursors() (map[uint64]string, error) {
	keys, err := s.db.ListSyncStateKeysByPrefix(
		models.RewardPrecomputeCursorPrefix, nil,
	)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]string, len(keys))
	for _, k := range keys {
		epoch, err := strconv.ParseUint(
			strings.TrimPrefix(k, models.RewardPrecomputeCursorPrefix), 10, 64,
		)
		if err != nil {
			continue
		}
		if out[epoch], err = s.db.Metadata().GetSyncState(k, nil); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (storeLedger) AnchorOpCertCounters(
	context.Context, uint64,
) ([]metadata.OpCertCounter, error) {
	return nil, nil
}

func (storeLedger) AnchorStateReady(context.Context) error { return nil }

func (storeLedger) AnchorImportedAccounts(
	context.Context, uint64, []byte, int,
) ([]metadata.ImportedAccount, []byte, error) {
	return nil, nil, nil
}

func (storeLedger) EachAnchorRewardJournal(
	context.Context, uint64, func(metadata.RewardJournalEntry) error,
) error {
	return nil
}

func (storeLedger) AnchorUtxoDigest(
	context.Context, uint64,
) (metadata.UtxoSetDigest, error) {
	return metadata.UtxoSetDigest{}, nil
}

// newStoreFacts copies the facts seeded in src into dst.
func newStoreFacts(t *testing.T, src, dst *database.Database) *MetadataFacts {
	t.Helper()
	f, err := NewMetadataFacts(newStoreLedger(t, src), dst)
	require.NoError(t, err)
	return f
}
