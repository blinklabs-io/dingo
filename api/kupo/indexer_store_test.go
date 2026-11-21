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

package kupo

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/stretchr/testify/require"
)

type fakeTipState struct{ tip string }

func (f *fakeTipState) GetSyncState(string, *database.Txn) (string, error) {
	return f.tip, nil
}

func (f *fakeTipState) SetSyncState(
	string,
	string,
	*database.Txn,
) error {
	return nil
}

func (f *fakeTipState) DeleteSyncState(
	string,
	*database.Txn,
) error {
	return nil
}

func (f *fakeTipState) ListSyncStateKeysByPrefix(
	string,
	*database.Txn,
) ([]string, error) {
	return nil, nil
}

func TestAdapterReadsFromIndexerStore(t *testing.T) {
	t.Parallel()
	idxDB := &database.Database{}
	store, err := indexerdb.NewStore(idxDB, &fakeTipState{tip: "42:7:abcd"}, 10)
	require.NoError(t, err)

	a := &NodeAdapter{indexer: store}
	require.Same(t, idxDB, a.db())

	point, err := a.Tip()
	require.NoError(t, err)
	require.Equal(t, Point{SlotNo: 42, HeaderHash: "abcd"}, point)
}
