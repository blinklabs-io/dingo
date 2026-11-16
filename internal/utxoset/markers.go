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

package utxoset

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/ledger"
)

var (
	_ ledger.SyncMarkerReader = (*Set)(nil)
	_ database.PParamsSource  = (*Set)(nil)
)

// syncMarkerNames are the sync_state keys the set keeps, which the store
// names the same.
var syncMarkerNames = []string{
	database.SyntheticV2CostModelSyncKey,
	database.SyntheticV2CostModelClearedEpochSyncKey,
}

func syncMarkerWrites(lc ledger.LedgerChanges) []badgerutxo.MarkerWrite {
	out := make([]badgerutxo.MarkerWrite, 0, len(lc.SyncMarkers))
	for _, c := range lc.SyncMarkers {
		out = append(out, badgerutxo.MarkerWrite{
			Name: c.Name, Value: c.Value, Delete: c.Delete,
		})
	}
	return out
}

// ensureSyncMarkers loads the metadata store's protocol parameter sync
// markers into a set from before it kept them.
func (s *Set) ensureSyncMarkers(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.MarkersLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the sync marker marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	values := map[string]string{}
	for _, name := range syncMarkerNames {
		v, err := ldb.GetSyncState(name, txn)
		if err != nil {
			return fmt.Errorf("utxoset: read sync marker %s: %w", name, err)
		}
		if v != "" {
			values[name] = v
		}
	}
	if err := s.store.LoadMarkers(values); err != nil {
		return fmt.Errorf("utxoset: load sync markers: %w", err)
	}
	return nil
}

// SyncMarkersLoaded reports whether the set holds the protocol parameter
// sync markers.
func (s *Set) SyncMarkersLoaded() (bool, error) {
	return s.store.MarkersLoaded()
}

// SyncMarker returns the value of the protocol parameter sync marker name
// and whether it is set.
func (s *Set) SyncMarker(name string) (string, bool, error) {
	return s.store.Marker(name)
}
