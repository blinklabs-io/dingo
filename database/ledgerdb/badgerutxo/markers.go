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

package badgerutxo

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger/v4"
)

// The protocol parameter sync markers, named as the metadata store's
// sync_state keys they replace.
const (
	// SyntheticV2Marker is "true" while the PlutusV2 cost model in force is
	// the fabricated default, "false" once real data replaced it.
	SyntheticV2Marker = "synthetic_v2_cost_model"
	// SyntheticV2ClearedMarker is the epoch, in decimal, at which real
	// PlutusV2 cost model data was first confirmed.
	SyntheticV2ClearedMarker = "synthetic_v2_cost_model_cleared_epoch"
)

// MarkerWrite sets the marker Name to Value, or deletes it when Delete is
// set.
type MarkerWrite struct {
	Name   string
	Value  string
	Delete bool
}

func markerKey(name string) ([]byte, error) {
	switch name {
	case SyntheticV2Marker:
		return syntheticV2Key, nil
	case SyntheticV2ClearedMarker:
		return syntheticV2ClearedKey, nil
	default:
		return nil, fmt.Errorf("badgerutxo: unknown sync marker %q", name)
	}
}

func putMarkers(txn *badger.Txn, writes []MarkerWrite) error {
	for _, w := range writes {
		k, err := markerKey(w.Name)
		if err != nil {
			return err
		}
		if w.Delete {
			err = txn.Delete(k)
		} else {
			err = txn.Set(k, []byte(w.Value))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// revertSyntheticV2After drops both synthetic PlutusV2 markers when a
// rollback leaves an epoch, newest, before the one real data was confirmed
// at, as the metadata store's rollback does. A rollback that leaves no epoch
// row keeps them: no epoch maps its slot.
func revertSyntheticV2After(
	txn *badger.Txn,
	newest uint64,
	found bool,
) error {
	if !found {
		return nil
	}
	item, err := txn.Get(syntheticV2ClearedKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}
	cleared, err := strconv.ParseUint(string(v), 10, 64)
	if err != nil {
		return fmt.Errorf(
			"badgerutxo: malformed synthetic marker %q: %w",
			v,
			err,
		)
	}
	if cleared <= newest {
		return nil
	}
	if err := txn.Delete(syntheticV2ClearedKey); err != nil {
		return err
	}
	return txn.Delete(syntheticV2Key)
}

// Marker returns the value of the sync marker name and whether it is set.
func (s *Store) Marker(name string) (string, bool, error) {
	k, err := markerKey(name)
	if err != nil {
		return "", false, err
	}
	var out string
	var ok bool
	err = s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		out, ok = string(v), err == nil
		return err
	})
	return out, ok, err
}

// LoadMarkers replaces the store's sync markers with values, by name, and
// marks them loaded, for a store from before it kept them.
func (s *Store) LoadMarkers(values map[string]string) error {
	writes := make([]MarkerWrite, 0, len(values))
	for name, v := range values {
		writes = append(writes, MarkerWrite{Name: name, Value: v})
	}
	return s.update(func(txn *badger.Txn) error {
		for _, k := range [][]byte{syntheticV2Key, syntheticV2ClearedKey} {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		if err := putMarkers(txn, writes); err != nil {
			return err
		}
		return txn.Set(syncMarkersKey, []byte{1})
	})
}

// MarkersLoaded reports whether the store holds the sync markers up to its
// tip.
func (s *Store) MarkersLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(syncMarkersKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}
