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
	"fmt"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DirFor is the live UTxO set's directory under a node's data directory.
func DirFor(dataDir string) string {
	return filepath.Join(dataDir, "ledger", "utxo")
}

// LoadUtxoSet replaces the set with the outputs feed puts, as of point. It
// implements ledgerstate.UtxoSetLoader for a ledger-state import.
func (s *Set) LoadUtxoSet(
	point ocommon.Point,
	feed func(put func(txID []byte, index uint32, cbor []byte) error) error,
) error {
	err := s.store.Bootstrap(point,
		func(put func(badgerutxo.Output) error) error {
			return feed(func(txID []byte, index uint32, cbor []byte) error {
				return put(badgerutxo.Output{
					Ref:  badgerutxo.NewRef(txID, index),
					Cbor: cbor,
				})
			})
		},
	)
	if err != nil {
		return fmt.Errorf("utxoset: load: %w", err)
	}
	n, _ := s.store.Count()
	s.logger.Info("loaded the live UTxO set", "live_outputs", n, "slot", point.Slot)
	return nil
}

// LiveUtxoCbor implements types.LiveUtxoSource over the set's committed
// state, for a metadata store with no ledger staging changes over it.
func (s *Set) LiveUtxoCbor(_ types.Txn, txID []byte, index uint32) ([]byte, bool, error) {
	return s.Get(txID, index)
}

// LiveUtxoStake implements types.LiveUtxoSource over the set's committed
// state.
func (s *Set) LiveUtxoStake(_ types.Txn, tag uint8, hash []byte) (uint64, error) {
	return s.CredentialStake(tag, hash)
}

// EachLiveUtxoStake implements types.LiveUtxoSource over the set's committed
// state.
func (s *Set) EachLiveUtxoStake(
	_ types.Txn,
	fn func(tag uint8, hash []byte, lovelace uint64) error,
) error {
	return s.EachCredentialStake(func(tag uint8, hash []byte, n uint64) error {
		if n == 0 {
			return nil
		}
		return fn(tag, hash, n)
	})
}

// LivePointerStakes implements types.LiveUtxoSource over the set's committed
// state.
func (s *Set) LivePointerStakes(types.Txn) ([]types.PointerStake, error) {
	var ret []types.PointerStake
	err := s.store.EachSum(func(key []byte, n uint64) error {
		slot, txIndex, certIndex, ok := ledger.ParsePointerStakeKey(key)
		if !ok || n == 0 {
			return nil
		}
		ret = append(ret, types.PointerStake{
			Slot: slot, TxIndex: txIndex, CertIndex: certIndex, Lovelace: n,
		})
		return nil
	})
	return ret, err
}
