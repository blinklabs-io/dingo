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

package ledger

import (
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/models"
)

// ClosureUtxoEffect is a staked output a Leios closure transaction created or
// spent. The closure was applied at Applied, which journals and rolls the
// effect back, and its outputs and spends take effect at Effective, the
// closure's context slot, before it.
type ClosureUtxoEffect struct {
	Ref       models.UtxoId
	Applied   uint64
	Effective uint64
	Spent     bool
	// StakeKey is the key of the output's stake sum, CredentialStakeKey.
	StakeKey []byte
	Lovelace uint64
}

// ClosureUtxoEffectReader serves the closure output effects from a live UTxO
// set that keeps them.
type ClosureUtxoEffectReader interface {
	ClosureUtxoEffectsLoaded() (bool, error)
	// ClosureUtxoEffectsAfter calls fn with every effect applied after slot,
	// in applied order.
	ClosureUtxoEffectsAfter(
		slot uint64,
		fn func(ClosureUtxoEffect) error,
	) error
}

// AdjustStakeSumsEffective turns sums, the credential stake read from the
// outputs live at the applied slot, into the sums at slot as the store's stake
// queries count them (sqlstore/historical_stake.go:1080-1081, 1122-1123): an
// output counts when its creating transaction's effective slot is at or before
// slot (<=) and it is unspent or its spending transaction's effective slot is
// after slot (>). A closure applied after slot whose effective slot is at or
// before it creates outputs that count and spends outputs that no longer do;
// effects are the closure output effects applied after slot.
func AdjustStakeSumsEffective(
	sums map[string]uint64,
	effects []ClosureUtxoEffect,
	slot uint64,
) error {
	// An output a closure created after slot, effective after slot, is not
	// counted, so a spend of it takes nothing away.
	notCreated := map[string]struct{}{}
	for _, e := range effects {
		if e.Spent || e.Applied <= slot || e.Effective <= slot {
			continue
		}
		notCreated[outputKey(e.Ref)] = struct{}{}
	}
	for _, e := range effects {
		if e.Spent || e.Applied <= slot || e.Effective > slot {
			continue
		}
		if e.Lovelace > math.MaxUint64-sums[string(e.StakeKey)] {
			return fmt.Errorf("stake sum %x overflows", e.StakeKey)
		}
		sums[string(e.StakeKey)] += e.Lovelace
	}
	for _, e := range effects {
		if !e.Spent || e.Applied <= slot || e.Effective > slot {
			continue
		}
		if _, ok := notCreated[outputKey(e.Ref)]; ok {
			continue
		}
		if sums[string(e.StakeKey)] < e.Lovelace {
			return fmt.Errorf(
				"stake sum %x is below a spent closure output", e.StakeKey,
			)
		}
		sums[string(e.StakeKey)] -= e.Lovelace
		if sums[string(e.StakeKey)] == 0 {
			delete(sums, string(e.StakeKey))
		}
	}
	return nil
}

func outputKey(r models.UtxoId) string {
	return fmt.Sprintf("%x#%d", r.Hash, r.Idx)
}
