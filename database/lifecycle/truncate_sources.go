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

package lifecycle

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
)

// ErrTruncateNeedsUtxoSet is returned by Truncate of a database with no
// metadata store when no live UTxO set that answers the rollback's reads was
// opened.
var ErrTruncateNeedsUtxoSet = errors.New(
	"a truncate of the ledger's database needs its live UTxO set",
)

// frozenEraSwitch serves the era switch as it was before the truncate rolled
// the set back: the set's rollback reverts and drops its own record before the
// database's rollback reads it, and the database still has to return the
// switched epoch's SQL row to its source era.
type frozenEraSwitch struct {
	database.EpochSource
	sw *database.MidEpochEraSwitch
}

func (f frozenEraSwitch) EraSwitch() (*database.MidEpochEraSwitch, error) {
	return f.sw, nil
}

func bindEpochSource(db *database.Database, set TruncateUtxoSet) error {
	src, ok := set.(database.EpochSource)
	if !ok {
		return fmt.Errorf(
			"%w: the set does not serve epochs or the era switch",
			ErrTruncateNeedsUtxoSet,
		)
	}
	sw, err := src.EraSwitch()
	if err != nil {
		return err
	}
	db.SetEpochSource(frozenEraSwitch{EpochSource: src, sw: sw})
	return nil
}

// bindUtxoSetSources wires the live UTxO set into the reads TruncateAfterSlot
// makes of a database with no metadata store: the target's nonce and the
// checkpoint check, the epochs and the era switch, and the protocol
// parameters. A database with a metadata store answers them itself.
func (c truncateConfig) bindUtxoSetSources(db *database.Database) error {
	if db.Metadata() != nil {
		return nil
	}
	if c.utxoSet == nil {
		return fmt.Errorf(
			"%w: no live UTxO set was opened", ErrTruncateNeedsUtxoSet,
		)
	}
	nonces, ok := c.utxoSet.(database.NonceSource)
	if !ok {
		return fmt.Errorf(
			"%w: the set does not serve nonces", ErrTruncateNeedsUtxoSet,
		)
	}
	db.SetNonceSource(nonces)
	if err := bindEpochSource(db, c.utxoSet); err != nil {
		return err
	}
	pparams, ok := c.utxoSet.(database.PParamsSource)
	if !ok {
		return fmt.Errorf(
			"%w: the set does not serve protocol parameters",
			ErrTruncateNeedsUtxoSet,
		)
	}
	db.SetPParamsSource(pparams)
	if _, ok := c.utxoSet.(TruncateTxIndex); !ok {
		return fmt.Errorf(
			"%w: the set does not index transactions",
			ErrTruncateNeedsUtxoSet,
		)
	}
	return nil
}
