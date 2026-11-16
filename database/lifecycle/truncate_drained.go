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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// ErrDrainedFamilyUnserved is returned by Truncate when a ledger-state family
// is drained from the metadata store and the truncate has no live UTxO set
// that can answer the reads the rollback makes of it.
var ErrDrainedFamilyUnserved = errors.New(
	"a drained ledger-state family has no live UTxO set to serve a truncate",
)

// drainedTruncateReads says, per drained family, how Truncate gets the reads
// TruncateAfterSlot makes of it from the live UTxO set. A family is listed
// only once its rollback reads are handled; Truncate refuses a drained
// family that is not listed.
var drainedTruncateReads = map[nodesettings.Family]func(
	db *database.Database,
	set TruncateUtxoSet,
) error{
	// The rollback reads the target's nonce and the checkpoint check.
	nodesettings.FamilyNonces: func(
		db *database.Database,
		set TruncateUtxoSet,
	) error {
		src, ok := set.(database.NonceSource)
		if !ok {
			return fmt.Errorf(
				"%w: the set does not serve nonces", ErrDrainedFamilyUnserved,
			)
		}
		db.SetNonceSource(src)
		return nil
	},
	// The rollback reads the newest epoch it leaves, and the recorded era
	// switch, and the database's own epoch reads need the same source.
	nodesettings.FamilyEpochs:    bindEpochSource,
	nodesettings.FamilyEraSwitch: bindEpochSource,
	// The rollback only skips this family's deletes.
	nodesettings.FamilySummaries: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback refreshes no live stake of a drained family.
	nodesettings.FamilyLiveStake: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips this family's deletes, in the certificate
	// rollback as well as the committee and constitution deletes.
	nodesettings.FamilyCommitteeConstitution: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips this family's rows and the pool column that
	// mirrors them.
	nodesettings.FamilyOpCerts: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips this family's deletes; the set drops its rows
	// in the same rollback.
	nodesettings.FamilyGovernance: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback skips this family's deletes, and the set reverts its own
	// markers; the database's protocol parameter reads need the source.
	nodesettings.FamilyPParamsUpdates: func(
		db *database.Database,
		set TruncateUtxoSet,
	) error {
		src, ok := set.(database.PParamsSource)
		if !ok {
			return fmt.Errorf(
				"%w: the set does not serve protocol parameters",
				ErrDrainedFamilyUnserved,
			)
		}
		db.SetPParamsSource(src)
		return nil
	},
	// The rollback only skips this family's deletes and reads nothing from
	// it.
	nodesettings.FamilyPoolsHistory: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips this family's deletes.
	nodesettings.FamilyNetworkDonations: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips these families' restores and deletes: the
	// set's own rollback restores each row from its journal.
	nodesettings.FamilyAccounts: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	nodesettings.FamilyDReps: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips the genesis delegation delete.
	nodesettings.FamilyMIRGenesis: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips this family's deletes, and the set's own
	// rollback restores its counts and fees by slot.
	// The rollback only skips this family's deletes, and the set's own
	// rollback restores its rows by slot.
	nodesettings.FamilyRewardInputs: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	nodesettings.FamilyRewardBlocksFees: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
	// The rollback only skips this family's deletes, and the set's own
	// rollback drops the outputs of an epoch captured after the target.
	nodesettings.FamilyRewardOutputs: func(
		*database.Database,
		TruncateUtxoSet,
	) error {
		return nil
	},
}

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
			ErrDrainedFamilyUnserved,
		)
	}
	sw, err := src.EraSwitch()
	if err != nil {
		return err
	}
	db.SetEpochSource(frozenEraSwitch{EpochSource: src, sw: sw})
	return nil
}

// bindDrainedFamilies reloads the latch from db and, for every drained
// family, wires the set into the reads TruncateAfterSlot makes of it. A
// freshly opened database has not read the latch, so without this a truncate
// of a drained database would look for rows the drain deleted.
func (c truncateConfig) bindDrainedFamilies(
	ctx context.Context,
	db *database.Database,
) error {
	mask, err := db.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	for _, family := range mask.Families() {
		if c.utxoSet == nil {
			return fmt.Errorf(
				"%w: family %s, and no live UTxO set was opened",
				ErrDrainedFamilyUnserved, family,
			)
		}
		bind, ok := drainedTruncateReads[family]
		if !ok {
			return fmt.Errorf(
				"%w: truncate does not read family %s from the set",
				ErrDrainedFamilyUnserved, family,
			)
		}
		if err := bind(db, c.utxoSet); err != nil {
			return fmt.Errorf("family %s: %w", family, err)
		}
	}
	return nil
}
