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

package models

import (
	"fmt"
	"log/slog"

	"gorm.io/gorm"
)

// orphanPurgeSpec describes a cascade foreign key whose orphaned child rows
// must be removed before AutoMigrate can add the constraint.
type orphanPurgeSpec struct {
	// child is a pointer to the child model (the DELETE target). Its table
	// holds the foreign key column.
	child any
	// parent is a pointer to the parent model referenced by the foreign key.
	parent any
	// relOwner is a pointer to the model that declares the has-many/has-one
	// relationship (the one carrying the constraint:OnDelete:CASCADE tag).
	relOwner any
	// relField is the relationship field name on relOwner, used to detect
	// whether the foreign key constraint already exists.
	relField string
	// fkColumn is the foreign key column on the child table.
	fkColumn string
	// nullable is true when fkColumn may be NULL (an unset optional
	// association), in which case NULL rows are valid and must be kept.
	nullable bool
	// label names the child table for logging and error messages.
	label string
}

// orphanPurgeSpecs lists every OnDelete:CASCADE relationship declared across
// the models, ordered so that parents are purged before their children. This
// ordering matters for multi-level chains: deleting an orphaned utxo turns its
// asset rows into orphans, so the asset purge must run after the utxo purge;
// likewise pool_registration is purged before its owners and relays.
func orphanPurgeSpecs() []orphanPurgeSpec {
	return []orphanPurgeSpec{
		// transaction -> children
		{
			child: &Utxo{}, parent: &Transaction{},
			relOwner: &Transaction{}, relField: "Outputs",
			fkColumn: "transaction_id", nullable: true, label: "utxo",
		},
		{
			child: &Utxo{}, parent: &Transaction{},
			relOwner: &Transaction{}, relField: "CollateralReturn",
			fkColumn: "collateral_return_for_tx_id", nullable: true, label: "utxo",
		},
		{
			child: &PlutusData{}, parent: &Transaction{},
			relOwner: &Transaction{}, relField: "PlutusData",
			fkColumn: "transaction_id", label: "plutus_data",
		},
		{
			child: &Certificate{}, parent: &Transaction{},
			relOwner: &Transaction{}, relField: "Certificates",
			fkColumn: "transaction_id", label: "certs",
		},
		{
			child: &KeyWitness{}, parent: &Transaction{},
			relOwner: &Transaction{}, relField: "KeyWitnesses",
			fkColumn: "transaction_id", label: "key_witness",
		},
		{
			child: &WitnessScripts{}, parent: &Transaction{},
			relOwner: &Transaction{}, relField: "WitnessScripts",
			fkColumn: "transaction_id", label: "witness_scripts",
		},
		{
			child: &Redeemer{}, parent: &Transaction{},
			relOwner: &Transaction{}, relField: "Redeemers",
			fkColumn: "transaction_id", label: "redeemer",
		},
		// utxo -> asset (must run after the utxo purges above)
		{
			child: &Asset{}, parent: &Utxo{},
			relOwner: &Utxo{}, relField: "Assets",
			fkColumn: "utxo_id", label: "asset",
		},
		// pool -> registration/retirement
		{
			child: &PoolRegistration{}, parent: &Pool{},
			relOwner: &Pool{}, relField: "Registration",
			fkColumn: "pool_id", label: "pool_registration",
		},
		{
			child: &PoolRetirement{}, parent: &Pool{},
			relOwner: &Pool{}, relField: "Retirement",
			fkColumn: "pool_id", label: "pool_retirement",
		},
		// pool_registration -> owners/relays (after the registration purge)
		{
			child: &PoolRegistrationOwner{}, parent: &PoolRegistration{},
			relOwner: &PoolRegistration{}, relField: "Owners",
			fkColumn: "pool_registration_id", label: "pool_registration_owner",
		},
		{
			child: &PoolRegistrationRelay{}, parent: &PoolRegistration{},
			relOwner: &PoolRegistration{}, relField: "Relays",
			fkColumn: "pool_registration_id", label: "pool_registration_relay",
		},
		// move_instantaneous_rewards -> reward
		{
			child: &MoveInstantaneousRewardsReward{}, parent: &MoveInstantaneousRewards{},
			relOwner: &MoveInstantaneousRewards{}, relField: "Rewards",
			fkColumn: "mir_id", label: "move_instantaneous_rewards_reward",
		},
	}
}

// PurgeOrphanedCascadeRows removes child rows whose OnDelete:CASCADE parent no
// longer exists. Databases created before sqlite auto-migrate was enabled
// (#2686) never had these foreign keys, so cascade deletes were not enforced
// and orphaned child rows accumulated (e.g. asset rows left behind when their
// utxo was deleted). When AutoMigrate rebuilds such a table to add the foreign
// key it copies the rows with the constraint enforced, and the orphaned rows
// cause "FOREIGN KEY constraint failed (787)", aborting startup (#2696).
//
// Purging these rows is equivalent to what the cascade would have done: their
// parent is already gone, so they are dead data. This must run before
// AutoMigrate. It is a no-op once the foreign keys exist, since the constraint
// then prevents orphans from accumulating.
func PurgeOrphanedCascadeRows(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	for _, spec := range orphanPurgeSpecs() {
		if err := purgeOrphanedChildRows(db, logger, spec); err != nil {
			return err
		}
	}
	return nil
}

func purgeOrphanedChildRows(
	db *gorm.DB,
	logger *slog.Logger,
	spec orphanPurgeSpec,
) error {
	// Nothing to do if the child table has not been created yet (fresh DB;
	// AutoMigrate creates it with the constraint already in place).
	if !db.Migrator().HasTable(spec.child) {
		return nil
	}
	// If the cascade foreign key already exists, the database is already
	// consistent and no orphans can accumulate. Skip the scan, which would
	// otherwise run on every startup against large tables.
	if db.Migrator().HasConstraint(spec.relOwner, spec.relField) {
		return nil
	}

	query := db.Where(
		spec.fkColumn+" NOT IN (?)",
		db.Model(spec.parent).Select("id"),
	)
	if spec.nullable {
		// A NULL foreign key is an unset optional association, not an
		// orphan, and does not violate the constraint. Keep such rows.
		query = query.Where(spec.fkColumn + " IS NOT NULL")
	}
	result := query.Delete(spec.child)
	if result.Error != nil {
		return fmt.Errorf(
			"purging orphaned %s rows: %w", spec.label, result.Error,
		)
	}
	if result.RowsAffected > 0 {
		logger.Info(
			"purged orphaned rows before adding cascade foreign key",
			"table", spec.label,
			"foreign_key", spec.fkColumn,
			"deleted", result.RowsAffected,
		)
	}
	return nil
}
