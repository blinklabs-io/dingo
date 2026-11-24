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

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

func (s *Store) rewardUpgradeTableExists(
	ctx context.Context,
	tx *sql.Tx,
	table string,
) (bool, error) {
	query := `SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`
	switch s.dialect.Name() {
	case "postgres":
		query = `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?)`
	case "mysql":
		query = `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)`
	}
	var exists bool
	err := tx.QueryRowContext(ctx, s.dialect.Rebind(query), table).Scan(&exists)
	return exists, err
}

// preflightRewardStakeUpgrade certifies only the unchanged Conway-and-earlier
// accounting case. Refusal rolls back every certification before schema changes
// can make the database unreadable by the binary that wrote its snapshots.
func (s *Store) preflightRewardStakeUpgrade(
	ctx context.Context,
	conn *sql.Conn,
) error {
	tx, err := conn.BeginTx(ctx, s.dialect.BeginOptions(false))
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // Commit or rollback closes the transaction.
	tables := make(map[string]bool)
	for _, table := range []string{"pool_stake_snapshot", "reward_snapshot", "epoch", "pparams", "leios_transaction_context"} {
		tables[table], err = s.rewardUpgradeTableExists(ctx, tx, table)
		if err != nil {
			return fmt.Errorf("inspect reward stake provenance tables: %w", err)
		}
	}
	if !tables["pool_stake_snapshot"] && !tables["reward_snapshot"] {
		return nil
	}
	unsafe := func(reason string) error {
		return fmt.Errorf(
			"consensus stake snapshot provenance cannot be upgraded safely: %s; rebootstrap from immutable blocks or a trusted snapshot",
			reason,
		)
	}
	var stale int64
	for _, item := range []struct{ table, predicate string }{
		{"pool_stake_snapshot", "snapshot_type IN ('mark', 'set', 'go')"},
		{"reward_snapshot", "snapshot_type = 'mark'"},
	} {
		if !tables[item.table] {
			continue
		}
		var count int64
		err = tx.QueryRowContext(ctx, s.dialect.Rebind("SELECT COUNT(*) FROM "+item.table+" WHERE "+item.predicate+" AND (calculation_version IS NULL OR calculation_version <> ?)"), models.RewardStakeCalculationVersion).
			Scan(&count)
		if err != nil {
			return fmt.Errorf(
				"read reward stake provenance before schema upgrade: %w",
				err,
			)
		}
		stale += count
	}
	if stale == 0 {
		return nil
	}
	if models.RewardStakeCalculationVersion != 3 {
		return unsafe("unsupported accounting certification target")
	}
	if !s.genesisStakingEmpty {
		return unsafe("genesis staking is present or unverified")
	}
	for _, table := range []string{"pool_stake_snapshot", "reward_snapshot", "epoch", "pparams"} {
		if !tables[table] {
			return unsafe("missing " + table + " provenance")
		}
	}
	checks := []string{
		`SELECT COUNT(*) FROM pool_stake_snapshot s WHERE snapshot_type IN ('mark', 'set', 'go') AND (calculation_version IS NULL OR calculation_version NOT IN (2, 3))`,
		fmt.Sprintf(
			`SELECT COUNT(*) FROM reward_snapshot s WHERE snapshot_type = 'mark' AND (calculation_version IS NULL OR calculation_version NOT IN (2, 3) OR protocol_version IS NULL OR protocol_version < 0 OR protocol_version >= %d)`,
			common.ProtocolVersionDijkstra,
		),
		fmt.Sprintf(
			`SELECT COUNT(*) FROM epoch WHERE era_id IS NULL OR era_id < 0 OR era_id >= %d`,
			dijkstra.EraIdDijkstra,
		),
		fmt.Sprintf(
			`SELECT COUNT(*) FROM pool_stake_snapshot s WHERE snapshot_type IN ('mark', 'set', 'go') AND calculation_version = 2 AND NOT EXISTS (SELECT 1 FROM epoch e WHERE e.epoch_id = s.epoch AND e.era_id >= 0 AND e.era_id < %d)`,
			dijkstra.EraIdDijkstra,
		),
		fmt.Sprintf(
			`SELECT COUNT(*) FROM reward_snapshot s WHERE snapshot_type = 'mark' AND calculation_version = 2 AND NOT EXISTS (SELECT 1 FROM epoch e WHERE e.epoch_id = s.epoch AND e.era_id >= 0 AND e.era_id < %d)`,
			dijkstra.EraIdDijkstra,
		),
	}
	if tables["leios_transaction_context"] {
		checks = append(
			checks,
			`SELECT COUNT(*) FROM leios_transaction_context`,
		)
	}
	for _, query := range checks {
		var count int64
		if err := tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf(
				"check reward stake accounting compatibility: %w",
				err,
			)
		}
		if count != 0 {
			return unsafe("older, unknown, or Dijkstra accounting state")
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT era_id, cbor FROM pparams`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var foundParams bool
	for rows.Next() {
		var era sql.NullInt64
		var raw []byte
		if err := rows.Scan(&era, &raw); err != nil {
			rows.Close()
			return err
		}
		foundParams = true
		if !era.Valid || era.Int64 < 0 ||
			era.Int64 >= int64(dijkstra.EraIdDijkstra) {
			rows.Close()
			return unsafe("unknown or Dijkstra protocol parameters")
		}
		if err := rewardUpgradeParameters(era.Int64, raw); err != nil {
			rows.Close()
			return unsafe(err.Error())
		}
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	if !foundParams {
		return unsafe("missing protocol parameter history")
	}
	for _, query := range []string{
		`UPDATE pool_stake_snapshot SET calculation_version = ? WHERE snapshot_type IN ('mark', 'set', 'go') AND calculation_version = 2`,
		`UPDATE reward_snapshot SET calculation_version = ? WHERE snapshot_type = 'mark' AND calculation_version = 2`,
	} {
		if _, err := tx.ExecContext(ctx, s.dialect.Rebind(query), models.RewardStakeCalculationVersion); err != nil {
			return fmt.Errorf(
				"certify unchanged reward stake accounting: %w",
				err,
			)
		}
	}
	return tx.Commit()
}

// Every recorded parameter set must prove its era and protocol version. Byron
// has no stored protocol parameter decoder and cannot provide this proof.
func rewardUpgradeParameters(era int64, raw []byte) error {
	if len(raw) == 0 {
		return errors.New("missing protocol parameters")
	}
	var target any
	var major func() uint
	var minVersion, maxVersion uint
	switch era {
	case shelley.EraIdShelley:
		p := new(shelley.ShelleyProtocolParameters)
		target = p
		major = func() uint { return uint(p.ProtocolMajor) }
		minVersion, maxVersion = shelley.MinProtocolVersionShelley, shelley.MaxProtocolVersionShelley
	case allegra.EraIdAllegra:
		p := new(allegra.AllegraProtocolParameters)
		target = p
		major = func() uint { return uint(p.ProtocolMajor) }
		minVersion, maxVersion = allegra.MinProtocolVersionAllegra, allegra.MaxProtocolVersionAllegra
	case mary.EraIdMary:
		p := new(mary.MaryProtocolParameters)
		target = p
		major = func() uint { return uint(p.ProtocolMajor) }
		minVersion, maxVersion = mary.MinProtocolVersionMary, mary.MaxProtocolVersionMary
	case alonzo.EraIdAlonzo:
		p := new(alonzo.AlonzoProtocolParameters)
		target = p
		major = func() uint { return uint(p.ProtocolMajor) }
		minVersion, maxVersion = alonzo.MinProtocolVersionAlonzo, alonzo.MaxProtocolVersionAlonzo
	case babbage.EraIdBabbage:
		p := new(babbage.BabbageProtocolParameters)
		target = p
		major = func() uint { return uint(p.ProtocolMajor) }
		minVersion, maxVersion = babbage.MinProtocolVersionBabbage, babbage.MaxProtocolVersionBabbage
	case conway.EraIdConway:
		p := new(conway.ConwayProtocolParameters)
		target = p
		major = func() uint { return uint(p.ProtocolVersion.Major) }
		minVersion, maxVersion = conway.MinProtocolVersionConway, conway.MaxProtocolVersionConway

	default:
		return errors.New("unsupported protocol parameter era")
	}
	consumed, err := cbor.Decode(raw, target)
	if err != nil {
		return fmt.Errorf("malformed protocol parameters: %w", err)
	}
	if consumed != len(raw) {
		return errors.New("trailing protocol parameter CBOR")
	}
	if major() < minVersion || major() > maxVersion {
		return errors.New("protocol version inconsistent with era")
	}
	return nil
}
