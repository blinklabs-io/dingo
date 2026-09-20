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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
)

type alonzoPParamsEraStore interface {
	HasPParamsForEra(uint, types.Txn) (bool, error)
}

func (d *Database) checkAlonzoPParamsUnit() error {
	gates, err := d.Metadata().GetNodeSettingsGates()
	if err != nil {
		return fmt.Errorf(
			"read Alonzo protocol-parameter unit marker: %w",
			err,
		)
	}
	unit, ok := gates[nodesettings.AlonzoPParamsUnitGateName]
	if !ok {
		return fmt.Errorf(
			"Alonzo protocol-parameter unit marker is missing; " +
				"recreate both metadata and blob stores and resync from genesis",
		)
	}
	switch unit {
	case nodesettings.AlonzoPParamsUnitWordV1:
		return nil
	case nodesettings.AlonzoPParamsUnitLegacyByteV0:
		return fmt.Errorf(
			"persisted Alonzo protocol parameters use legacy byte units; " +
				"recreate both metadata and blob stores and resync from genesis",
		)
	default:
		return fmt.Errorf(
			"Alonzo protocol-parameter unit marker has unknown value %q; "+
				"recreate both metadata and blob stores and resync from genesis",
			unit,
		)
	}
}

// ReconcileAlonzoPParamsUnitAfterRecovery repairs a conservative v20
// classification only when commit-timestamp recovery removed every persisted
// Alonzo protocol-parameter row. The migration must initially treat any such
// row as legacy because releases before gouroboros v0.205.7 stored a lossy
// per-byte value. Once recovery has rolled all of those rows back, however,
// there is no incompatible value left to protect and a genesis replay can
// safely persist the corrected per-word representation.
//
// Missing and unknown markers are never inferred here. They remain hard
// failures in checkAlonzoPParamsUnit rather than being silently blessed.
func (d *Database) ReconcileAlonzoPParamsUnitAfterRecovery() error {
	gates, err := d.Metadata().GetNodeSettingsGates()
	if err != nil {
		return fmt.Errorf(
			"read Alonzo protocol-parameter unit marker: %w",
			err,
		)
	}
	if gates[nodesettings.AlonzoPParamsUnitGateName] !=
		nodesettings.AlonzoPParamsUnitLegacyByteV0 {
		return nil
	}
	store, ok := d.Metadata().(alonzoPParamsEraStore)
	if !ok {
		// This optional capability avoids expanding the public metadata-store
		// interface solely for recovery of one conservative marker. A provider
		// that cannot make the exact era-only query stays safely classified as
		// legacy and the following phase-1 check requires a resync.
		return nil
	}
	hasAlonzo, err := store.HasPParamsForEra(
		alonzo.EraIdAlonzo,
		nil,
	)
	if err != nil {
		return fmt.Errorf(
			"check recovered Alonzo protocol parameters: %w",
			err,
		)
	}
	if hasAlonzo {
		return nil
	}
	epoch, slot := d.currentEpochSlot()
	if err := d.Metadata().SetNodeSettingsGates(
		nodesettings.Values{
			nodesettings.AlonzoPParamsUnitGateName: nodesettings.AlonzoPParamsUnitWordV1,
		},
		epoch,
		slot,
	); err != nil {
		return fmt.Errorf(
			"record recovered Alonzo protocol-parameter unit: %w",
			err,
		)
	}
	return nil
}
