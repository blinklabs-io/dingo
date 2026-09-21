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
	"bytes"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
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
		return errors.New(
			"alonzo protocol-parameter unit marker is missing; " +
				"recreate both metadata and blob stores and resync from genesis",
		)
	}
	switch unit {
	case nodesettings.AlonzoPParamsUnitWordV1:
		return nil
	case nodesettings.AlonzoPParamsUnitLegacyByteV0:
		// The lossy value is recomputable from Alonzo genesis for a row the
		// pre-v0.205.7 era transition wrote, so a resync is the fallback
		// rather than the first answer. See repairAlonzoPParamsUnit.
		repaired, err := d.repairAlonzoPParamsUnit()
		if repaired {
			return nil
		}
		var unrepairable errAlonzoPParamsUnitUnrepairable
		if errors.As(err, &unrepairable) {
			return fmt.Errorf(
				"persisted Alonzo protocol parameters use legacy byte units "+
					"and cannot be repaired in place (%s); "+
					"recreate both metadata and blob stores and resync from genesis",
				unrepairable.reason,
			)
		}
		if err != nil {
			return err
		}
		// repairAlonzoPParamsUnit never returns (false, nil), so this is
		// unreachable. It fails closed rather than accepting a legacy
		// database if that contract is ever broken.
		return errors.New(
			"persisted Alonzo protocol parameters use legacy byte units; " +
				"recreate both metadata and blob stores and resync from genesis",
		)
	default:
		return fmt.Errorf(
			"alonzo protocol-parameter unit marker has unknown value %q; "+
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

// alonzoPParamsRepairStore lists and rewrites persisted protocol-parameter
// rows for one era. It is an optional capability for the same reason
// alonzoPParamsEraStore is: a provider that cannot offer it stays classified
// as legacy and requires a resync instead.
type alonzoPParamsRepairStore interface {
	ListPParamsForEra(uint, types.Txn) ([]models.PParams, error)
	UpdatePParamsCbor(uint, []byte, types.Txn) error
}

// errAlonzoPParamsUnitUnrepairable reports why a legacy classification could
// not be repaired in place. It is advisory: the caller still fails closed and
// asks for a resync, but names the reason so an operator can tell a
// chain-sourced value apart from a missing capability.
type errAlonzoPParamsUnitUnrepairable struct {
	reason string
}

func (e errAlonzoPParamsUnitUnrepairable) Error() string {
	return e.reason
}

// repairAlonzoPParamsUnit rewrites persisted Alonzo protocol-parameter rows
// from the lossy per-byte value written before gouroboros v0.205.7 to the
// per-word value key 17 actually holds in Alonzo, and clears the legacy
// marker once every row is accounted for.
//
// The lost value is recoverable because it was not arbitrary. Releases up to
// v0.205.5 set AdaPerUtxoByte to genesis.LovelacePerUtxoWord / 8 when Alonzo
// started (alonzo.UpdateFromGenesis), and integer division is the only reason
// the row cannot simply be multiplied back: a row still holding
// lovelacePerUtxoWord/8 identifies itself, and Alonzo genesis supplies the
// exact value it came from. An on-chain update to key 17 was applied
// verbatim (alonzo.Update), so any other value is a real chain-sourced word
// count this repair must not touch.
//
// Rows already holding lovelacePerUtxoWord are accepted unchanged. That is
// what makes the repair crash-idempotent: the row rewrites and the marker
// write cannot share one transaction, so a crash between them leaves some
// rows converted and the next start has to converge rather than refuse.
//
// It returns true only when the marker has been cleared. Every other return
// carries a non-nil error: an errAlonzoPParamsUnitUnrepairable carries the
// reason the repair could not be applied, and any other error is a store
// failure. It never returns (false, nil).
func (d *Database) repairAlonzoPParamsUnit() (bool, error) {
	word := d.config.AlonzoLovelacePerUtxoWord
	if word == 0 {
		return false, errAlonzoPParamsUnitUnrepairable{
			reason: "Alonzo genesis lovelacePerUTxOWord was not supplied",
		}
	}
	legacy := word / 8
	// A word count below 8 divides to zero, and a row legitimately holding
	// key 17 = 0 is then indistinguishable from the lossy form this repair
	// rewrites, so no rewrite is provable. Only a synthetic genesis reaches
	// this.
	if legacy == 0 {
		return false, errAlonzoPParamsUnitUnrepairable{
			reason: fmt.Sprintf(
				"Alonzo genesis lovelacePerUTxOWord %d is not separable from its per-byte form",
				word,
			),
		}
	}
	store, ok := d.Metadata().(alonzoPParamsRepairStore)
	if !ok {
		return false, errAlonzoPParamsUnitUnrepairable{
			reason: "metadata store cannot rewrite protocol-parameter rows",
		}
	}
	rows, err := store.ListPParamsForEra(alonzo.EraIdAlonzo, nil)
	if err != nil {
		return false, fmt.Errorf(
			"list persisted Alonzo protocol parameters: %w",
			err,
		)
	}
	type repair struct {
		id   uint
		cbor []byte
	}
	repairs := make([]repair, 0, len(rows))
	for _, row := range rows {
		var params alonzo.AlonzoProtocolParameters
		if _, err := cbor.Decode(row.Cbor, &params); err != nil {
			return false, errAlonzoPParamsUnitUnrepairable{
				reason: fmt.Sprintf(
					"protocol-parameter row %d does not decode as Alonzo: %v",
					row.ID,
					err,
				),
			}
		}
		switch params.AdaPerUtxoByte {
		case word:
			continue
		case legacy:
		default:
			return false, errAlonzoPParamsUnitUnrepairable{
				reason: fmt.Sprintf(
					"protocol-parameter row %d holds key 17 value %d, which is neither the lossy %d nor the corrected %d, so it came from an on-chain update and cannot be rewritten",
					row.ID,
					params.AdaPerUtxoByte,
					legacy,
					word,
				),
			}
		}
		// Re-encoding the row unchanged must reproduce the stored bytes
		// exactly before a rewrite is trusted. Without that check a codec
		// difference anywhere else in the struct would be persisted
		// silently alongside the corrected key 17.
		verbatim, err := cbor.Encode(&params)
		if err != nil {
			return false, errAlonzoPParamsUnitUnrepairable{
				reason: fmt.Sprintf(
					"protocol-parameter row %d does not re-encode: %v",
					row.ID,
					err,
				),
			}
		}
		if !bytes.Equal(verbatim, row.Cbor) {
			return false, errAlonzoPParamsUnitUnrepairable{
				reason: fmt.Sprintf(
					"protocol-parameter row %d does not round-trip through the Alonzo codec",
					row.ID,
				),
			}
		}
		params.AdaPerUtxoByte = word
		corrected, err := cbor.Encode(&params)
		if err != nil {
			return false, errAlonzoPParamsUnitUnrepairable{
				reason: fmt.Sprintf(
					"corrected protocol-parameter row %d does not encode: %v",
					row.ID,
					err,
				),
			}
		}
		repairs = append(repairs, repair{id: row.ID, cbor: corrected})
	}
	if len(repairs) > 0 {
		if err := d.Transaction(true).Do(func(txn *Txn) error {
			for _, item := range repairs {
				if err := store.UpdatePParamsCbor(
					item.id,
					item.cbor,
					txn.Metadata(),
				); err != nil {
					return fmt.Errorf(
						"rewrite Alonzo protocol-parameter row %d: %w",
						item.id,
						err,
					)
				}
			}
			return nil
		}); err != nil {
			return false, err
		}
	}
	epoch, slot := d.currentEpochSlot()
	if err := d.Metadata().SetNodeSettingsGates(
		nodesettings.Values{
			nodesettings.AlonzoPParamsUnitGateName: nodesettings.AlonzoPParamsUnitWordV1,
		},
		epoch,
		slot,
	); err != nil {
		return false, fmt.Errorf(
			"record repaired Alonzo protocol-parameter unit: %w",
			err,
		)
	}
	d.logger.Info(
		"repaired persisted Alonzo protocol-parameter units",
		"rows", len(repairs),
		"lovelace_per_utxo_word", word,
	)
	return true, nil
}
