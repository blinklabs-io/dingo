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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

func pparamsRow(p models.PParams) badgerutxo.PParamsRow {
	return badgerutxo.PParamsRow{
		Era:       uint64(p.EraId),
		Epoch:     p.Epoch,
		AddedSlot: p.AddedSlot,
		Cbor:      p.Cbor,
	}
}

func pparamsModel(r badgerutxo.PParamsRow) models.PParams {
	return models.PParams{
		Cbor:      r.Cbor,
		AddedSlot: r.AddedSlot,
		Epoch:     r.Epoch,
		EraId:     uint(r.Era),
	}
}

// ensurePParams loads the metadata store's protocol parameter rows into a
// set from before it kept them.
func (s *Set) ensurePParams(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.PParamsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the pparams marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	var all []models.PParams
	for _, era := range eras.ErasWithDijkstra {
		rows, err := ldb.ListPParamsForEra(era.Id, txn)
		if err != nil {
			return fmt.Errorf("utxoset: read pparams of era %d: %w", era.Id, err)
		}
		all = append(all, rows...)
	}
	// The row IDs give the write order the reads depend on.
	slices.SortFunc(all, func(a, b models.PParams) int { return cmp.Compare(a.ID, b.ID) })
	rows := make([]badgerutxo.PParamsRow, 0, len(all))
	for _, p := range all {
		rows = append(rows, pparamsRow(p))
	}
	if err := s.store.LoadPParams(rows); err != nil {
		return fmt.Errorf("utxoset: load pparams: %w", err)
	}
	s.logger.Info("loaded protocol parameters into the live UTxO set",
		"rows", len(rows))
	return nil
}

// DecodePParamsRows converts a store's protocol parameter rows to models, in
// the order given and without row IDs.
func DecodePParamsRows(rows []badgerutxo.PParamsRow) []models.PParams {
	out := make([]models.PParams, 0, len(rows))
	for _, r := range rows {
		out = append(out, pparamsModel(r))
	}
	return out
}

// PParamsLoaded reports whether the set holds every protocol parameter row.
func (s *Set) PParamsLoaded() (bool, error) {
	return s.store.PParamsLoaded()
}

// PParamsForEra returns era's protocol parameter rows in the order they
// were written, without row IDs.
func (s *Set) PParamsForEra(era uint) ([]models.PParams, error) {
	rows, err := s.store.PParamsForEra(uint64(era))
	if err != nil {
		return nil, err
	}
	return DecodePParamsRows(rows), nil
}
