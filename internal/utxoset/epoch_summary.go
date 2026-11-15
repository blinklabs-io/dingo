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
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
)

// The row IDs belong to the metadata store, so the copies leave them out.
func encodeSummary(sm models.EpochSummary) (badgerutxo.SummaryRow, error) {
	sm.ID = 0
	v, err := cbor.Encode(sm)
	if err != nil {
		return badgerutxo.SummaryRow{}, fmt.Errorf(
			"encode epoch summary %d: %w", sm.Epoch, err,
		)
	}
	return badgerutxo.SummaryRow{Epoch: sm.Epoch, Value: v}, nil
}

func decodeSummary(r badgerutxo.SummaryRow) (*models.EpochSummary, error) {
	var sm models.EpochSummary
	if err := decodeInto(r.Value, &sm); err != nil {
		return nil, fmt.Errorf("decode epoch summary %d: %w", r.Epoch, err)
	}
	return &sm, nil
}

// summaryBatch converts a commit's epoch summary rows for the store.
func summaryBatch(lc ledger.LedgerChanges) ([]badgerutxo.SummaryRow, error) {
	rows := make([]badgerutxo.SummaryRow, 0, len(lc.Summaries))
	for _, sm := range lc.Summaries {
		row, err := encodeSummary(sm)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ensureSummaries loads the metadata store's epoch summary rows into a set
// from before it kept them. It follows ensureEpochState, whose epoch rows
// bound the summaries to read.
func (s *Set) ensureSummaries(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.SummariesLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the epoch summary marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read epochs: %w", err)
	}
	var rows []badgerutxo.SummaryRow
	var hi uint64
	for _, e := range epochs {
		hi = max(hi, e.EpochId)
	}
	if len(epochs) > 0 {
		// A boundary summarises the epoch it closes, and snapshot capture
		// can run a little past the newest epoch row.
		for epoch := uint64(0); epoch <= hi+3; epoch++ {
			sm, err := ldb.GetEpochSummary(epoch, txn)
			if err != nil {
				return fmt.Errorf(
					"utxoset: read epoch summary %d: %w", epoch, err,
				)
			}
			if sm == nil {
				continue
			}
			row, err := encodeSummary(*sm)
			if err != nil {
				return err
			}
			rows = append(rows, row)
		}
	}
	if err := s.store.LoadSummaries(rows); err != nil {
		return fmt.Errorf("utxoset: load epoch summaries: %w", err)
	}
	s.logger.Info("loaded epoch summaries into the live UTxO set",
		"summaries", len(rows))
	return nil
}

// EpochSummariesLoaded reports whether the set holds every epoch summary
// row.
func (s *Set) EpochSummariesLoaded() (bool, error) {
	return s.store.SummariesLoaded()
}

// EpochSummary returns epoch's summary row, or nil when the set has none.
func (s *Set) EpochSummary(epoch uint64) (*models.EpochSummary, error) {
	r, ok, err := s.store.Summary(epoch)
	if err != nil || !ok {
		return nil, err
	}
	return decodeSummary(r)
}

// EpochSummaries returns every epoch summary row the set holds, in epoch
// order.
func (s *Set) EpochSummaries() ([]models.EpochSummary, error) {
	rows, err := s.store.Summaries()
	if err != nil {
		return nil, err
	}
	out := make([]models.EpochSummary, 0, len(rows))
	for _, r := range rows {
		sm, err := decodeSummary(r)
		if err != nil {
			return nil, err
		}
		out = append(out, *sm)
	}
	return out, nil
}
