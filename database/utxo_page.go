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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

// UtxosByAddressPage scans at most maxCandidates outputs in ascending ledger
// order, checking exact addresses against CBOR. Native assets are not loaded.
// On cancellation the partial page and last fully examined candidate accompany
// the error. A candidate that failed to load or decode is never skipped.
// SQL uses ctx; blob reads have no context parameter, so cancellation is checked
// between them and cannot impose a hard deadline on an in-flight blob read.
func (d *Database) UtxosByAddressPage(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
) (models.UtxoAddressPage, error) {
	page := models.UtxoAddressPage{}
	if q == nil {
		return page, models.ErrNilUtxoWithOrderingQuery
	}
	if q.Limit <= 0 || maxCandidates <= 0 || q.Offset != 0 || q.Descending ||
		q.MatchAllAddresses || !models.RequiresExactAddressFilter(q.AddressPatterns) {
		return page, errors.New(
			"address page requires exact patterns, positive limits, and ascending cursor pagination",
		)
	}
	page.Next = q.After
	if err := ctx.Err(); err != nil {
		return page, err
	}
	txn := NewTxnContext(ctx, d, false)
	defer txn.Release()
	scan := *q
	scan.SkipAssets = true
	for page.Scanned < maxCandidates && len(page.Utxos) < q.Limit {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		scan.Limit = min(128, maxCandidates-page.Scanned)
		scan.After = page.Next
		batch, err := d.utxoStore().
			GetUtxosByAddressWithOrdering(&scan, txn.Metadata())
		if err != nil {
			if ctx.Err() != nil {
				return page, ctx.Err()
			}
			return page, err
		}
		for i := range batch {
			if err := ctx.Err(); err != nil {
				return page, err
			}
			u := &batch[i]
			if err := loadCbor(&u.Utxo, txn); err != nil {
				return page, err
			}
			if err := ctx.Err(); err != nil {
				return page, err
			}
			output, err := u.Decode()
			if err != nil {
				return page, fmt.Errorf("decode address candidate: %w", err)
			}
			match, err := models.MatchesUtxoAddressPatterns(
				output.Address(),
				q.AddressPatterns,
			)
			if err != nil {
				return page, err
			}
			page.Scanned++
			page.Next = &models.UtxoOrderingCursor{
				Slot: u.TxSlot, BlockIndex: u.TxBlockIndex,
				OutputIdx: u.OutputIdx, TxId: u.TxId,
			}
			if match {
				page.Utxos = append(page.Utxos, *u)
			}
			if len(page.Utxos) == q.Limit {
				// Only a short, fully processed batch proves exhaustion.
				if i == len(batch)-1 && len(batch) < scan.Limit {
					page.Next = nil
				}
				return page, ctx.Err()
			}
		}
		if len(batch) < scan.Limit {
			page.Next = nil
			return page, ctx.Err()
		}
	}
	return page, ctx.Err()
}
