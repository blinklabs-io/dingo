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
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
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
	if q != nil && (q.MatchAllAddresses ||
		!models.RequiresExactAddressFilter(q.AddressPatterns)) {
		return models.UtxoAddressPage{}, errors.New(
			"address page requires exact address patterns",
		)
	}
	return d.utxosByAddressPage(ctx, q, maxCandidates, 0)
}

// UtxosByAddressBoundedPage scans an ordered page while bounding both the
// number of candidate rows examined and the CBOR retained for matches. A zero
// byte limit is invalid; callers that do not need a byte bound use
// UtxosByAddressPage.
func (d *Database) UtxosByAddressBoundedPage(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
) (models.UtxoAddressPage, error) {
	if maxBytes <= 0 {
		return models.UtxoAddressPage{}, errors.New(
			"address page requires a positive byte limit",
		)
	}
	return d.utxosByAddressPage(ctx, q, maxCandidates, maxBytes)
}

// ScanUtxosByAddress visits every UTxO matching q's exact address patterns
// in ascending ledger order, one page at a time, and returns the tip stored
// in the same read transaction. q.Limit is the page size and q.After must be
// nil; maxCandidates and maxBytes bound each page as in
// UtxosByAddressBoundedPage, so memory is bounded by one page however many
// outputs match. Every page and the tip come from one snapshot, so a block
// applied mid-scan neither duplicates nor drops an output, and the tip names
// the state the outputs belong to. The transaction stays open until the scan
// returns, including while visit runs. Native assets are not loaded. A
// non-nil error from visit ends the scan and is returned.
func (d *Database) ScanUtxosByAddress(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
	visit func([]models.UtxoWithOrdering) error,
) (ochainsync.Tip, error) {
	if q == nil {
		return ochainsync.Tip{}, models.ErrNilUtxoWithOrderingQuery
	}
	if q.MatchAllAddresses ||
		!models.RequiresExactAddressFilter(q.AddressPatterns) {
		return ochainsync.Tip{}, errors.New(
			"address scan requires exact address patterns",
		)
	}
	if q.After != nil || maxBytes <= 0 {
		return ochainsync.Tip{}, errors.New(
			"address scan requires no cursor and a positive byte limit",
		)
	}
	if err := ctx.Err(); err != nil {
		return ochainsync.Tip{}, err
	}
	txn := NewTxnContext(ctx, d, false)
	defer txn.Release()
	// The first read fixes the snapshot on engines that take it lazily.
	tip, err := d.GetTip(txn)
	if err != nil {
		return ochainsync.Tip{}, err
	}
	pageQuery := *q
	for {
		page, err := d.utxosByAddressPageTxn(
			ctx, txn, &pageQuery, maxCandidates, maxBytes,
		)
		if err != nil {
			return ochainsync.Tip{}, err
		}
		if len(page.Utxos) > 0 {
			if err := visit(page.Utxos); err != nil {
				return ochainsync.Tip{}, err
			}
		}
		if page.Next == nil {
			return tip, nil
		}
		pageQuery.After = page.Next
	}
}

func (d *Database) utxosByAddressPage(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
) (models.UtxoAddressPage, error) {
	if err := validateAddressPageQuery(q, maxCandidates); err != nil {
		return models.UtxoAddressPage{}, err
	}
	if err := ctx.Err(); err != nil {
		return models.UtxoAddressPage{Next: q.After}, err
	}
	txn := NewTxnContext(ctx, d, false)
	defer txn.Release()
	return d.utxosByAddressPageTxn(ctx, txn, q, maxCandidates, maxBytes)
}

func validateAddressPageQuery(
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
) error {
	if q == nil {
		return models.ErrNilUtxoWithOrderingQuery
	}
	if q.Limit <= 0 || maxCandidates <= 0 || q.Offset != 0 || q.Descending {
		return errors.New(
			"address page requires positive limits and ascending cursor pagination",
		)
	}
	return nil
}

func (d *Database) utxosByAddressPageTxn(
	ctx context.Context,
	txn *Txn,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
) (models.UtxoAddressPage, error) {
	page := models.UtxoAddressPage{}
	if err := validateAddressPageQuery(q, maxCandidates); err != nil {
		return page, err
	}
	page.Next = q.After
	if err := ctx.Err(); err != nil {
		return page, err
	}
	scan := *q
	scan.SkipAssets = true
	var retainedBytes int64
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
			match := q.MatchAllAddresses
			if !match && len(q.AddressPatterns) > 0 {
				output, err := u.Decode()
				if err != nil {
					return page, fmt.Errorf("decode address candidate: %w", err)
				}
				match, err = models.MatchesUtxoAddressPatterns(
					output.Address(),
					q.AddressPatterns,
				)
				if err != nil {
					return page, err
				}
			}
			cursor := &models.UtxoOrderingCursor{
				Slot: u.TxSlot, BlockIndex: u.TxBlockIndex,
				OutputIdx: u.OutputIdx, TxId: u.TxId,
			}
			if match {
				cborBytes := int64(len(u.Cbor))
				if maxBytes > 0 && len(page.Utxos) > 0 &&
					retainedBytes+cborBytes > maxBytes {
					return page, nil
				}
				retainedBytes += cborBytes
				page.Utxos = append(page.Utxos, *u)
			}
			page.Scanned++
			page.Next = cursor
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
