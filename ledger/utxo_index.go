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

package ledger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Index body kinds. A live UTxO set that keeps an output index files each
// output under one body per kind that applies to it, so a read by payment
// credential, stake credential, address or inline datum hash walks only the
// outputs it names.
const (
	utxoIndexPayment = 'p'
	utxoIndexStake   = 's'
	utxoIndexAddress = 'a'
	utxoIndexDatum   = 'd'
)

// UtxoIndexBodies returns the index bodies of a live output: its payment
// credential, or its whole address when it has none (a Byron address), its
// stake credential, and the hash of its inline datum. It is the Indexer the
// live set keeps its output index with. Its signature cannot fail a commit,
// so CBOR that does not decode has no bodies and is never found by an address
// or datum read, as it is not by any other decoded read.
func UtxoIndexBodies(cbor []byte) [][]byte {
	out, err := gledger.NewTransactionOutputFromCbor(cbor)
	if err != nil || out == nil {
		return nil
	}
	addr := out.Address()
	var bodies [][]byte
	if body, err := addressIndexBody(addr); err == nil {
		bodies = append(bodies, body)
	}
	if skh := addr.StakeKeyHash(); skh != (lcommon.Blake2b224{}) {
		bodies = append(bodies, indexBody(utxoIndexStake, skh.Bytes()))
	}
	if raw := inlineDatumCbor(out); len(raw) > 0 {
		hash := lcommon.Blake2b256Hash(raw)
		bodies = append(bodies, indexBody(utxoIndexDatum, hash.Bytes()))
	}
	return bodies
}

func indexBody(kind byte, key []byte) []byte {
	return append([]byte{kind}, key...)
}

// addressIndexBody is the body an address is found under: its payment
// credential when it has one, else its whole encoding.
func addressIndexBody(addr lcommon.Address) ([]byte, error) {
	if addr.Type() <= lcommon.AddressTypeScriptNone {
		return indexBody(utxoIndexPayment, addr.PaymentKeyHash().Bytes()), nil
	}
	raw, err := addr.Bytes()
	if err != nil {
		return nil, err
	}
	return indexBody(utxoIndexAddress, raw), nil
}

func inlineDatumCbor(out lcommon.TransactionOutput) []byte {
	d := out.Datum()
	if d == nil {
		return nil
	}
	if raw := d.Cbor(); len(raw) > 0 {
		return raw
	}
	raw, err := d.MarshalCBOR()
	if err != nil {
		return nil
	}
	return raw
}

// patternIndexBody is the body whose outputs include every output the
// pattern matches. The pattern's own predicate still decides each of them.
func patternIndexBody(p models.UtxoAddressPattern) ([]byte, error) {
	switch {
	case len(p.ExactAddress) > 0:
		addr, err := lcommon.NewAddressFromBytes(p.ExactAddress)
		if err != nil {
			return nil, fmt.Errorf("decode exact address: %w", err)
		}
		return addressIndexBody(addr)
	case len(p.PaymentPart) > 0:
		if len(p.PaymentPart) != lcommon.AddressHashSize {
			return nil, fmt.Errorf(
				"payment part length %d, expected %d",
				len(p.PaymentPart), lcommon.AddressHashSize,
			)
		}
		return indexBody(utxoIndexPayment, p.PaymentPart), nil
	case len(p.DelegationPart) > 0:
		if len(p.DelegationPart) != lcommon.AddressHashSize {
			return nil, fmt.Errorf(
				"delegation part length %d, expected %d",
				len(p.DelegationPart), lcommon.AddressHashSize,
			)
		}
		return indexBody(utxoIndexStake, p.DelegationPart), nil
	}
	return nil, models.ErrEmptyUtxoAddressPattern
}

// UtxoSetIndex is a live UTxO set that keeps an output index, which lets a
// read name the outputs of one address or datum without walking the set.
type UtxoSetIndex interface {
	// AddressIndexed reports whether every live output is indexed.
	AddressIndexed() (bool, error)
	// EachIndexedUtxo calls fn for the live outputs found under any of
	// bodies, each once in ascending reference order, after the output after
	// when it is not nil, until fn returns true.
	EachIndexedUtxo(
		bodies [][]byte,
		after *models.UtxoId,
		fn func(txID []byte, index uint32, cbor []byte) (bool, error),
	) error
	// EachUtxoAfter is EachIndexedUtxo over every live output.
	EachUtxoAfter(
		after *models.UtxoId,
		fn func(txID []byte, index uint32, cbor []byte) (bool, error),
	) error
}

// ErrUtxoSetNotIndexed reports a read that needs the live UTxO set's output
// index before the set has built it.
var ErrUtxoSetNotIndexed = errors.New(
	"the live UTxO set has not finished indexing its outputs",
)

type overlayRow struct {
	key  utxoref.Key
	cbor []byte
}

// scanLive calls fn for the live outputs found under any of bodies, or for
// every live output when all is set, each once in ascending reference order
// after the output after, until fn returns true. It reads the set as an
// unpinned reader does: the set's applied state with the changes of every
// committed transaction not yet applied over it, so an output spent by such
// a transaction is not returned and one it created is.
func (ls *LedgerState) scanLive(
	bodies [][]byte,
	all bool,
	after *models.UtxoId,
	fn func(models.Utxo) (bool, error),
) error {
	idx, ok := ls.readSet(nil).(UtxoSetIndex)
	if !ok {
		return fmt.Errorf("%w: the live UTxO set keeps no output index", ErrUtxoSetUnserved)
	}
	if ls.utxoSetFailed.Load() {
		return errLiveUtxoSetUnavailable
	}
	indexed, err := idx.AddressIndexed()
	if err != nil {
		return err
	}
	if !indexed {
		return ErrUtxoSetNotIndexed
	}
	// The overlay is taken before the set is read, so a batch applied in
	// between is seen in both and read once, from the overlay.
	ls.utxoSetApplyMu.RLock()
	batches := ls.utxoSetBatchesSeenBy(nil, nil)
	overlay := make(map[utxoref.Key]utxoSetOverlayEntry)
	ls.utxoSetMu.Lock()
	for _, b := range batches {
		maps.Copy(overlay, b.overlay)
	}
	ls.utxoSetMu.Unlock()
	ls.utxoSetApplyMu.RUnlock()
	var staged []overlayRow
	for k, e := range overlay {
		if !e.live || !overlayAfter(k, after) {
			continue
		}
		if !all && !hasAnyBody(UtxoIndexBodies(e.cbor), bodies) {
			continue
		}
		staged = append(staged, overlayRow{key: k, cbor: e.cbor})
	}
	slices.SortFunc(staged, func(a, b overlayRow) int {
		return compareUtxoKeys(a.key, b.key)
	})
	stopped := false
	emit := func(txID []byte, index uint32, cbor []byte) error {
		u, err := liveUtxoRow(txID, index, cbor)
		if err != nil {
			return err
		}
		stop, err := fn(u)
		stopped = stop
		return err
	}
	visit := func(txID []byte, index uint32, cbor []byte) (bool, error) {
		key := utxoSetKey(txID, index)
		for len(staged) > 0 && compareUtxoKeys(staged[0].key, key) < 0 {
			row := staged[0]
			staged = staged[1:]
			if err := emit(row.key.TxId.Bytes(), row.key.Index, row.cbor); err != nil || stopped {
				return true, err
			}
		}
		if _, replaced := overlay[key]; replaced {
			return false, nil
		}
		return false, emit(txID, index, cbor)
	}
	// stopped ends the walk through the wrapper below, since a stop from
	// emit inside visit must reach the store's iterator as a stop.
	wrapped := func(txID []byte, index uint32, cbor []byte) (bool, error) {
		stop, err := visit(txID, index, cbor)
		return stop || stopped, err
	}
	if all {
		err = idx.EachUtxoAfter(after, wrapped)
	} else {
		err = idx.EachIndexedUtxo(bodies, after, wrapped)
	}
	if err != nil || stopped {
		return err
	}
	for _, row := range staged {
		if err := emit(row.key.TxId.Bytes(), row.key.Index, row.cbor); err != nil || stopped {
			return err
		}
	}
	return nil
}

// liveUtxoRow is the row a live output is reported as: the set stores only
// its CBOR, so the lovelace amount is read from it.
func liveUtxoRow(txID []byte, index uint32, cbor []byte) (models.Utxo, error) {
	u := models.Utxo{TxId: txID, OutputIdx: index, Cbor: cbor}
	out, err := u.Decode()
	if err != nil {
		return u, fmt.Errorf("decode output %x#%d: %w", txID, index, err)
	}
	amount, err := models.CheckedUint64FromBigInt(out.Amount())
	if err != nil {
		return u, fmt.Errorf("output %x#%d amount: %w", txID, index, err)
	}
	u.Amount = types.Uint64(amount)
	return u, nil
}

func compareUtxoKeys(a, b utxoref.Key) int {
	if c := bytes.Compare(a.TxId.Bytes(), b.TxId.Bytes()); c != 0 {
		return c
	}
	switch {
	case a.Index < b.Index:
		return -1
	case a.Index > b.Index:
		return 1
	}
	return 0
}

func overlayAfter(k utxoref.Key, after *models.UtxoId) bool {
	if after == nil {
		return true
	}
	return compareUtxoKeys(k, utxoSetKey(after.Hash, after.Idx)) > 0
}

func hasAnyBody(have, want [][]byte) bool {
	for _, h := range have {
		for _, w := range want {
			if bytes.Equal(h, w) {
				return true
			}
		}
	}
	return false
}

// UtxosByAddressBoundedPage returns an ordered, resumable page of the live
// outputs matching q, reading the live UTxO set's output index. Outputs are
// ordered by reference, not by the block that created them, which the live
// set does not record, so the returned cursors carry a zero slot and block
// index. maxCandidates bounds the index rows a call examines and maxBytes the
// output CBOR it retains, as for the IndexerDB; a page that stops on either
// carries the cursor to resume from. A query matching every address walks the
// set's outputs in reference order, bounded the same way.
func (ls *LedgerState) UtxosByAddressBoundedPage(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
) (models.UtxoAddressPage, error) {
	page := models.UtxoAddressPage{}
	if q == nil {
		return page, models.ErrNilUtxoWithOrderingQuery
	}
	if q.Limit <= 0 || q.Offset != 0 || q.Descending {
		return page, errors.New(
			"address page requires a positive limit and ascending cursor pagination",
		)
	}
	if len(q.IndexedExactAddress) > 0 {
		return page, errors.New(
			"address page does not take an indexed exact address",
		)
	}
	if q.FilterByAsset && len(q.AssetPolicyID) == 0 {
		return page, models.ErrEmptyAssetPolicyID
	}
	page.Next = q.After
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if !q.MatchAllAddresses && len(q.AddressPatterns) == 0 {
		page.Next = nil
		return page, nil
	}
	var bodies [][]byte
	if !q.MatchAllAddresses {
		for _, p := range q.AddressPatterns {
			body, err := patternIndexBody(p)
			if err != nil {
				return page, err
			}
			bodies = append(bodies, body)
		}
	}
	var after *models.UtxoId
	if q.After != nil {
		after = &models.UtxoId{Hash: q.After.TxId, Idx: q.After.OutputIdx}
	}
	if maxCandidates <= 0 {
		maxCandidates = math.MaxInt
	}
	var retained int64
	stopped := false
	err := ls.scanLive(
		bodies, q.MatchAllAddresses, after,
		func(u models.Utxo) (bool, error) {
			if err := ctx.Err(); err != nil {
				return true, err
			}
			if page.Scanned >= maxCandidates {
				stopped = true
				return true, nil
			}
			match, err := liveUtxoMatches(&u, q)
			if err != nil {
				return true, err
			}
			if match {
				size := int64(len(u.Cbor))
				if maxBytes > 0 && len(page.Utxos) > 0 &&
					retained+size > maxBytes {
					stopped = true
					return true, nil
				}
				retained += size
				page.Utxos = append(page.Utxos, models.UtxoWithOrdering{Utxo: u})
			}
			page.Scanned++
			page.Next = &models.UtxoOrderingCursor{
				OutputIdx: u.OutputIdx, TxId: u.TxId,
			}
			if len(page.Utxos) == q.Limit {
				stopped = true
			}
			return stopped, nil
		},
	)
	if err != nil {
		return page, err
	}
	// The walk ends on its own only when the set holds no further candidate.
	if !stopped {
		page.Next = nil
	}
	return page, ctx.Err()
}

// liveUtxoMatches reports whether a candidate output satisfies q's address
// patterns and asset filter, decoded from its CBOR.
func liveUtxoMatches(u *models.Utxo, q *models.UtxoWithOrderingQuery) (bool, error) {
	out, err := u.Decode()
	if err != nil {
		return false, fmt.Errorf(
			"decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err,
		)
	}
	if !q.MatchAllAddresses {
		matched := false
		for _, p := range q.AddressPatterns {
			ok, err := models.MatchesUtxoAddressPatterns(
				out.Address(), []models.UtxoAddressPattern{p},
			)
			if err != nil {
				return false, err
			}
			if ok && p.PaymentScript != nil && len(p.PaymentPart) > 0 &&
				models.PaymentScriptFromAddress(out.Address()) != *p.PaymentScript {
				ok = false
			}
			if ok {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	if !q.FilterByAsset {
		return true, nil
	}
	assets := out.Assets()
	if assets == nil {
		return false, nil
	}
	var policy lcommon.Blake2b224
	if len(q.AssetPolicyID) != len(policy) {
		return false, nil
	}
	copy(policy[:], q.AssetPolicyID)
	for _, name := range assets.Assets(policy) {
		if q.AssetName == nil || bytes.Equal(name, q.AssetName) {
			return true, nil
		}
	}
	return false, nil
}

// InlineDatums returns the inline datums of live outputs, one per hash and
// nil where no live output carries an inline datum with that hash. Datums
// that reached the chain only in a transaction witness set, or that only an
// output's datum hash names, are history the live set does not keep.
func (ls *LedgerState) InlineDatums(
	ctx context.Context,
	hashes [][]byte,
) ([]*models.Datum, error) {
	found := make([]*models.Datum, len(hashes))
	for i, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		body := indexBody(utxoIndexDatum, hash)
		err := ls.scanLive(
			[][]byte{body}, false, nil,
			func(u models.Utxo) (bool, error) {
				out, err := u.Decode()
				if err != nil {
					return false, fmt.Errorf(
						"decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err,
					)
				}
				raw := inlineDatumCbor(out)
				if len(raw) == 0 ||
					!bytes.Equal(lcommon.Blake2b256Hash(raw).Bytes(), hash) {
					return false, nil
				}
				found[i] = &models.Datum{Hash: bytes.Clone(hash), RawDatum: bytes.Clone(raw)}
				return true, nil
			},
		)
		if err != nil {
			return nil, err
		}
	}
	return found, nil
}
