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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// utxoWholeResult caches the wire representation. LocalStateQuery's server
// encoder calls MarshalCBOR after the query callback returns, so caching avoids
// rebuilding the complete response.
type utxoWholeResult struct {
	encoded []byte
}

func (r *utxoWholeResult) MarshalCBOR() ([]byte, error) {
	return r.encoded, nil
}

// bareResult returns the UTxO map without the one-element array that wraps
// the direct reply. GetCBOR carries the bare result inside tag 24, as every
// other GetCBOR-wrapped query does. The wrapper is a single definite-length
// array head byte, so the map follows it directly.
func (r *utxoWholeResult) bareResult() []byte {
	return r.encoded[1:]
}

type encodedUtxoWholeEntry struct {
	key   []byte
	value []byte
}

func appendCborHead(dst []byte, major byte, value uint64) []byte {
	switch {
	case value < 24:
		return append(dst, major<<5|byte(value))
	case value <= 0xff:
		return append(dst, major<<5|24, byte(value))
	case value <= 0xffff:
		dst = append(dst, major<<5|25, 0, 0)
		binary.BigEndian.PutUint16(dst[len(dst)-2:], uint16(value))
		return dst
	case value <= 0xffffffff:
		dst = append(dst, major<<5|26, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(dst[len(dst)-4:], uint32(value))
		return dst
	default:
		dst = append(dst, major<<5|27, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(dst[len(dst)-8:], value)
		return dst
	}
}

func encodeUtxoWholeResult(
	ctx context.Context,
	utxos map[olocalstatequery.UtxoId]ledger.TransactionOutput,
) ([]byte, error) {
	entries := make([]encodedUtxoWholeEntry, 0, len(utxos))
	for id, txOut := range utxos {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := id.MarshalCBOR()
		if err != nil {
			return nil, fmt.Errorf("encode whole UTxO key: %w", err)
		}
		value := txOut.Cbor()
		if len(value) == 0 {
			value, err = cbor.Encode(txOut)
			if err != nil {
				return nil, fmt.Errorf("encode whole UTxO output: %w", err)
			}
		}
		entries = append(entries, encodedUtxoWholeEntry{key: key, value: value})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// gouroboros uses fxamacker's Core Deterministic mode, which orders map
	// keys by their encoded bytes. Preserve that wire representation.
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})

	encoded := appendCborHead(nil, 4, 1)
	encoded = appendCborHead(encoded, 5, uint64(len(entries)))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		encoded = append(encoded, entry.key...)
		encoded = append(encoded, entry.value...)
	}
	return encoded, nil
}

// queryShelleyUtxoWhole answers GetUTxOWhole from the live UTxO set: every
// UTxO live at the tip, or at a pinned at as of that exact point.
//
// The successful response is always the complete set. GetUTxOWhole cannot
// apply item, byte, time, pagination, or offset limits because a partial or
// refused response based on result size breaks the protocol contract.
func (ls *LedgerState) queryShelleyUtxoWhole(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) (any, error) {
	// Detach an acquired view before waiting for admission. Queued whole-UTxO
	// queries must not retain database snapshots or read connections while the
	// admitted query completes.
	var detachedPoint *QueryPoint
	if detach := utxoWholeDetachFrom(ctx); detach != nil {
		if point, ok := detach(); ok {
			detachedPoint = &point
		}
	}
	releaseAdmission, err := ls.acquireUtxoWholeAdmission(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseAdmission()
	if detachedPoint != nil {
		at = *detachedPoint
		txn = nil
		// The point could be rolled back while the query waited for
		// admission.
		if err := ls.VerifyPointQueryable(ctx, nil, at); err != nil {
			return nil, err
		}
	}
	view, err := ls.liveUtxoView(at, txn)
	if err != nil {
		return nil, err
	}
	if view != nil {
		defer view.Close()
		return liveUtxoWhole(ctx, eachInView(view))
	}
	return liveUtxoWhole(ctx, func(fn func(*models.Utxo) error) error {
		return ls.iterateLiveUtxos(txn, fn)
	})
}

// acquireUtxoWholeAdmission serializes complete GetUTxOWhole
// materializations for one ledger state. Waiting follows the request context,
// so a disconnected client does not leave queued work behind. Admission never
// changes the reply: an admitted query still resolves and encodes every UTxO.
func (ls *LedgerState) acquireUtxoWholeAdmission(
	ctx context.Context,
) (func(), error) {
	ls.utxoWholeAdmissionOnce.Do(func() {
		ls.utxoWholeAdmission = make(chan struct{}, 1)
	})
	select {
	case ls.utxoWholeAdmission <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-ls.utxoWholeAdmission
			return nil, err
		}
		return func() { <-ls.utxoWholeAdmission }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// decodeUtxoWholeCborFunc is decodeUtxoWholeCbor by default. liveUtxoWhole
// calls it through this package-level variable so a test can substitute a
// stub, such as one that panics, without depending on a CBOR byte sequence
// that happens to panic the real decoder. Not t.Parallel-safe for a test
// that swaps it.
var decodeUtxoWholeCborFunc = decodeUtxoWholeCbor

// decodeUtxoWholeCbor decodes one UTxO's CBOR into GetUTxOWhole's reply
// shape.
func decodeUtxoWholeCbor(
	ref database.UtxoRef,
	cbor []byte,
) (ledger.TransactionOutput, error) {
	txOut, err := ledger.NewTransactionOutputFromCbor(cbor)
	if err != nil {
		// hex.EncodeToString rather than a %x-formatted slice of TxId: a
		// malformed row's TxId could be shorter than a fixed-length
		// slice bound, which would panic while building this very error
		// message.
		return nil, fmt.Errorf(
			"decode utxo %s#%d: %w",
			hex.EncodeToString(ref.TxId[:]),
			ref.OutputIdx,
			err,
		)
	}
	return localStateQueryTransactionOutput(txOut), nil
}

// liveUtxoWhole decodes and encodes the GetUTxOWhole reply for the outputs
// each yields. It decodes on the calling goroutine: the set hands over each
// output's CBOR already in memory, and a panic stays within the request's
// own recovery.
func liveUtxoWhole(
	ctx context.Context,
	each func(func(*models.Utxo) error) error,
) (result any, resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			resultErr = database.NewTxnPanicError(
				"iterate live UTxO set", recovered,
			)
		}
	}()
	ret := make(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	err := each(func(u *models.Utxo) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		txID, err := ledger.NewBlake2b256Checked(u.TxId)
		if err != nil {
			return fmt.Errorf("utxo ref tx id: %w", err)
		}
		ref := database.UtxoRef{TxId: txID, OutputIdx: u.OutputIdx}
		txOut, err := decodeUtxoWholeCborFunc(ref, u.Cbor)
		if err != nil {
			return err
		}
		ret[olocalstatequery.UtxoId{
			Hash: ledger.NewBlake2b256(ref.TxId[:]),
			Idx:  int(ref.OutputIdx),
		}] = txOut
		return nil
	})
	if err != nil {
		return nil, err
	}
	encoded, err := encodeUtxoWholeResult(ctx, ret)
	if err != nil {
		return nil, fmt.Errorf("encode whole UTxO result: %w", err)
	}
	return &utxoWholeResult{encoded: encoded}, nil
}
