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
	"github.com/blinklabs-io/dingo/database/types"
)

// The payload columns (datum.raw_datum, script.content,
// applied_transaction.metadata, transaction_metadata_label.cbor_value,
// pparam_update.cbor and the Midnight datum columns) hold the CBOR offset of
// the bytes in the block that carried them, not a copy. A write names the
// payload by its bytes and the store turns them into an offset through the
// locator of the block being written; a read turns the offset back into the
// bytes through the resolver.

var (
	_ types.PayloadResolverSetter = (*Store)(nil)
	_ types.PayloadLocatorSetter  = (*sqlTxn)(nil)
	_ types.PayloadLocatorSource  = (*sqlTxn)(nil)
	_ types.PayloadLocatorSetter  = (*transactionBatchAccumulator)(nil)
)

// SetPayloadResolver installs the block source the readers resolve payload
// offsets through. Call it before the first read.
func (s *Store) SetPayloadResolver(resolver types.PayloadResolver) {
	s.payloads = resolver
}

// SetPayloadLocator sets the block the next writes of this transaction locate
// their payloads in.
func (t *sqlTxn) SetPayloadLocator(locator types.PayloadLocator) {
	t.mu.Lock()
	t.locator = locator
	t.mu.Unlock()
}

// PayloadLocator returns the locator last set on the transaction, or nil.
func (t *sqlTxn) PayloadLocator() types.PayloadLocator {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.locator
}

// SetPayloadLocator sets the block the next transactions applied through this
// accumulator locate their payloads in.
func (a *transactionBatchAccumulator) SetPayloadLocator(
	locator types.PayloadLocator,
) {
	a.locator = locator
}

// payloadLocatorFor returns the locator the write carries: the accumulator's
// when it has one, the transaction's otherwise.
func payloadLocatorFor(
	txn types.Txn,
	acc *transactionBatchAccumulator,
) types.PayloadLocator {
	if acc != nil && acc.locator != nil {
		return acc.locator
	}
	if t, ok := txn.(*sqlTxn); ok && t != nil {
		return t.PayloadLocator()
	}
	return nil
}

// payloadLocate returns the function that turns payload bytes into the
// encoded offset a column holds. An empty payload stays empty, which is how
// a nullable column records no payload.
func payloadLocate(
	locator types.PayloadLocator,
) locateFunc {
	return func(payload []byte, container ...[]byte) ([]byte, error) {
		if len(payload) == 0 {
			return nil, nil
		}
		if locator == nil {
			return nil, types.ErrPayloadLocatorRequired
		}
		if contained, ok := locator.(types.ContainedPayloadLocator); ok &&
			len(container) > 0 && len(container[0]) > 0 {
			return contained.LocatePayloadIn(payload, container[0])
		}
		return locator.LocatePayload(payload)
	}
}

// locateFunc turns payload bytes into an encoded offset. The optional
// container is the CBOR of the output that carries the payload, which the
// offset is then taken inside.
type locateFunc = func(payload []byte, container ...[]byte) ([]byte, error)

// resolvePayload returns the bytes a payload column's offset names. An empty
// column holds no payload.
func (s *Store) resolvePayload(offset []byte) ([]byte, error) {
	if len(offset) == 0 {
		return nil, nil
	}
	if s.payloads == nil {
		return nil, types.ErrPayloadResolverRequired
	}
	return s.payloads.ResolvePayload(offset)
}

// payloadOffset returns the offset a payload column records: the one the row
// already carries because it was read from the store, else the payload's
// position in the block being written. An empty payload has no offset.
func payloadOffset(txn types.Txn, known, payload []byte) ([]byte, error) {
	if len(known) > 0 {
		return known, nil
	}
	return payloadLocate(payloadLocatorFor(txn, nil))(payload)
}

// readPayload returns the stored offset of a payload column and the bytes it
// names.
func (s *Store) readPayload(column []byte) (offset, payload []byte, err error) {
	payload, err = s.resolvePayload(column)
	if err != nil {
		return nil, nil, err
	}
	return column, payload, nil
}
