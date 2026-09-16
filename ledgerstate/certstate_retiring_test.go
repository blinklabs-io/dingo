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

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
)

const testPoolDeposit = uint64(500_000_000)

// encodeCborMap encodes a definite-length CBOR map from alternating
// key/value arguments. Entries are emitted in the order given.
func encodeCborMap(t *testing.T, pairs ...any) []byte {
	t.Helper()

	if len(pairs)%2 != 0 {
		t.Fatalf(
			"encodeCborMap needs key/value pairs, got %d values",
			len(pairs),
		)
	}
	entries := len(pairs) / 2
	if entries > 23 {
		t.Fatalf(
			"encodeCborMap only encodes short maps, got %d entries",
			entries,
		)
	}

	data := []byte{byte(0xa0 + entries)}
	for _, item := range pairs {
		raw, err := cbor.Encode(item)
		if err != nil {
			t.Fatalf("encoding map item: %v", err)
		}
		data = append(data, raw...)
	}
	return data
}

// testPoolParams builds the pool params value for a pool map entry. The
// operator is omitted because it is already the map key.
func testPoolParams(seed byte) []any {
	return []any{
		bytes.Repeat([]byte{seed + 1}, 32), // vrf
		uint64(500_000_000),                // pledge
		uint64(340_000_000),                // cost
		[]uint64{1, 20},                    // margin
		[]any{
			uint64(0),
			[]any{uint64(0), bytes.Repeat([]byte{seed + 2}, 28)},
		}, // reward account
		[]any{bytes.Repeat([]byte{seed + 3}, 28)}, // owners
		[]any{}, // relays
		[]any{}, // metadata
		testPoolDeposit,
		[]any{},
	}
}

// encodeTestPState encodes a Shelley-shaped PState:
// [poolParams, futurePoolParams, retiring, poolDeposits].
func encodeTestPState(
	t *testing.T,
	poolParams, futureParams, retiring, deposits []byte,
) []byte {
	t.Helper()

	pstate, err := cbor.Encode([]any{
		cbor.RawMessage(poolParams),
		cbor.RawMessage(futureParams),
		cbor.RawMessage(retiring),
		cbor.RawMessage(deposits),
	})
	if err != nil {
		t.Fatalf("encoding PState: %v", err)
	}
	return pstate
}

func poolByHash(pools []ParsedPool, hash []byte) *ParsedPool {
	for i := range pools {
		if bytes.Equal(pools[i].PoolKeyHash, hash) {
			return &pools[i]
		}
	}
	return nil
}

// A pool scheduled to retire must carry its retirement epoch and keep its
// deposit: it stays live, earning rewards and leading slots, until the
// boundary actually refunds the deposit.
func TestParsePStateDecodesPendingRetirements(t *testing.T) {
	t.Parallel()

	retiringHash := bytes.Repeat([]byte{0x11}, 28)
	stayingHash := bytes.Repeat([]byte{0x21}, 28)

	poolParams := encodeCborMap(
		t,
		retiringHash, testPoolParams(0x11),
		stayingHash, testPoolParams(0x21),
	)
	emptyMap := encodeCborMap(t)
	retiring := encodeCborMap(t, retiringHash, uint64(658))
	deposits := encodeCborMap(
		t,
		retiringHash, testPoolDeposit,
		stayingHash, testPoolDeposit,
	)

	pools, err := parsePState(
		encodeTestPState(t, poolParams, emptyMap, retiring, deposits),
	)
	if err != nil {
		t.Fatalf("parsePState failed: %v", err)
	}
	if len(pools) != 2 {
		t.Fatalf("expected 2 pools, got %d", len(pools))
	}

	got := poolByHash(pools, retiringHash)
	if got == nil {
		t.Fatalf("retiring pool %x not parsed", retiringHash)
	}
	if got.RetiringEpoch == nil {
		t.Fatalf("expected a retirement epoch for pool %x", retiringHash)
	}
	if *got.RetiringEpoch != 658 {
		t.Fatalf(
			"retirement epoch mismatch: got %d, want 658",
			*got.RetiringEpoch,
		)
	}
	if got.Deposit != testPoolDeposit {
		t.Fatalf(
			"retiring pool lost its deposit: got %d, want %d",
			got.Deposit,
			testPoolDeposit,
		)
	}

	other := poolByHash(pools, stayingHash)
	if other == nil {
		t.Fatalf("pool %x not parsed", stayingHash)
	}
	if other.RetiringEpoch != nil {
		t.Fatalf(
			"pool %x is not retiring but got epoch %d",
			stayingHash,
			*other.RetiringEpoch,
		)
	}
	if other.Deposit != testPoolDeposit {
		t.Fatalf(
			"deposit mismatch: got %d, want %d",
			other.Deposit,
			testPoolDeposit,
		)
	}
}

// The deposits map has the same pool-key-hash -> uint64 shape as the
// retiring map, so it must never be mistaken for one.
func TestParsePStateDoesNotReadDepositsAsRetirements(t *testing.T) {
	t.Parallel()

	poolHash := bytes.Repeat([]byte{0x11}, 28)
	poolParams := encodeCborMap(t, poolHash, testPoolParams(0x11))
	emptyMap := encodeCborMap(t)
	deposits := encodeCborMap(t, poolHash, testPoolDeposit)

	pools, err := parsePState(
		encodeTestPState(t, poolParams, emptyMap, emptyMap, deposits),
	)
	if err != nil {
		t.Fatalf("parsePState failed: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}
	if pools[0].RetiringEpoch != nil {
		t.Fatalf(
			"deposits map read as a retirement epoch: %d",
			*pools[0].RetiringEpoch,
		)
	}
	if pools[0].Deposit != testPoolDeposit {
		t.Fatalf(
			"deposit mismatch: got %d, want %d",
			pools[0].Deposit,
			testPoolDeposit,
		)
	}
}

// cardano-ledger removes a pool from psRetiring and psStakePoolParams
// together, so every retiring key names a registered pool. A map holding an
// unknown key is some other small-uint map and must be rejected rather than
// scheduling a retirement no pool asked for.
func TestParsePStateRejectsRetiringMapWithUnknownPool(t *testing.T) {
	t.Parallel()

	poolHash := bytes.Repeat([]byte{0x11}, 28)
	strangerHash := bytes.Repeat([]byte{0x77}, 28)

	poolParams := encodeCborMap(t, poolHash, testPoolParams(0x11))
	emptyMap := encodeCborMap(t)
	notRetiring := encodeCborMap(
		t,
		poolHash, uint64(658),
		strangerHash, uint64(3),
	)
	deposits := encodeCborMap(t, poolHash, testPoolDeposit)

	pools, err := parsePState(
		encodeTestPState(t, poolParams, emptyMap, notRetiring, deposits),
	)
	if err != nil {
		t.Fatalf("parsePState failed: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}
	if pools[0].RetiringEpoch != nil {
		t.Fatalf(
			"unknown-pool map accepted as retiring: epoch %d",
			*pools[0].RetiringEpoch,
		)
	}
}

// The Conway PState is a seven-element array whose order is not fixed, so
// the retiring map must still be found by shape.
func TestParsePStateConwayDecodesPendingRetirements(t *testing.T) {
	t.Parallel()

	poolHash := bytes.Repeat([]byte{0x11}, 28)
	poolParams := encodeCborMap(t, poolHash, testPoolParams(0x11))
	emptyMap := encodeCborMap(t)
	deposits := encodeCborMap(t, poolHash, testPoolDeposit)
	retiring := encodeCborMap(t, poolHash, uint64(658))

	pstate, err := cbor.Encode([]any{
		cbor.RawMessage(emptyMap),
		cbor.RawMessage(deposits),
		cbor.RawMessage(poolParams),
		cbor.RawMessage(emptyMap),
		cbor.RawMessage(retiring),
		cbor.RawMessage(emptyMap),
		cbor.RawMessage(emptyMap),
	})
	if err != nil {
		t.Fatalf("encoding Conway PState: %v", err)
	}

	pools, err := parsePStateConway(pstate)
	if err != nil {
		t.Fatalf("parsePStateConway failed: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}
	if pools[0].RetiringEpoch == nil {
		t.Fatalf("expected a retirement epoch for pool %x", poolHash)
	}
	if *pools[0].RetiringEpoch != 658 {
		t.Fatalf(
			"retirement epoch mismatch: got %d, want 658",
			*pools[0].RetiringEpoch,
		)
	}
	if pools[0].Deposit != testPoolDeposit {
		t.Fatalf(
			"deposit mismatch: got %d, want %d",
			pools[0].Deposit,
			testPoolDeposit,
		)
	}
}
