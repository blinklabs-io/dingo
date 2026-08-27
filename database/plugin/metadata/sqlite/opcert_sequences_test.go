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

package sqlite

import (
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLatestPoolOpCertSequences covers the bulk read backing the
// GetChainDepState query's operational-certificate counters.
//
// The per-pool accessor answers the same question one pool at a time; this one
// has to agree with it for every pool at once, and has to reduce each pool's
// rows to the highest sequence rather than the newest, since the chain
// enforces the highest issue number it has accepted.
func TestLatestPoolOpCertSequences(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	poolA := bytes.Repeat([]byte{0xA1}, 28)
	poolB := bytes.Repeat([]byte{0xB2}, 28)
	pkhA := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolA))
	pkhB := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolB))

	// Pool A rotates certificates, with the highest issue number in the
	// middle: a query returning the newest row would report 4, not 9.
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 2, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 9, 20, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 4, 30, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 1, 15, nil))

	sequences, err := store.LatestPoolOpCertSequences(nil)
	require.NoError(t, err)

	assert.Equal(t, map[string]uint64{
		string(pkhA.Bytes()): 9,
		string(pkhB.Bytes()): 1,
	}, sequences)

	// The two accessors must not be able to disagree.
	for _, pkh := range []lcommon.PoolKeyHash{pkhA, pkhB} {
		single, found, err := store.LatestPoolOpCertSequence(pkh, nil)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, single, sequences[string(pkh.Bytes())],
			"bulk and per-pool reads must agree for pool %x", pkh.Bytes())
	}
}

// TestLatestPoolOpCertSequencesEmpty covers a chain on which no pool has
// issued a block. The caller builds a CBOR map from the result, and the node
// emits an empty map there rather than null, so an empty read is not an error.
func TestLatestPoolOpCertSequencesEmpty(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	sequences, err := store.LatestPoolOpCertSequences(nil)
	require.NoError(t, err)
	assert.Empty(t, sequences)
}

func TestLatestPoolOpCertSequenceAtOrBefore(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0xC3}, 28)
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkh, 2, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkh, 9, 20, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkh, 4, 30, nil))

	sequence, found, err := store.LatestPoolOpCertSequenceAtOrBefore(
		pkh,
		9,
		nil,
	)
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, sequence)

	sequence, found, err = store.LatestPoolOpCertSequenceAtOrBefore(
		pkh,
		20,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), sequence)

	sequence, found, err = store.LatestPoolOpCertSequenceAtOrBefore(
		pkh,
		30,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), sequence,
		"the highest accepted counter, not the newest row, is authoritative")
}
