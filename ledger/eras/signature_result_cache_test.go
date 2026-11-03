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

package eras

import (
	"crypto/ed25519"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// signedConwayTestTransaction returns a Conway transaction with no inputs
// whose body is set by fee and which carries one vkey witness from the key
// seedByte selects. corrupt flips a bit of the signature.
func signedConwayTestTransaction(
	t *testing.T,
	fee uint64,
	seedByte byte,
	corrupt bool,
) *conway.ConwayTransaction {
	t.Helper()
	tx := &conway.ConwayTransaction{}
	tx.Body.TxFee = fee
	bodyCbor, err := cbor.Encode(&tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	seed := make([]byte, ed25519.SeedSize)
	seed[len(seed)-1] = seedByte
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	require.True(t, ok)
	txHash := tx.Hash()
	signature := ed25519.Sign(privateKey, txHash[:])
	if corrupt {
		signature[0] ^= 0x01
	}
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{Vkey: publicKey, Signature: signature}},
		false,
	)
	return tx
}

type signatureCachingLedgerState struct {
	*mockLedgerState
	cache *SignatureResultCache
}

func (s signatureCachingLedgerState) SignatureResultCache() *SignatureResultCache {
	return s.cache
}

// countSignatureVerifications replaces preverifySignaturesFunc with a
// wrapper that counts its calls, for the duration of the test. Tests that use
// it must not run in parallel.
func countSignatureVerifications(t *testing.T) *int {
	t.Helper()
	calls := 0
	original := preverifySignaturesFunc
	preverifySignaturesFunc = func(
		tx lcommon.Transaction,
	) *lcommon.PreverifiedSignatures {
		calls++
		return original(tx)
	}
	t.Cleanup(func() { preverifySignaturesFunc = original })
	return &calls
}

func TestSignatureResultCacheVerifiesEachTransactionOnce(t *testing.T) {
	calls := countSignatureVerifications(t)
	ls := signatureCachingLedgerState{
		mockLedgerState: newMockLedgerState(),
		cache:           NewSignatureResultCache(),
	}
	rule := reusePreverifiedSignatures(lcommon.UtxoValidateSignatures)
	tx := signedConwayTestTransaction(t, 1, 1, false)
	for range 3 {
		require.NoError(t, rule(tx, 0, ls, nil))
	}
	require.Equal(t, 1, *calls, "signatures must be verified once")

	// Without a cache every validation verifies inline: a corrupted
	// signature still fails, and the cache's verifier is never called.
	*calls = 0
	forged := signedConwayTestTransaction(t, 1, 1, true)
	for range 2 {
		require.NoError(t, rule(tx, 0, newMockLedgerState(), nil))
		require.Error(t, rule(forged, 0, newMockLedgerState(), nil))
	}
	require.Zero(t, *calls)
}

func TestSignatureResultCacheZeroValueIsUsable(t *testing.T) {
	var c SignatureResultCache
	tx := signedConwayTestTransaction(t, 4, 1, false)
	first := c.Verify(tx)
	require.NotNil(t, first)
	require.Same(t, first, c.Verify(tx))
}

func TestSignatureResultCacheResultCoversOnlyItsWitnesses(t *testing.T) {
	ls := signatureCachingLedgerState{
		mockLedgerState: newMockLedgerState(),
		cache:           NewSignatureResultCache(),
	}
	rule := reusePreverifiedSignatures(lcommon.UtxoValidateSignatures)
	good := signedConwayTestTransaction(t, 2, 1, false)
	forged := signedConwayTestTransaction(t, 2, 1, true)
	require.Equal(t, good.Hash(), forged.Hash(), "the bodies must match")

	// A cached success must not vouch for a corrupted signature over the
	// same body, nor a cached failure condemn a valid one.
	require.NoError(t, rule(good, 0, ls, nil))
	require.Error(t, rule(forged, 0, ls, nil))
	require.NoError(t, rule(good, 0, ls, nil))
	require.Error(t, rule(forged, 0, ls, nil))
}

func TestSignatureResultCacheKeepsRecentEntries(t *testing.T) {
	calls := countSignatureVerifications(t)
	c := NewSignatureResultCache()
	txs := make([]lcommon.Transaction, 2*signatureResultCacheGeneration+1)
	for i := range txs {
		// Unsigned transactions verify trivially, which keeps this fast.
		tx := &conway.ConwayTransaction{}
		tx.Body.TxFee = uint64(i) + 1000 // #nosec G115 -- small test index
		bodyCbor, err := cbor.Encode(&tx.Body)
		require.NoError(t, err)
		tx.Body.SetCbor(bodyCbor)
		txs[i] = tx
	}
	for i := range signatureResultCacheGeneration {
		c.Verify(txs[i])
	}
	for i := signatureResultCacheGeneration; i < 2*signatureResultCacheGeneration; i++ {
		c.Verify(txs[i])
		if i == signatureResultCacheGeneration {
			c.Verify(txs[0])
		}
	}
	// One more starts a third generation and drops the first, except txs[0],
	// which the lookup moved forward.
	c.Verify(txs[2*signatureResultCacheGeneration])
	*calls = 0
	c.Verify(txs[0])
	c.Verify(txs[2*signatureResultCacheGeneration-1])
	require.Zero(t, *calls)
	c.Verify(txs[1])
	require.Equal(t, 1, *calls, "the oldest entries must be dropped")
}

func TestSignatureResultCacheNilVerifiesWithoutCaching(t *testing.T) {
	calls := countSignatureVerifications(t)
	var c *SignatureResultCache
	tx := signedConwayTestTransaction(t, 3, 1, false)
	require.NotNil(t, c.Verify(tx))
	require.NotNil(t, c.Verify(tx))
	require.Equal(t, 2, *calls)
}
