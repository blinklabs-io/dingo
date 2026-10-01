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
	"math/big"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withCountingEvalContextConstructor replaces the package-level
// newEvalContextFunc seam with a wrapper that counts real cek.NewEvalContext
// calls and forwards to the original, restoring it on cleanup. Every test
// using it swaps process/package-global state, so none of them call
// t.Parallel.
func withCountingEvalContextConstructor(t *testing.T) *atomic.Int64 {
	t.Helper()
	var calls atomic.Int64
	orig := newEvalContextFunc
	newEvalContextFunc = func(
		version lang.LanguageVersion,
		protoVersion cek.ProtoVersion,
		costModelParams []int64,
	) (*cek.EvalContext, error) {
		calls.Add(1)
		return orig(version, protoVersion, costModelParams)
	}
	t.Cleanup(func() { newEvalContextFunc = orig })
	return &calls
}

// TestPlutusEvalContextCacheBuildsOncePerKeyConcurrently proves the
// acceptance criterion directly: cek.NewEvalContext is called at most once
// per distinct (language version, protocol major, cost-model list,
// synthetic-V2 flag) key, even when many goroutines race to resolve the same
// key at once (TestEvalContextReuseIsRaceFree in plutigo covers reuse of an
// already-built *cek.EvalContext; this covers the construction race this
// cache itself introduces).
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextCacheBuildsOncePerKeyConcurrently(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)
	cache := NewPlutusEvalContextCache()
	costModel := []int64{1, 2, 3, 4, 5}
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}

	const goroutines = 32
	results := make([]*cek.EvalContext, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			ctx, err := cache.get(
				lang.LanguageVersionV2,
				protoVersion,
				costModel,
				false,
			)
			assert.NoError(t, err)
			results[i] = ctx
		}(i)
	}
	wg.Wait()

	require.Equal(
		t,
		int64(1),
		calls.Load(),
		"cek.NewEvalContext must be built at most once per distinct key",
	)
	for i, ctx := range results {
		require.NotNil(t, ctx, "goroutine %d got a nil EvalContext", i)
		require.Same(
			t,
			results[0],
			ctx,
			"every caller sharing a key must observe the same *cek.EvalContext",
		)
	}
}

// TestPlutusEvalContextCacheKeyIsCollisionFree proves the cost-model
// component of the cache key reproduces the full parameter list rather than
// a digest or truncated form. []int64{1, 23} and []int64{12, 3} render to the
// same digit sequence ("123") if concatenated without a delimiter, which
// would wrongly collide two distinct cost-model lists into one cache entry.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextCacheKeyIsCollisionFree(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)
	cache := NewPlutusEvalContextCache()
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}

	ctxA, err := cache.get(
		lang.LanguageVersionV2,
		protoVersion,
		[]int64{1, 23},
		false,
	)
	require.NoError(t, err)
	ctxB, err := cache.get(
		lang.LanguageVersionV2,
		protoVersion,
		[]int64{12, 3},
		false,
	)
	require.NoError(t, err)

	assert.Equal(
		t,
		int64(2),
		calls.Load(),
		"two distinct cost-model lists must build two distinct entries",
	)
	assert.NotSame(
		t,
		ctxA,
		ctxB,
		"[]int64{1, 23} and []int64{12, 3} must not share a cache entry",
	)
}

// TestPlutusEvalContextCacheKeyCoversEveryComponent proves each of the four
// key components (language version, protocol major, cost model, synthetic-V2
// flag) independently distinguishes cache entries: changing only one
// component while holding the other three fixed must still miss the cache.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextCacheKeyCoversEveryComponent(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)
	cache := NewPlutusEvalContextCache()
	baseCostModel := []int64{7, 8, 9}
	baseProto := cek.ProtoVersion{Major: 9, Minor: 0}

	_, err := cache.get(lang.LanguageVersionV1, baseProto, baseCostModel, false)
	require.NoError(t, err)
	_, err = cache.get(lang.LanguageVersionV2, baseProto, baseCostModel, false)
	require.NoError(t, err)
	_, err = cache.get(
		lang.LanguageVersionV1,
		cek.ProtoVersion{Major: 10, Minor: 0},
		baseCostModel,
		false,
	)
	require.NoError(t, err)
	_, err = cache.get(
		lang.LanguageVersionV1,
		baseProto,
		[]int64{7, 8, 10},
		false,
	)
	require.NoError(t, err)
	_, err = cache.get(lang.LanguageVersionV1, baseProto, baseCostModel, true)
	require.NoError(t, err)

	assert.Equal(
		t,
		int64(5),
		calls.Load(),
		"changing any one of language version, protocol major, cost model, "+
			"or synthetic-V2 flag must produce a distinct entry",
	)

	// Repeating the original key must still hit the cache (no 6th build).
	_, err = cache.get(lang.LanguageVersionV1, baseProto, baseCostModel, false)
	require.NoError(t, err)
	assert.Equal(t, int64(5), calls.Load())
}

// TestPlutusEvalContextFallsBackWithoutProvider proves plutusEvalContext
// builds an uncached *cek.EvalContext -- identical to this cache's absence --
// when ls does not implement PlutusEvalContextCacheProvider, or implements it
// but returns a nil cache. This is the fallback every existing LedgerState
// test stand-in that doesn't opt in relies on.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextFallsBackWithoutProvider(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}
	costModel := []int64{1, 2, 3}

	ls := newMockLedgerState() // plutusEvalContextCache is nil (zero value)
	_, err := plutusEvalContext(
		ls,
		lang.LanguageVersionV1,
		protoVersion,
		costModel,
		false,
	)
	require.NoError(t, err)
	_, err = plutusEvalContext(
		ls,
		lang.LanguageVersionV1,
		protoVersion,
		costModel,
		false,
	)
	require.NoError(t, err)

	assert.Equal(
		t,
		int64(2),
		calls.Load(),
		"without a provided cache, every call must build its own EvalContext",
	)
}

// TestPlutusEvalContextUsesProviderCache proves plutusEvalContext resolves
// and reuses ls's cache when ls implements PlutusEvalContextCacheProvider
// with a non-nil cache.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextUsesProviderCache(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}
	costModel := []int64{1, 2, 3}

	ls := newMockLedgerState()
	ls.plutusEvalContextCache = NewPlutusEvalContextCache()
	for range 5 {
		_, err := plutusEvalContext(
			ls,
			lang.LanguageVersionV1,
			protoVersion,
			costModel,
			false,
		)
		require.NoError(t, err)
	}

	assert.Equal(
		t,
		int64(1),
		calls.Load(),
		"5 calls sharing a key through the same provided cache must build once",
	)
}

// buildMintingV1Script returns a trivial always-succeeding PlutusV1 script
// (a two-argument constant function, matching the shape a minting policy
// evaluates: redeemer and script context) along with its hash.
func buildMintingV1Script(
	t testing.TB,
) (lcommon.PlutusV1Script, lcommon.ScriptHash) {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Constant{Con: &syn.Unit{}},
			},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	plutusScript := lcommon.PlutusV1Script(scriptBytes)
	return plutusScript, plutusScript.Hash()
}

// TestConwayEvalContextCacheReusedAcrossValidateAndEvaluate is the
// production-path regression test for this issue: ValidateTxConway (phase-2
// restrictive-budget validation) and EvaluateTxConway (fee/ExUnits
// evaluation) are two entirely independent call sites, each building its own
// per-tx txInfoCache, that both reach evaluateConwayPlutusScript's V1 branch
// for the same protocol-parameter snapshot. Before this change each call
// built its own *cek.EvalContext; sharing ls's PlutusEvalContextCache across
// both must reduce the total construction count to 1.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestConwayEvalContextCacheReusedAcrossValidateAndEvaluate(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)

	plutusScript, scriptHash := buildMintingV1Script(t)
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
				redeemers: &mockRedeemers{
					entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagMint,
								Index: 0,
							},
							val: lcommon.RedeemerValue{
								// Generous declared budget: the point of
								// this test is cache reuse, not budget
								// comparison, so both the restrictive and
								// exact evaluation paths must succeed
								// cleanly.
								ExUnits: lcommon.ExUnits{
									Steps:  1_000_000,
									Memory: 1_000_000,
								},
							},
						},
					},
				},
			},
		},
		assetMint: &assetMint,
	}

	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	ls := newMockLedgerState()
	ls.plutusEvalContextCache = NewPlutusEvalContextCache()
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		MaxTxExUnits: lcommon.ExUnits{
			Steps:  1_000_000,
			Memory: 1_000_000,
		},
		CostModels: map[uint][]int64{
			0: defaultMachineCostModel(t, lang.LanguageVersionV1),
		},
	}

	err := ValidateTxConway(tx, 0, ls, pp)
	require.NoError(t, err)

	_, _, _, err = EvaluateTxConway(tx, ls, pp)
	require.NoError(t, err)

	assert.Equal(
		t,
		int64(1),
		calls.Load(),
		"ValidateTxConway and EvaluateTxConway sharing ls's cache must "+
			"build cek.NewEvalContext only once between them",
	)
}

// TestConwayEvalContextCacheNotSharedAcrossDifferentProtocolParams is the
// era-boundary regression test the acceptance criteria calls for: it proves
// the cache never conflates two different protocol-parameter snapshots (such
// as an era boundary's current vs. previous-era pparams) into one entry --
// each distinct cost model must still build its own *cek.EvalContext, even
// though both calls share the same ls and its cache.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestConwayEvalContextCacheNotSharedAcrossDifferentProtocolParams(
	t *testing.T,
) {
	calls := withCountingEvalContextConstructor(t)

	plutusScript, scriptHash := buildMintingV1Script(t)
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	newTx := func() *mockConwayFeeTx {
		return &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType: txTypeAlonzo,
				witnesses: &mockWitnessSet{
					plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								key: lcommon.RedeemerKey{
									Tag:   lcommon.RedeemerTagMint,
									Index: 0,
								},
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Steps:  1_000_000,
										Memory: 1_000_000,
									},
								},
							},
						},
					},
				},
			},
			assetMint: &assetMint,
		}
	}

	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	ls := newMockLedgerState()
	ls.plutusEvalContextCache = NewPlutusEvalContextCache()

	// currentEraPParams: protocol major 9, one cost model.
	currentEraPParams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 9},
		MaxTxExUnits: lcommon.ExUnits{
			Steps:  1_000_000,
			Memory: 1_000_000,
		},
		CostModels: map[uint][]int64{
			0: defaultMachineCostModel(t, lang.LanguageVersionV1),
		},
	}
	// prevEraPParams: same major version, but a different cost model, as a
	// governance-enacted cost-model change at the era boundary would produce.
	prevEraCostModel := append(
		[]int64(nil),
		defaultMachineCostModel(t, lang.LanguageVersionV1)...,
	)
	prevEraCostModel[0]++
	prevEraPParams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 9},
		MaxTxExUnits: lcommon.ExUnits{
			Steps:  1_000_000,
			Memory: 1_000_000,
		},
		CostModels: map[uint][]int64{
			0: prevEraCostModel,
		},
	}

	require.NoError(t, ValidateTxConway(newTx(), 0, ls, currentEraPParams))
	require.NoError(t, ValidateTxConway(newTx(), 0, ls, prevEraPParams))

	assert.Equal(
		t,
		int64(2),
		calls.Load(),
		"two protocol-parameter snapshots with different cost models must "+
			"never share a cached EvalContext, even via the same ls",
	)

	// Revalidating under currentEraPParams again must still hit the cache
	// (no leaked/overwritten entry from the prevEraPParams call).
	require.NoError(t, ValidateTxConway(newTx(), 0, ls, currentEraPParams))
	assert.Equal(t, int64(2), calls.Load())
}

// TestAlonzoBabbageEvalContextCacheReusedAcrossValidateAndEvaluate covers the
// six Alonzo and Babbage call sites the Conway test above does not reach:
// each era's validate and evaluate paths must share one construction per key
// through ls's cache.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam and the
// era rule tables.
func TestAlonzoBabbageEvalContextCacheReusedAcrossValidateAndEvaluate(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)
	origAlonzo := alonzoUtxoValidationRules
	t.Cleanup(func() { alonzoUtxoValidationRules = origAlonzo })
	alonzoUtxoValidationRules = nil

	exUnits := lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000}
	testCases := []struct {
		name     string
		version  lang.LanguageVersion
		validate func(lcommon.Transaction, lcommon.LedgerState) error
		evaluate func(lcommon.Transaction, lcommon.LedgerState) error
	}{
		{
			name:    "alonzo v1",
			version: lang.LanguageVersionV1,
			validate: func(tx lcommon.Transaction, ls lcommon.LedgerState) error {
				return ValidateTxAlonzo(
					tx,
					0,
					ls,
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 6,
						MaxTxExUnits:  exUnits,
						CostModels: map[uint][]int64{
							0: defaultMachineCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
			},
			evaluate: func(tx lcommon.Transaction, ls lcommon.LedgerState) error {
				_, _, _, err := EvaluateTxAlonzo(
					tx,
					ls,
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 6,
						MaxTxExUnits:  exUnits,
						CostModels: map[uint][]int64{
							0: defaultMachineCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
				return err
			},
		},
		{
			name:    "babbage v1",
			version: lang.LanguageVersionV1,
		},
		{
			name:    "babbage v2",
			version: lang.LanguageVersionV2,
		},
	}
	babbagePParams := func() *babbage.BabbageProtocolParameters {
		return &babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits:  exUnits,
			CostModels: map[uint][]int64{
				0: defaultMachineCostModel(t, lang.LanguageVersionV1),
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		}
	}
	for i := range testCases {
		if testCases[i].validate != nil {
			continue
		}
		testCases[i].validate = func(tx lcommon.Transaction, ls lcommon.LedgerState) error {
			return ValidateTxBabbage(tx, 0, ls, babbagePParams())
		}
		testCases[i].evaluate = func(tx lcommon.Transaction, ls lcommon.LedgerState) error {
			_, _, _, err := EvaluateTxBabbage(tx, ls, babbagePParams())
			return err
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calls := withCountingEvalContextConstructor(t)
			ls := newMockLedgerState()
			ls.plutusEvalContextCache = NewPlutusEvalContextCache()
			tx := newConwayValidityOutcomeTx(
				t,
				true,
				tc.version,
				false,
				exUnits,
			)

			require.NoError(t, tc.validate(tx, ls))
			require.NoError(t, tc.evaluate(tx, ls))
			assert.Equal(
				t,
				int64(1),
				calls.Load(),
				"validate and evaluate sharing ls's cache must build once",
			)
		})
	}
}

// TestPlutusEvalContextCacheKeyDistinguishesListLength proves a cost-model
// list and its zero-extended form build separate entries: plutigo costs a
// parameter missing from a short list at maxBound, so the two lists build
// different contexts and must never share one.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextCacheKeyDistinguishesListLength(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)
	cache := NewPlutusEvalContextCache()
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}

	short, err := cache.get(
		lang.LanguageVersionV2,
		protoVersion,
		[]int64{1, 2},
		false,
	)
	require.NoError(t, err)
	long, err := cache.get(
		lang.LanguageVersionV2,
		protoVersion,
		[]int64{1, 2, 0},
		false,
	)
	require.NoError(t, err)

	assert.Equal(t, int64(2), calls.Load())
	assert.NotSame(t, short, long)
}

// TestPlutusEvalContextCacheEntryAfterPanicReturnsError proves a
// construction that panics does not leave an entry reporting success with a
// nil context: sync.Once marks the entry done even when its function panics,
// so every later caller for the key must see an error.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextCacheEntryAfterPanicReturnsError(t *testing.T) {
	orig := newEvalContextFunc
	t.Cleanup(func() { newEvalContextFunc = orig })
	newEvalContextFunc = func(
		lang.LanguageVersion,
		cek.ProtoVersion,
		[]int64,
	) (*cek.EvalContext, error) {
		panic("construction failed")
	}
	cache := NewPlutusEvalContextCache()
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}
	costModel := []int64{1, 2, 3}

	require.Panics(t, func() {
		_, _ = cache.get(lang.LanguageVersionV1, protoVersion, costModel, false)
	})
	ctx, err := cache.get(
		lang.LanguageVersionV1,
		protoVersion,
		costModel,
		false,
	)
	require.Error(t, err)
	assert.Nil(t, ctx)
}

// evalContextCacheTestCostModel returns a cost-model list unique to i, so
// each i is a distinct cache key.
func evalContextCacheTestCostModel(i int) []int64 {
	return []int64{1, 2, 3, int64(i)}
}

func evalContextCacheLen(c *PlutusEvalContextCache) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// TestPlutusEvalContextCacheBoundsEntries proves a stream of distinct keys,
// such as successive governance cost-model updates, cannot grow the cache
// past plutusEvalContextCacheMaxEntries.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextCacheBoundsEntries(t *testing.T) {
	withCountingEvalContextConstructor(t)
	cache := NewPlutusEvalContextCache()
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}
	for i := range plutusEvalContextCacheMaxEntries * 3 {
		ctx, err := cache.get(
			lang.LanguageVersionV2,
			protoVersion,
			evalContextCacheTestCostModel(i),
			false,
		)
		require.NoError(t, err)
		require.NotNil(t, ctx)
		require.LessOrEqual(
			t,
			evalContextCacheLen(cache),
			plutusEvalContextCacheMaxEntries,
		)
	}
	require.Equal(
		t,
		plutusEvalContextCacheMaxEntries,
		evalContextCacheLen(cache),
	)
}

// TestPlutusEvalContextCacheEvictsLeastRecentlyUsed proves admission to a
// full cache evicts the least recently used key, not a key still in use, and
// that an evicted key is rebuilt on its next lookup.
//
// Not t.Parallel: swaps the package-level newEvalContextFunc seam.
func TestPlutusEvalContextCacheEvictsLeastRecentlyUsed(t *testing.T) {
	calls := withCountingEvalContextConstructor(t)
	cache := NewPlutusEvalContextCache()
	protoVersion := cek.ProtoVersion{Major: 10, Minor: 0}
	get := func(i int) *cek.EvalContext {
		t.Helper()
		ctx, err := cache.get(
			lang.LanguageVersionV2,
			protoVersion,
			evalContextCacheTestCostModel(i),
			false,
		)
		require.NoError(t, err)
		require.NotNil(t, ctx)
		return ctx
	}
	first := make([]*cek.EvalContext, plutusEvalContextCacheMaxEntries)
	for i := range plutusEvalContextCacheMaxEntries {
		first[i] = get(i)
	}
	// Key 0 is the oldest insertion but the most recent use, so key 1 is the
	// least recently used when the new key is admitted.
	require.Same(t, first[0], get(0))
	get(plutusEvalContextCacheMaxEntries)
	built := calls.Load()

	require.Same(t, first[0], get(0), "a recently used key must survive")
	require.Equal(t, built, calls.Load())

	rebuilt := get(1)
	require.Equal(t, built+1, calls.Load(), "the evicted key must rebuild")
	require.NotSame(t, first[1], rebuilt)
}

// BenchmarkPlutusEvalContextPerBlock measures EvalContext construction for
// one block's worth of same-version PlutusV3 redeemers. The cached case
// starts every block from an empty cache, so it pays one build per block and
// never benefits from reuse across blocks.
func BenchmarkPlutusEvalContextPerBlock(b *testing.B) {
	const redeemersPerBlock = 16
	costModel := defaultMachineCostModel(b, lang.LanguageVersionV3)
	protoVersion := cek.ProtoVersion{Major: 10}

	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for range redeemersPerBlock {
				if _, err := cek.NewEvalContext(
					lang.LanguageVersionV3,
					protoVersion,
					costModel,
				); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cache := NewPlutusEvalContextCache()
			for range redeemersPerBlock {
				if _, err := cache.get(
					lang.LanguageVersionV3,
					protoVersion,
					costModel,
					false,
				); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
