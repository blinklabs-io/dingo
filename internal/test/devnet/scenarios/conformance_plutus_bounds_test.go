//go:build linux && devnet && devnet_conformance

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

package scenarios

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// This scenario checks the protocol-version dependent limits of the Plutus
// evaluator against a live cardano-node. From protocol version 11 integer
// builtin arguments must lie in [-(2^262143), 2^262143-1] and byte string
// arguments must be at most 65536 bytes, and all builtins (and Case/Constr)
// become available to PlutusV1/V2/V3. Each case is a one-input script
// spend that is submitted to both producers; the test asserts that Dingo
// and cardano-node give the same verdict, and reports whether that verdict
// matches what plutigo assumes for the protocol version the network runs.
//
// Run it with a conformance spec whose Shelley genesis starts at the wanted
// protocol version, for example:
//
//	DEVNET_SPEC_OVERRIDE=./testnet-accelerated-pv11.yaml \
//	  internal/test/devnet/run-tests.sh --conformance --accelerated \
//	  -run TestPlutusBoundedArgsConformance

// boundsCase is one script spend. The accept fields are what plutigo
// assumes for protocol versions 10 and 11.
type boundsCase struct {
	name        string
	lang        int // 2 or 3
	program     string
	acceptPV10  bool
	acceptPV11  bool
	acceptPV12  bool
	explanation string
}

func (c boundsCase) wantAccept(pv uint) bool {
	switch {
	case pv >= 12:
		return c.acceptPV12
	case pv == 11:
		return c.acceptPV11
	default:
		return c.acceptPV10
	}
}

// boundsSquarings wraps body in a chain of lets s1 = s0*s0, s2 = s1*s1, ...
// so that sN = 2^(2^N) is computed once rather than expanded into a tree.
func boundsSquarings(n int, body uplc) uplc {
	for i := n; i >= 1; i-- {
		prev := fmt.Sprintf("s%d", i-1)
		body = uLet(fmt.Sprintf("s%d", i),
			uApp(uBuiltin("multiplyInteger"), uplc(prev), uplc(prev)), body)
	}
	return uLet("s0", uInt(2), body)
}

// boundsIntegerProgram binds the interesting integers around the use of one
// of them. x = 2^131072 and a = 2^131071, so the integers are built only
// from arguments inside the PV11 bound:
//
//	over  = 2^262143      (first out-of-range positive value)
//	max   = 2^262143 - 1  (last in-range positive value)
//	min   = -(2^262143)   (last in-range negative value)
//	under = -(2^262143)-1 (first out-of-range negative value)
func boundsIntegerProgram(use string) string {
	body := uLet("r", uApp(uBuiltin("addInteger"), uplc(use), uInt(0)), uUnit)
	chain := uLet("x", "s17",
		uLet("a", uApp(uBuiltin("divideInteger"), "x", uInt(2)),
			uLet("big", uApp(uBuiltin("multiplyInteger"), "x",
				uApp(uBuiltin("subtractInteger"), "a", uInt(1))),
				uLet("over", uApp(uBuiltin("addInteger"), "big", "x"),
					uLet("max", uApp(uBuiltin("addInteger"), "big",
						uApp(uBuiltin("subtractInteger"), "x", uInt(1))),
						uLet("min", uApp(uBuiltin("multiplyInteger"), "x",
							uApp(uBuiltin("subtractInteger"), uInt(0), "a")),
							uLet("under", uApp(uBuiltin("subtractInteger"), "min", uInt(1)),
								body)))))))
	return uProgram(uLam("ctx", boundsSquarings(17, chain)))
}

// boundsBytesProgram builds 65536 bytes by doubling one byte 16 times and
// applies f to that string or to the string plus one byte.
func boundsBytesProgram(f string, plusOne bool) string {
	var s uplc = "d16"
	if plusOne {
		s = uApp(uBuiltin("appendByteString"), "d16", "d0")
	}
	var body uplc = uLet("r", uApp(uBuiltin(f), s), uUnit)
	for i := 16; i >= 1; i-- {
		prev := fmt.Sprintf("d%d", i-1)
		body = uLet(fmt.Sprintf("d%d", i),
			uApp(uBuiltin("appendByteString"), uplc(prev), uplc(prev)), body)
	}
	return uProgram(uLam("ctx", uLet("d0", uBS([]byte{1}), body)))
}

// boundsIntegerBuiltinProgram is boundsIntegerProgram applying f to the
// chosen integer (and 1 or 0) instead of addInteger.
func boundsIntegerBuiltinProgram(f, use string, rest ...uplc) string {
	prog := boundsIntegerProgram(use)
	args := append([]uplc{uplc(use)}, rest...)
	old := string(uLet("r", uApp(uBuiltin("addInteger"), uplc(use), uInt(0)), uUnit))
	repl := string(uLet("r", uApp(uBuiltin(f), args...), uUnit))
	if !strings.Contains(prog, old) {
		panic("boundsIntegerProgram body changed")
	}
	return strings.Replace(prog, old, repl, 1)
}

// boundsBytes2Program applies a two-argument byte string builtin to the
// 65537-byte string and an empty string.
func boundsBytes2Program(f string) string {
	prog := boundsBytesProgram("sha2_256", true)
	old := string(uLet("r", uApp(uBuiltin("sha2_256"),
		uApp(uBuiltin("appendByteString"), "d16", "d0")), uUnit))
	repl := string(uLet("r", uApp(uBuiltin(f),
		uApp(uBuiltin("appendByteString"), "d16", "d0"), uBS(nil)), uUnit))
	if !strings.Contains(prog, old) {
		panic("boundsBytesProgram body changed")
	}
	return strings.Replace(prog, old, repl, 1)
}

// boundsCases lists the scripts. The accept columns are what plutigo
// assumes for protocol versions 10 and 11 (bounded_args.go).
//
// The generated devnet genesis can only carry the cost model parameters that
// existed before protocol version 11 (Alonzo and Conway genesis files take
// exactly 166/175/251 parameters for PlutusV1/V2/V3), and a hard-fork
// parameter update would be needed to add the rest. A builtin or CEK
// feature whose cost parameters were added in PV11 (dropList, Case/Constr
// and the newly available builtins in V1/V2) is therefore costed at
// "unavailable" on both implementations whatever its availability, so those
// cases only compare how the two implementations treat missing parameters.
// PlutusV2 cannot be exercised at all: on a genesis-bootstrapped devnet
// cardano-node carries no PlutusV2 cost model (it ignores the PlutusV2 entry
// of the Alonzo genesis), so a V2 script cannot be costed there.
func boundsCases() []boundsCase {
	return []boundsCase{
		{"add max in-range integer 2^262143-1", 3, boundsIntegerProgram("max"),
			true, true, true, "last in-range integer"},
		{"add min in-range integer -(2^262143)", 3, boundsIntegerProgram("min"),
			true, true, true, "last in-range negative integer"},
		{"add integer 2^262143", 3, boundsIntegerProgram("over"),
			true, false, true, "PV11 bound fails"},
		{"add integer -(2^262143)-1", 3, boundsIntegerProgram("under"),
			true, false, true, "PV11 bound fails"},
		{"multiply integer 2^262143 by 1", 3,
			boundsIntegerBuiltinProgram("multiplyInteger", "over", uInt(1)),
			true, false, true, "PV11 bound fails"},
		{"equalsInteger of 2^262143 and 0 (unbounded builtin)", 3,
			boundsIntegerBuiltinProgram("equalsInteger", "over", uInt(0)),
			true, true, true, "not in plutigo's bounded list"},
		{"sha2_256 of 65536 bytes", 3, boundsBytesProgram("sha2_256", false),
			true, true, true, "largest allowed byte string"},
		{"sha2_256 of 65537 bytes", 3, boundsBytesProgram("sha2_256", true),
			true, false, false, "PV11+ byte string bound"},
		{"appendByteString of 65537 bytes and empty", 3, boundsBytes2Program("appendByteString"),
			true, false, false, "PV11+ byte string bound"},
		{"equalsByteString of 65537 bytes and empty", 3, boundsBytes2Program("equalsByteString"),
			true, false, false, "PV11+ byte string bound"},
		{"lengthOfByteString of 65537 bytes (unbounded builtin)", 3, boundsBytesProgram("lengthOfByteString", true),
			true, true, true, "not in plutigo's bounded list"},
		{"V3 dropList (PV11 builtin, no cost params)", 3, uProgram(uLam("ctx",
			uLet("r", uApp(uplc("(force (builtin dropList))"), uInt(1),
				uplc("(con (list integer) [1,2,3])")), uUnit))),
			false, false, false, "PV11 cost params absent from genesis"},
	}
}

func boundsScript(t *testing.T, program string) []byte {
	t.Helper()
	p, err := syn.Parse(program)
	require.NoError(t, err, "parse UPLC")
	db, err := syn.NameToDeBruijn(p)
	require.NoError(t, err)
	flat, err := syn.Encode(db)
	require.NoError(t, err)
	wrapper, err := cbor.Encode(flat)
	require.NoError(t, err)
	return wrapper
}

// boundsScriptHash returns the hash of a script of the given language.
func boundsScriptHash(lang int, wrapper []byte) [28]byte {
	// The script hash prefix is the language number: V1=1, V2=2, V3=3.
	return [28]byte(common.Blake2b224Hash(append([]byte{byte(lang)}, wrapper...)).Bytes())
}

// boundsParams reads the protocol parameters and the era of a node.
func boundsParams(
	addr string, magic uint32,
) (*conway.ConwayProtocolParameters, int, error) {
	type res struct {
		params *conway.ConwayProtocolParameters
		era    int
	}
	r, err := plutusQuery(addr, magic, func(c *olsq.Client) (res, error) {
		era, err := c.GetCurrentEra()
		if err != nil {
			return res{}, err
		}
		pp, err := c.GetCurrentProtocolParams()
		if err != nil {
			return res{}, err
		}
		switch p := pp.(type) {
		case *conway.ConwayProtocolParameters:
			return res{p, era}, nil
		case *gledger.DijkstraProtocolParameters:
			return res{&p.ConwayProtocolParameters, era}, nil
		}
		return res{}, fmt.Errorf("unexpected protocol parameter type %T", pp)
	})
	return r.params, r.era, err
}

func boundsSubmit(addr string, magic uint32, era int, tx []byte) error {
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return fmt.Errorf("ouroboros.New: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	if err := conn.DialTimeout("tcp", addr, 10*time.Second); err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	protocol := conn.LocalTxSubmission()
	if protocol == nil || protocol.Client == nil {
		return fmt.Errorf("LocalTxSubmission unavailable at %s", addr)
	}
	return protocol.Client.SubmitTx(uint16(era), tx) //nolint:gosec
}

type boundsSpend struct {
	scriptIn   plutusRef
	lang       int
	script     []byte
	redeemer   []byte
	mem, steps uint64
	collateral plutusRef
}

// buildBoundsSpend builds a spend of an inline-datum script output with the
// script of the given language, solving the fee for the supplied ExUnits.
func (e *plutusEnv) buildBoundsSpend(s boundsSpend) []byte {
	e.t.Helper()
	redeemers := cMap([2][]byte{
		cArr(cUint(0), cUint(0)),
		cArr(s.redeemer, cArr(cUint(s.mem), cUint(s.steps))),
	})
	model := e.params.CostModels[uint(s.lang-1)]
	require.NotEmpty(e.t, model, "no cost model for PlutusV%d", s.lang)
	items := make([][]byte, 0, len(model))
	for _, v := range model {
		items = append(items, cInt(v))
	}
	views := cMap([2][]byte{cUint(uint64(s.lang - 1)), cArr(items...)})
	dataHash := common.Blake2b256Hash(cCat(redeemers, views))
	// Witness set keys: PlutusV2 scripts are 6, PlutusV3 scripts are 7.
	scriptKey := uint64(7)
	if s.lang == 2 {
		scriptKey = 6
	}
	_, tx := e.solveFee(s.mem, s.steps, func(fee uint64) []byte {
		total := (fee*uint64(e.params.CollateralPercentage) + 99) / 100 //nolint:gosec
		body := cMap(
			[2][]byte{cUint(0), cTagSet(cInput(s.scriptIn))},
			[2][]byte{cUint(1), cArr(plutusOutput(e.key.address, plutusScriptOutput-fee, nil))},
			[2][]byte{cUint(2), cUint(fee)},
			[2][]byte{cUint(11), cBytes(dataHash.Bytes())},
			[2][]byte{cUint(13), cTagSet(cInput(s.collateral))},
			[2][]byte{cUint(16), plutusOutput(e.key.address, plutusCollateralOut-total, nil)},
			[2][]byte{cUint(17), cUint(total)},
		)
		witness := cMap(
			[2][]byte{cUint(0), cArr(cArr(cBytes(e.key.vkey), cBytes(e.sign(body))))},
			[2][]byte{cUint(5), redeemers},
			[2][]byte{cUint(scriptKey), cArr(cBytes(s.script))},
		)
		return cArr(body, witness, []byte{0xf5}, []byte{0xf6})
	})
	return tx
}

func TestPlutusBoundedArgsConformance(t *testing.T) {
	keysDir := os.Getenv("DEVNET_GENESIS_KEYS_DIR")
	if keysDir == "" {
		t.Skip("requires genesis keys; run internal/test/devnet/run-tests.sh --conformance")
	}
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err)
	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(t, endpoints, devnet.WithNetworkMagic(cfg.NetworkMagic))
	h.WaitForAllNodesReady(60 * time.Second)

	control, err := devnet.NewNodeControl(t.Logf)
	require.NoError(t, err)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	require.NoError(t, control.Stop(stopCtx, "txpump"))
	stopCancel()
	t.Cleanup(func() {
		startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer startCancel()
		if err := control.Start(startCtx, "txpump"); err != nil {
			t.Errorf("restart txpump: %v", err)
		}
	})
	t.Cleanup(func() {
		logCtx, logCancel := context.WithTimeout(context.Background(), time.Minute)
		defer logCancel()
		logs, err := control.Logs(logCtx, "dingo-producer", 0)
		if err != nil {
			t.Logf("dingo-producer logs unavailable: %v", err)
			return
		}
		for line := range strings.SplitSeq(logs, "\n") {
			if strings.Contains(line, "failed to add transaction to mempool") {
				if i := strings.Index(line, "msg="); i >= 0 {
					line = line[i:]
				}
				if len(line) > 800 {
					line = line[:800]
				}
				t.Logf("dingo mempool rejection: %s", line)
			}
		}
	})

	env := &plutusEnv{
		t:          t,
		magic:      cfg.NetworkMagic,
		dingoNtc:   devnet.DingoProducerNtcAddr(),
		cardanoNtc: devnet.CardanoProducerNtcAddr(),
	}

	var eraTag int
	submit := func(tx []byte) plutusVerdict {
		return plutusVerdict{
			cardanoErr: boundsSubmit(env.cardanoNtc, env.magic, eraTag, tx),
			dingoErr:   boundsSubmit(env.dingoNtc, env.magic, eraTag, tx),
		}
	}

	// The test only starts once both nodes are in the wanted era: Conway
	// (6) unless the spec starts in Dijkstra at epoch 0; a later scheduled
	// fork keeps the Conway expectation.
	wantEra := 6
	if spec, specErr := devnet.LoadDevNetConfig(); specErr == nil &&
		spec.DijkstraHardForkAtEpoch != nil &&
		*spec.DijkstraHardForkAtEpoch == 0 {
		wantEra = 7
	}
	var cardanoParams, dingoParams *conway.ConwayProtocolParameters
	var cardanoEra, dingoEra int
	deadline := time.Now().Add(6 * time.Minute)
	for {
		cardanoParams, cardanoEra, err = boundsParams(env.cardanoNtc, env.magic)
		if err == nil {
			dingoParams, dingoEra, err = boundsParams(env.dingoNtc, env.magic)
		}
		if err == nil && cardanoEra >= wantEra && dingoEra >= wantEra {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("era %d not reached: cardano-node era=%d dingo era=%d err=%v",
				wantEra, cardanoEra, dingoEra, err)
		}
		time.Sleep(3 * time.Second)
	}
	env.params = cardanoParams
	pv := cardanoParams.ProtocolMajorVersion()
	t.Logf("PROTOCOL cardano-node: era=%d pv=%d.%d | dingo: era=%d pv=%d.%d",
		cardanoEra, pv, cardanoParams.ProtocolVersion.Minor,
		dingoEra, dingoParams.ProtocolMajorVersion(), dingoParams.ProtocolVersion.Minor)
	for lang := 1; lang <= 3; lang++ {
		t.Logf("cost model PlutusV%d: cardano-node %d params, dingo %d params, equal=%v",
			lang, len(cardanoParams.CostModels[uint(lang-1)]),
			len(dingoParams.CostModels[uint(lang-1)]),
			fmt.Sprint(cardanoParams.CostModels[uint(lang-1)]) ==
				fmt.Sprint(dingoParams.CostModels[uint(lang-1)]))
	}
	t.Logf("maxTxSize=%d maxTxExUnits mem=%d steps=%d", cardanoParams.MaxTxSize,
		cardanoParams.MaxTxExUnits.Memory, cardanoParams.MaxTxExUnits.Steps)
	require.Equal(t, cardanoParams.ProtocolMajorVersion(), dingoParams.ProtocolMajorVersion(),
		"protocol version differs between dingo and cardano-node")
	eraTag = cardanoEra

	// txpump is stopped, but a transaction it submitted earlier can still be
	// pending and holding a genesis UTxO this test would otherwise pick.
	env.waitMempoolsEmpty()

	keys := loadPlutusGenesisKeys(t, keysDir)
	require.NotEmpty(t, keys)
	addrs := make([]common.Address, 0, len(keys))
	keyByAddr := map[string]*plutusKey{}
	for _, k := range keys {
		a, err := common.NewAddressFromBytes(k.address)
		require.NoError(t, err)
		addrs = append(addrs, a)
		keyByAddr[string(k.address)] = k
	}
	dUtxos, err := plutusUTxOsByAddress(env.dingoNtc, env.magic, addrs)
	require.NoError(t, err)
	cUtxos, err := plutusUTxOsByAddress(env.cardanoNtc, env.magic, addrs)
	require.NoError(t, err)
	type fundCandidate struct {
		in    plutusRef
		value uint64
		rank  int
		key   *plutusKey
	}
	var candidates []fundCandidate
	for id, out := range dUtxos {
		if _, ok := cUtxos[id]; !ok {
			continue
		}
		addrBytes := mustAddrBytes(t, out)
		key := keyByAddr[string(addrBytes)]
		if key == nil {
			continue
		}
		rank := 0
		if addrBytes[0]>>4 != 0x6 {
			rank = 1
		}
		candidates = append(candidates, fundCandidate{
			in:    plutusRef{hash: id.Hash, idx: uint32(id.Idx)}, //nolint:gosec
			value: out.OutputAmount.Amount, rank: rank, key: key,
		})
	}
	require.NotEmpty(t, candidates, "no genesis UTxO is spendable with a known key")
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].rank != candidates[j].rank {
			return candidates[i].rank > candidates[j].rank
		}
		return candidates[i].value > candidates[j].value
	})

	cases := boundsCases()
	scripts := make([][]byte, len(cases))
	for i, c := range cases {
		scripts[i] = boundsScript(t, c.program)
		t.Logf("script %-45s PlutusV%d hash=%x size=%d", c.name, c.lang,
			boundsScriptHash(c.lang, scripts[i]), len(scripts[i]))
	}
	// A genesis UTxO can already be spent by a transaction still pending in
	// one node's mempool (txpump's last submission), which that node then
	// rejects as consumed. Submit to dingo first and, if it refuses an
	// input, move on to the next candidate without involving cardano-node.
	var fundBase plutusRef
	funded := false
	for _, cand := range candidates {
		env.key = cand.key
		var fundOutputs [][]byte
		for i, c := range cases {
			hash := boundsScriptHash(c.lang, scripts[i])
			addr := append([]byte{0x70}, hash[:]...)
			fundOutputs = append(fundOutputs, plutusOutput(addr, plutusScriptOutput, mustHex("d87980")))
		}
		for range cases {
			fundOutputs = append(fundOutputs, plutusOutput(env.key.address, plutusCollateralOut, nil))
		}
		totalOut := uint64(len(cases)) * (plutusScriptOutput + plutusCollateralOut)
		if cand.value < totalOut+2_000_000 {
			continue
		}
		var fundTx []byte
		fundTx, fundBase = env.fundingTx(cand.in, cand.value, fundOutputs, totalOut)
		// The funding transaction carries no script, so its shape is the
		// same in every era.
		dErr := boundsSubmit(env.dingoNtc, env.magic, eraTag, fundTx)
		if dErr != nil {
			t.Logf("FUNDING tx %x from %x#%d: dingo %s; trying the next UTxO",
				fundBase.hash, cand.in.hash, cand.in.idx, shortErr(dErr))
			continue
		}
		cErr := boundsSubmit(env.cardanoNtc, env.magic, eraTag, fundTx)
		t.Logf("FUNDING tx %x from %x#%d: dingo accepted | cardano-node %s",
			fundBase.hash, cand.in.hash, cand.in.idx, shortErr(cErr))
		require.NoError(t, cErr)
		funded = true
		break
	}
	require.True(t, funded, "no genesis UTxO could fund the scripts on both nodes")
	var allRefs []plutusRef
	for i := range 2 * len(cases) {
		allRefs = append(allRefs, plutusRef{hash: fundBase.hash, idx: uint32(i)}) //nolint:gosec
	}
	env.waitBoth(allRefs, true)
	// The spends must be judged against a ledger that has applied the
	// funding block on both nodes, with the funding transaction no longer
	// pending in either mempool.
	env.waitMempoolsEmpty()

	type outcome struct {
		c                    boundsCase
		dingoErr, cardanoErr error
		tx                   []byte
	}
	var results []outcome
	for i, c := range cases {
		tx := env.buildBoundsSpend(boundsSpend{
			scriptIn:   plutusRef{hash: fundBase.hash, idx: uint32(i)}, //nolint:gosec
			lang:       c.lang,
			script:     scripts[i],
			redeemer:   mustHex("d87980"),
			mem:        5_000_000,
			steps:      8_000_000_000,
			collateral: plutusRef{hash: fundBase.hash, idx: uint32(len(cases) + i)}, //nolint:gosec
		})
		o := outcome{c: c, tx: tx}
		v := submit(tx)
		o.dingoErr, o.cardanoErr = v.dingoErr, v.cardanoErr
		results = append(results, o)
		t.Logf("CASE PV%d %q tx %x (%d bytes): dingo %s | cardano-node %s",
			pv, c.name, txIDFromTx(t, tx), len(tx),
			fullErr(o.dingoErr), fullErr(o.cardanoErr))
	}

	t.Logf("SUMMARY at protocol version %d (era %d)", pv, cardanoEra)
	for _, o := range results {
		d, n := o.dingoErr == nil, o.cardanoErr == nil
		want := o.c.wantAccept(pv)
		t.Logf("SUMMARY PV%d | %-46s | dingo=%-8s node=%-8s | plutigo-assumes=%-8s | %s",
			pv, o.c.name, verdictWord(d), verdictWord(n), verdictWord(want), o.c.explanation)
		if d != n {
			t.Errorf("DISAGREEMENT on %q: dingo accepted=%v cardano-node accepted=%v",
				o.c.name, d, n)
		}
		if n != want {
			t.Errorf("ASSUMPTION MISMATCH on %q at PV%d: plutigo assumes accepted=%v, cardano-node accepted=%v",
				o.c.name, pv, want, n)
		}
	}

	// Accepted spends must also be forged identically: wait for their change
	// output on both producers.
	for _, o := range results {
		if o.dingoErr == nil && o.cardanoErr == nil {
			env.waitBoth([]plutusRef{{hash: txIDFromTx(t, o.tx), idx: 0}}, true)
		}
	}
}

// waitMempoolsEmpty waits until neither producer's mempool holds a
// transaction, read through LocalTxMonitor.
func (e *plutusEnv) waitMempoolsEmpty() {
	e.t.Helper()
	deadline := time.Now().Add(plutusConfirmTimeout)
	var last string
	for time.Now().Before(deadline) {
		empty := true
		for _, addr := range []string{e.dingoNtc, e.cardanoNtc} {
			n, err := boundsMempoolTxCount(addr, e.magic)
			if err != nil {
				last = err.Error()
				empty = false
				break
			}
			if n != 0 {
				last = fmt.Sprintf("%d transactions pending at %s", n, addr)
				empty = false
				break
			}
		}
		if empty {
			return
		}
		time.Sleep(2 * time.Second)
	}
	require.FailNowf(e.t, "mempool timeout", "mempools not empty: %s", last)
}

// boundsMempoolTxCount returns the number of transactions in the mempool of
// the node at addr.
func boundsMempoolTxCount(addr string, magic uint32) (uint32, error) {
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return 0, fmt.Errorf("ouroboros.New: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	if err := conn.DialTimeout("tcp", addr, 10*time.Second); err != nil {
		return 0, fmt.Errorf("dial %s: %w", addr, err)
	}
	protocol := conn.LocalTxMonitor()
	if protocol == nil || protocol.Client == nil {
		return 0, fmt.Errorf("LocalTxMonitor unavailable at %s", addr)
	}
	defer protocol.Client.Release() //nolint:errcheck
	_, _, count, err := protocol.Client.GetSizes()
	if err != nil {
		return 0, fmt.Errorf("mempool sizes at %s: %w", addr, err)
	}
	return count, nil
}

func verdictWord(ok bool) string {
	if ok {
		return "accepted"
	}
	return "rejected"
}

func fullErr(err error) string {
	if err == nil {
		return "accepted"
	}
	msg := strings.ReplaceAll(err.Error(), "\n", " | ")
	if len(msg) > 1500 {
		msg = msg[:1500] + "..."
	}
	return "REJECTED: " + msg
}
