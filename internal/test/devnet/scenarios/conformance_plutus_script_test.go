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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/blinklabs-io/dingo/internal/test/devnet"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// This scenario drives Plutus V3 script transactions through a live devnet
// that runs one Dingo producer and a cardano-node producer and relay. Every
// transaction is submitted to the mempool of both producers, so the test
// asserts that Dingo and cardano-node accept and reject exactly the same
// transactions, and then compares the whole UTxO set of both nodes.
//
// Transactions are assembled as raw CBOR so that every byte the ledgers see
// (datum and redeemer encodings, script data hash, ExUnits) is controlled by
// the test. Required ExUnits are measured with plutigo.

const (
	plutusConfirmTimeout = 3 * time.Minute
	plutusScriptOutput   = 10_000_000
	plutusCollateralOut  = 6_000_000
)

type plutusRef struct {
	hash [32]byte
	idx  uint32
}

func (r plutusRef) String() string {
	return fmt.Sprintf("%x#%d", r.hash, r.idx)
}

type plutusKey struct {
	vkey    []byte
	skey    []byte
	address []byte
}

// ---------------------------------------------------------------------------
// Minimal CBOR writers. The test controls every byte of the transaction.
// ---------------------------------------------------------------------------

func cHead(major byte, n uint64) []byte {
	switch {
	case n < 24:
		return []byte{major<<5 | byte(n)}
	case n <= 0xff:
		return []byte{major<<5 | 24, byte(n)}
	case n <= 0xffff:
		return []byte{major<<5 | 25, byte(n >> 8), byte(n)}
	case n <= 0xffffffff:
		return []byte{
			major<<5 | 26,
			byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
		}
	default:
		out := []byte{major<<5 | 27}
		for shift := 56; shift >= 0; shift -= 8 {
			out = append(out, byte(n>>uint(shift)))
		}
		return out
	}
}

func cUint(n uint64) []byte { return cHead(0, n) }

func cInt(n int64) []byte {
	if n >= 0 {
		return cHead(0, uint64(n))
	}
	return cHead(1, uint64(-1-n))
}

func cBytes(b []byte) []byte { return append(cHead(2, uint64(len(b))), b...) }

func cCat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func cArr(items ...[]byte) []byte {
	return cCat(cHead(4, uint64(len(items))), cCat(items...))
}

// cMap writes a definite map whose pairs the caller has already put in
// canonical key order.
func cMap(pairs ...[2][]byte) []byte {
	out := cHead(5, uint64(len(pairs)))
	for _, p := range pairs {
		out = append(out, p[0]...)
		out = append(out, p[1]...)
	}
	return out
}

func cTagSet(items ...[]byte) []byte {
	return cCat([]byte{0xd9, 0x01, 0x02}, cArr(items...))
}

func cInput(r plutusRef) []byte {
	return cArr(cBytes(r.hash[:]), cUint(uint64(r.idx)))
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		panic(err)
	}
	return b
}

// plutusOutput encodes a post-Alonzo output with an optional inline datum.
func plutusOutput(addr []byte, coin uint64, inlineDatum []byte) []byte {
	pairs := [][2][]byte{
		{cUint(0), cBytes(addr)},
		{cUint(1), cUint(coin)},
	}
	if inlineDatum != nil {
		pairs = append(pairs, [2][]byte{
			cUint(2),
			cArr(cUint(1), cCat([]byte{0xd8, 0x18}, cBytes(inlineDatum))),
		})
	}
	return cMap(pairs...)
}

// ---------------------------------------------------------------------------
// Scripts
// ---------------------------------------------------------------------------

type uplc string

func uApp(f uplc, args ...uplc) uplc {
	parts := []string{string(f)}
	for _, a := range args {
		parts = append(parts, string(a))
	}
	return uplc("[ " + strings.Join(parts, " ") + " ]")
}

func uLam(name string, body uplc) uplc {
	return uplc("(lam " + name + " " + string(body) + ")")
}

func uLet(name string, val, body uplc) uplc {
	return uApp(uLam(name, body), val)
}

func uBuiltin(name string) uplc { return uplc("(builtin " + name + ")") }

func uIf(cond, then, otherwise uplc) uplc {
	return uplc("(force [ (force (builtin ifThenElse)) " + string(cond) +
		" (delay " + string(then) + ") (delay " + string(otherwise) + ") ])")
}

const (
	uUnit  uplc = "(con unit ())"
	uError uplc = "(error)"
)

func uInt(n int64) uplc { return uplc(fmt.Sprintf("(con integer %d)", n)) }

func uBS(b []byte) uplc {
	return uplc("(con bytestring #" + hex.EncodeToString(b) + ")")
}

func uFst(x uplc) uplc {
	return uApp(uplc("(force (force (builtin fstPair)))"), x)
}

func uSnd(x uplc) uplc {
	return uApp(uplc("(force (force (builtin sndPair)))"), x)
}

func uHead(x uplc) uplc { return uApp(uplc("(force (builtin headList))"), x) }
func uTail(x uplc) uplc { return uApp(uplc("(force (builtin tailList))"), x) }

func uUnConstrFields(x uplc) uplc {
	return uSnd(uApp(uBuiltin("unConstrData"), x))
}

// uSpendingBindings binds ctx fields: txInfoFields, red, info, infoFields
// and dat (the inline datum of the spent output) around body.
func uSpendingBindings(body uplc) uplc {
	return uLam("ctx", uLet("fields", uUnConstrFields("ctx"),
		uLet("txInfoFields", uUnConstrFields(uHead("fields")),
			uLet("red", uHead(uTail("fields")),
				uLet("info", uHead(uTail(uTail("fields"))),
					uLet("infoFields", uUnConstrFields("info"),
						uLet("dat", uHead(uUnConstrFields(
							uHead(uTail("infoFields")))),
							body)))))))
}

func uProgram(term uplc) string {
	return "(program 1.1.0 " + string(term) + ")"
}

// plutusScriptFromText assembles UPLC text into the script bytes a V3
// witness carries and returns the script and its hash.
func plutusScriptFromText(
	t *testing.T, src string,
) (common.PlutusV3Script, common.ScriptHash) {
	t.Helper()
	program, err := syn.Parse(src)
	require.NoError(t, err, "parse UPLC")
	dbProgram, err := syn.NameToDeBruijn(program)
	require.NoError(t, err)
	flat, err := syn.Encode(dbProgram)
	require.NoError(t, err)
	wrapper, err := cbor.Encode(flat)
	require.NoError(t, err)
	s := common.PlutusV3Script(wrapper)
	return s, s.Hash()
}

// plutusScripts returns the UPLC source of each test script.
func plutusScripts() map[string]string {
	// Always succeeds.
	always := uProgram(uLam("ctx", uUnit))
	// Never succeeds.
	fail := uProgram(uLam("ctx", uError))

	// Inspects the ScriptContext: the purpose is Spending (constructor 1),
	// the tx fee is positive, and the redeemer equals the inline datum.
	check := uProgram(uSpendingBindings(
		uIf(
			uApp(uBuiltin("equalsInteger"), uFst(uApp(uBuiltin("unConstrData"), "info")), uInt(1)),
			uIf(
				uApp(uBuiltin("lessThanInteger"), uInt(0),
					uApp(uBuiltin("unIData"), uHead(uTail(uTail(uTail("txInfoFields")))))),
				uIf(uApp(uBuiltin("equalsData"), "red", "dat"), uUnit, uError),
				uError,
			),
			uError,
		),
	))

	// Indefinite-length and chunked encodings must all normalise to one
	// serialiseData form, for both the inline datum and the redeemer.
	ser := normalisedSerialisedData()
	serialise := uProgram(uSpendingBindings(
		uIf(
			uApp(uBuiltin("equalsByteString"),
				uApp(uBuiltin("serialiseData"), "dat"), uBS(ser)),
			uIf(
				uApp(uBuiltin("equalsByteString"),
					uApp(uBuiltin("serialiseData"), "red"), uBS(ser)),
				uUnit, uError),
			uError,
		),
	))

	// Costly builtins: Ed25519, SHA2, Blake2b, Keccak and BLS12-381.
	seed := sha256.Sum256([]byte("dingo-devnet-plutus-conformance"))
	priv := ed25519.NewKeyFromSeed(seed[:])
	pub := priv.Public().(ed25519.PublicKey)
	msg := []byte("dingo and cardano-node must agree on every ExUnit")
	sig := ed25519.Sign(priv, msg)
	sha := sha256.Sum256(msg)
	g1 := mustHex("97f1d3a73197d7942695638c4fa9ac0fc3688c4f9774b905a14e3a3f171bac586c55e83ff97a1aeffb3af00adb22c6bb")
	g2 := mustHex("93e02b6052719f607dacd3a088274f65596bd0d09920b61ab5da61bbdc7f5049334cf11213945d57e5ac7d055d042b7e024aa2b2f08f0a91260805272dc51051c6e47ad4fa403b02b4510b647ae3d1770bac0326a805bbefd48056c8c121bdb8")
	g1Elem := func() uplc { return uApp(uBuiltin("bls12_381_G1_uncompress"), uBS(g1)) }
	g2Elem := func() uplc { return uApp(uBuiltin("bls12_381_G2_uncompress"), uBS(g2)) }
	costly := uProgram(uLam("ctx",
		uIf(
			uApp(uBuiltin("verifyEd25519Signature"), uBS(pub), uBS(msg), uBS(sig)),
			uIf(
				uApp(uBuiltin("equalsByteString"),
					uApp(uBuiltin("sha2_256"), uBS(msg)), uBS(sha[:])),
				uIf(
					uApp(uBuiltin("equalsInteger"),
						uApp(uBuiltin("lengthOfByteString"),
							uApp(uBuiltin("keccak_256"),
								uApp(uBuiltin("blake2b_256"), uBS(msg)))),
						uInt(32)),
					uIf(
						// 3*G == G+G+G
						uApp(uBuiltin("bls12_381_G1_equal"),
							uApp(uBuiltin("bls12_381_G1_scalarMul"), uInt(3), g1Elem()),
							uApp(uBuiltin("bls12_381_G1_add"), g1Elem(),
								uApp(uBuiltin("bls12_381_G1_add"), g1Elem(), g1Elem()))),
						uIf(
							uApp(uBuiltin("bls12_381_finalVerify"),
								uApp(uBuiltin("bls12_381_millerLoop"), g1Elem(), g2Elem()),
								uApp(uBuiltin("bls12_381_millerLoop"), g1Elem(), g2Elem())),
							uUnit, uError),
						uError),
					uError),
				uError),
			uError,
		)))

	return map[string]string{
		"always":    always,
		"fail":      fail,
		"check":     check,
		"serialise": serialise,
		"costly":    costly,
	}
}

// ---------------------------------------------------------------------------
// Datum and redeemer encodings
// ---------------------------------------------------------------------------

func bytes65() []byte {
	b := make([]byte, 65)
	for i := range b {
		b[i] = byte(i + 1)
	}
	return b
}

// normalisedSerialisedData is the serialiseData result for the weirdly
// encoded datum and redeemer below, written out by hand from the Plutus Data
// CBOR rules: non-empty lists and constructor fields are indefinite, maps
// are definite, integers beyond 64 bits are tagged bignums and byte strings
// longer than 64 bytes are chunked into 64-byte pieces.
func normalisedSerialisedData() []byte {
	b := bytes65()
	return cCat(
		mustHex("d8799f"),
		mustHex("01"),
		mustHex("9f0203ff"),
		mustHex("426162"),
		mustHex("a10102"),
		mustHex("c249010000000000000000"),
		mustHex("5f5840"), b[:64], mustHex("41"), b[64:], mustHex("ff"),
		mustHex("ff"),
	)
}

// weirdDatum encodes the datum with indefinite-length containers and a
// chunked byte string.
func weirdDatum() []byte {
	b := bytes65()
	return cCat(
		mustHex("d8799f"), // constr 0, indefinite fields
		mustHex("01"),
		mustHex("9f0203ff"),               // indefinite list [2,3]
		mustHex("5f4161 4162 ff"),         // indefinite "ab"
		mustHex("a10102"),                 // map {1:2}
		mustHex("c249010000000000000000"), // 2^64
		mustHex("5f5840"), b[:64], mustHex("41"), b[64:], mustHex("ff"),
		mustHex("ff"),
	)
}

// weirdRedeemer is the same value as weirdDatum with a definite constructor
// and list, and differently chunked bytes.
func weirdRedeemer() []byte {
	b := bytes65()
	return cCat(
		mustHex("d87986"), // constr 0, definite fields
		mustHex("01"),
		mustHex("820203"),
		mustHex("426162"),
		mustHex("a10102"),
		mustHex("c249010000000000000000"),
		mustHex("5f5820"), b[:32], mustHex("5821"), b[32:], mustHex("ff"),
	)
}

// ---------------------------------------------------------------------------
// Keys and LocalStateQuery access
// ---------------------------------------------------------------------------

func loadPlutusGenesisKeys(t *testing.T, dir string) []*plutusKey {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "genesis.*.skey"))
	require.NoError(t, err)
	sort.Strings(paths)
	var keys []*plutusKey
	for _, path := range paths {
		prefix := strings.TrimSuffix(path, ".skey")
		skey, err := readPlutusKeyFile(path)
		require.NoError(t, err)
		vkey, err := readPlutusKeyFile(prefix + ".vkey")
		require.NoError(t, err)
		require.Len(t, skey, ed25519.SeedSize)
		derived := ed25519.NewKeyFromSeed(skey).Public().(ed25519.PublicKey)
		require.Equal(t, []byte(derived), vkey, "vkey mismatch for %s", prefix)
		info, err := os.ReadFile(prefix + ".addr.info") //nolint:gosec // test key dir
		if err != nil {
			continue
		}
		base16 := extractJSONString(string(info), "base16")
		addr, err := hex.DecodeString(base16)
		require.NoError(t, err, "address info %s", prefix)
		keys = append(keys, &plutusKey{vkey: vkey, skey: skey, address: addr})
	}
	return keys
}

func extractJSONString(doc, key string) string {
	marker := `"` + key + `"`
	i := strings.Index(doc, marker)
	if i < 0 {
		return ""
	}
	rest := doc[i+len(marker):]
	start := strings.Index(rest, `"`)
	if start < 0 {
		return ""
	}
	rest = rest[start+1:]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func readPlutusKeyFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path) //nolint:gosec // isolated test key directory
	if err != nil {
		return nil, err
	}
	cborHex := extractJSONString(string(data), "cborHex")
	raw, err := hex.DecodeString(cborHex)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if len(raw) == 34 && raw[0] == 0x58 && raw[1] == 0x20 {
		raw = raw[2:]
	}
	return raw, nil
}

func plutusQuery[T any](
	addr string, magic uint32, fn func(*olsq.Client) (T, error),
) (T, error) {
	var zero T
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return zero, fmt.Errorf("ouroboros.New: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	if err := conn.DialTimeout("tcp", addr, 10*time.Second); err != nil {
		return zero, fmt.Errorf("dial %s: %w", addr, err)
	}
	protocol := conn.LocalStateQuery()
	if protocol == nil || protocol.Client == nil {
		return zero, fmt.Errorf("LocalStateQuery unavailable at %s", addr)
	}
	if err := protocol.Client.AcquireVolatileTip(); err != nil {
		return zero, fmt.Errorf("acquire volatile tip at %s: %w", addr, err)
	}
	defer protocol.Client.Release() //nolint:errcheck
	return fn(protocol.Client)
}

func plutusUTxOsByAddress(
	addr string, magic uint32, addrs []common.Address,
) (map[olsq.UtxoId]babbage.BabbageTransactionOutput, error) {
	res, err := plutusQuery(addr, magic, func(c *olsq.Client) (*olsq.UTxOsResult, error) {
		return c.GetUTxOByAddress(addrs)
	})
	if err != nil {
		return nil, err
	}
	return res.Results, nil
}

func plutusUTxOsByRef(
	addr string, magic uint32, refs []plutusRef,
) (map[plutusRef]babbage.BabbageTransactionOutput, error) {
	ins := make([]common.TransactionInput, 0, len(refs))
	for _, r := range refs {
		ins = append(ins, shelley.ShelleyTransactionInput{
			TxId:        r.hash,
			OutputIndex: r.idx,
		})
	}
	res, err := plutusQuery(addr, magic, func(c *olsq.Client) (*olsq.UTxOByTxInResult, error) {
		return c.GetUTxOByTxIn(ins)
	})
	if err != nil {
		return nil, err
	}
	out := make(map[plutusRef]babbage.BabbageTransactionOutput, len(res.Results))
	for id, o := range res.Results {
		out[plutusRef{hash: id.Hash, idx: uint32(id.Idx)}] = o //nolint:gosec
	}
	return out, nil
}

func plutusParams(
	addr string, magic uint32,
) (*conway.ConwayProtocolParameters, error) {
	params, err := plutusQuery(addr, magic, func(c *olsq.Client) (common.ProtocolParameters, error) {
		return c.GetCurrentProtocolParams()
	})
	if err != nil {
		return nil, err
	}
	cp, ok := params.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil, fmt.Errorf("unexpected protocol parameter type %T", params)
	}
	return cp, nil
}

func plutusSubmit(addr string, magic uint32, tx []byte) error {
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
	return protocol.Client.SubmitTx(uint16(gledger.EraIdConway), tx)
}

// ---------------------------------------------------------------------------
// Transaction assembly
// ---------------------------------------------------------------------------

type plutusEnv struct {
	t          *testing.T
	magic      uint32
	dingoNtc   string
	cardanoNtc string
	params     *conway.ConwayProtocolParameters
	key        *plutusKey
	noted      bool
}

func (e *plutusEnv) sign(body []byte) []byte {
	hash := common.Blake2b256Hash(body)
	return ed25519.Sign(ed25519.NewKeyFromSeed(e.key.skey), hash[:])
}

func (e *plutusEnv) feeFor(size int, mem, steps uint64) uint64 {
	fee := uint64(e.params.MinFeeA)*uint64(size) + uint64(e.params.MinFeeB) //nolint:gosec
	if mem == 0 && steps == 0 {
		return fee
	}
	cost := new(big.Rat).Mul(
		e.params.ExecutionCosts.MemPrice.ToBigRat(),
		new(big.Rat).SetUint64(mem),
	)
	cost.Add(cost, new(big.Rat).Mul(
		e.params.ExecutionCosts.StepPrice.ToBigRat(),
		new(big.Rat).SetUint64(steps),
	))
	return fee + ratCeil(cost)
}

func ratCeil(r *big.Rat) uint64 {
	q, m := new(big.Int).QuoRem(r.Num(), r.Denom(), new(big.Int))
	if m.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Uint64()
}

// solveFee finds the smallest fee that covers the size of the transaction
// assemble builds with that fee.
func (e *plutusEnv) solveFee(
	mem, steps uint64, assemble func(fee uint64) []byte,
) (uint64, []byte) {
	e.t.Helper()
	fee := uint64(500_000)
	for range 12 {
		tx := assemble(fee)
		need := e.feeFor(len(tx), mem, steps)
		if need == fee {
			return fee, tx
		}
		fee = need
	}
	e.t.Fatalf("fee did not converge")
	return 0, nil
}

// fundingTx pays the script outputs and the collateral outputs from one
// genesis UTxO.
func (e *plutusEnv) fundingTx(
	in plutusRef, inValue uint64, outputs [][]byte, outValues uint64,
) ([]byte, plutusRef) {
	e.t.Helper()
	_, tx := e.solveFee(0, 0, func(fee uint64) []byte {
		all := append([][]byte{}, outputs...)
		all = append(all, plutusOutput(e.key.address, inValue-outValues-fee, nil))
		body := cMap(
			[2][]byte{cUint(0), cTagSet(cInput(in))},
			[2][]byte{cUint(1), cArr(all...)},
			[2][]byte{cUint(2), cUint(fee)},
		)
		return e.assembleTx(body, nil, nil, true)
	})
	return tx, plutusRef{hash: txIDFromTx(e.t, tx)}
}

// txIDFromTx hashes the body of a raw transaction. It does not decode the
// transaction, so it also works for transactions a decoder rejects.
func txIDFromTx(t *testing.T, tx []byte) [32]byte {
	t.Helper()
	var parts []cbor.RawMessage
	_, err := cbor.Decode(tx, &parts)
	require.NoError(t, err)
	require.NotEmpty(t, parts)
	return [32]byte(common.Blake2b256Hash(parts[0]).Bytes())
}

// assembleTx signs body and wraps it with the witness set and validity flag.
func (e *plutusEnv) assembleTx(
	body, redeemers []byte, script []byte, valid bool,
) []byte {
	witness := [][2][]byte{{
		cUint(0),
		cArr(cArr(cBytes(e.key.vkey), cBytes(e.sign(body)))),
	}}
	if redeemers != nil {
		witness = append(witness, [2][]byte{cUint(5), redeemers})
	}
	if script != nil {
		witness = append(witness, [2][]byte{cUint(7), cArr(cBytes(script))})
	}
	isValid := []byte{0xf5}
	if !valid {
		isValid = []byte{0xf4}
	}
	return cArr(body, cMap(witness...), isValid, []byte{0xf6})
}

// spendSpec describes one attempt to spend a script output.
type spendSpec struct {
	scriptIn    plutusRef
	scriptValue uint64
	script      common.PlutusV3Script
	redeemer    []byte
	mem, steps  uint64
	collateral  *plutusRef
	collValue   uint64
	invalid     bool // build with isValid=false
	badHash     bool // corrupt the script data hash
	feeDelta    int64
}

func (e *plutusEnv) languageViews() []byte {
	model := e.params.CostModels[2]
	items := make([][]byte, 0, len(model))
	for _, v := range model {
		items = append(items, cInt(v))
	}
	return cMap([2][]byte{cUint(2), cArr(items...)})
}

func (e *plutusEnv) buildSpend(s spendSpec) ([]byte, uint64) {
	e.t.Helper()
	redeemers := cMap([2][]byte{
		cArr(cUint(0), cUint(0)),
		cArr(s.redeemer, cArr(cUint(s.mem), cUint(s.steps))),
	})
	dataHash := common.Blake2b256Hash(cCat(redeemers, e.languageViews()))
	if s.badHash {
		dataHash[0] ^= 0xff
	}
	assemble := func(fee uint64) []byte {
		pairs := [][2][]byte{
			{cUint(0), cTagSet(cInput(s.scriptIn))},
			{cUint(1), cArr(plutusOutput(e.key.address, s.scriptValue-fee, nil))},
			{cUint(2), cUint(fee)},
			{cUint(11), cBytes(dataHash.Bytes())},
		}
		if s.collateral != nil {
			total := (fee*uint64(e.params.CollateralPercentage) + 99) / 100 //nolint:gosec
			pairs = append(pairs,
				[2][]byte{cUint(13), cTagSet(cInput(*s.collateral))},
				[2][]byte{cUint(16), plutusOutput(e.key.address, s.collValue-total, nil)},
				[2][]byte{cUint(17), cUint(total)},
			)
		}
		return e.assembleTx(cMap(pairs...), redeemers, s.script, !s.invalid)
	}
	fee, tx := e.solveFee(s.mem, s.steps, assemble)
	if s.feeDelta != 0 {
		fee = uint64(int64(fee) + s.feeDelta) //nolint:gosec
		tx = assemble(fee)
	}
	return tx, fee
}

// evalLedger resolves the spent outputs for plutigo evaluation.
type evalLedger struct {
	common.LedgerState
	utxos map[plutusRef]common.Utxo
}

func (l *evalLedger) UtxoById(in common.TransactionInput) (common.Utxo, error) {
	u, ok := l.utxos[plutusRef{hash: [32]byte(in.Id().Bytes()), idx: in.Index()}]
	if !ok {
		return common.Utxo{}, errors.New("unknown input")
	}
	return u, nil
}

func (l *evalLedger) SlotToTime(uint64) (time.Time, error) { return time.Time{}, nil }
func (l *evalLedger) TimeToSlot(time.Time) (uint64, error) { return 0, nil }

// measure evaluates the transaction's redeemer with plutigo.
func (e *plutusEnv) measure(
	tx []byte, known map[plutusRef]babbage.BabbageTransactionOutput,
) (common.ExUnits, error) {
	parsed, err := conway.NewConwayTransactionFromCbor(tx)
	if err != nil {
		return common.ExUnits{}, err
	}
	ledger := &evalLedger{utxos: map[plutusRef]common.Utxo{}}
	for ref, out := range known {
		out := out
		ledger.utxos[ref] = common.Utxo{
			Id: shelley.ShelleyTransactionInput{
				TxId:        ref.hash,
				OutputIndex: ref.idx,
			},
			Output: &out,
		}
	}
	budget := e.params.MaxTxExUnits
	used, err := script.EvaluatePlutusScripts(
		parsed, ledger, e.params.ProtocolMajorVersion(),
		e.params.CostModels, 3, "conway", &budget,
	)
	if err != nil {
		return common.ExUnits{}, err
	}
	for _, units := range used {
		return units, nil
	}
	return common.ExUnits{}, errors.New("no redeemer evaluated")
}

// exactSpend measures the script with plutigo and rebuilds the transaction
// with exactly the measured ExUnits.
func (e *plutusEnv) exactSpend(
	s spendSpec, known map[plutusRef]babbage.BabbageTransactionOutput,
) ([]byte, uint64, common.ExUnits) {
	e.t.Helper()
	s.mem = uint64(e.params.MaxTxExUnits.Memory)  //nolint:gosec
	s.steps = uint64(e.params.MaxTxExUnits.Steps) //nolint:gosec
	tx, _ := e.buildSpend(s)
	used, err := e.measure(tx, known)
	require.NoError(e.t, err, "plutigo evaluation")
	for range 3 {
		s.mem, s.steps = uint64(used.Memory), uint64(used.Steps) //nolint:gosec
		tx, fee := e.buildSpend(s)
		again, err := e.measure(tx, known)
		require.NoError(e.t, err, "plutigo re-evaluation")
		if again == used {
			return tx, fee, used
		}
		used = again
	}
	e.t.Fatalf("plutigo ExUnits did not stabilise")
	return nil, 0, common.ExUnits{}
}

// ---------------------------------------------------------------------------
// Submission to both implementations
// ---------------------------------------------------------------------------

type plutusVerdict struct {
	dingoErr, cardanoErr error
}

func (e *plutusEnv) submitBoth(tx []byte) plutusVerdict {
	return plutusVerdict{
		dingoErr:   plutusSubmit(e.dingoNtc, e.magic, tx),
		cardanoErr: plutusSubmit(e.cardanoNtc, e.magic, tx),
	}
}

func shortErr(err error) string {
	if err == nil {
		return "accepted"
	}
	msg := err.Error()
	if len(msg) > 400 {
		msg = msg[:400] + "..."
	}
	return "REJECTED: " + msg
}

// waitBoth waits until all refs have the wanted presence on both producers
// and returns the outputs as the Dingo producer reports them.
func (e *plutusEnv) waitBoth(
	refs []plutusRef, present bool,
) map[plutusRef]babbage.BabbageTransactionOutput {
	e.t.Helper()
	deadline := time.Now().Add(plutusConfirmTimeout)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		var dingoOut map[plutusRef]babbage.BabbageTransactionOutput
		for _, addr := range []string{e.dingoNtc, e.cardanoNtc} {
			got, err := plutusUTxOsByRef(addr, e.magic, refs)
			if err != nil {
				last = err.Error()
				ok = false
				break
			}
			if addr == e.dingoNtc {
				dingoOut = got
			}
			for _, r := range refs {
				if _, has := got[r]; has != present {
					last = fmt.Sprintf("%s presence=%v at %s", r, has, addr)
					ok = false
				}
			}
		}
		if ok {
			return dingoOut
		}
		time.Sleep(2 * time.Second)
	}
	require.FailNowf(e.t, "confirmation timeout",
		"want present=%v for %v: %s", present, refs, last)
	return nil
}

// requireSameOutputs asserts both producers report byte-identical outputs.
func (e *plutusEnv) requireSameOutputs(refs []plutusRef) {
	e.t.Helper()
	d, err := plutusUTxOsByRef(e.dingoNtc, e.magic, refs)
	require.NoError(e.t, err)
	c, err := plutusUTxOsByRef(e.cardanoNtc, e.magic, refs)
	require.NoError(e.t, err)
	require.Len(e.t, d, len(refs))
	require.Len(e.t, c, len(refs))
	for _, r := range refs {
		do, co := d[r], c[r]
		require.Equal(e.t, outputSummary(e.t, co), outputSummary(e.t, do),
			"output %s differs between dingo and cardano-node", r)
		db, err := cbor.Encode(&do)
		require.NoError(e.t, err)
		cb, err := cbor.Encode(&co)
		require.NoError(e.t, err)
		if !bytes.Equal(db, cb) && !e.noted {
			e.noted = true
			e.t.Logf("NOTE output %s is semantically equal but its LocalStateQuery"+
				" wire encoding differs: cardano-node %x | dingo %x", r, cb, db)
		}
	}
}

// ---------------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------------

type plutusScriptOutputSpec struct {
	name   string
	script string
	datum  []byte
}

func TestPlutusScriptConformance(t *testing.T) {
	keysDir := os.Getenv("DEVNET_GENESIS_KEYS_DIR")
	if keysDir == "" {
		t.Skip("requires genesis keys; run internal/test/devnet/run-tests.sh --conformance")
	}
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err)
	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(t, endpoints, devnet.WithNetworkMagic(cfg.NetworkMagic))
	h.WaitForAllNodesReady(60 * time.Second)

	// The pump spends genesis UTxOs and floods Dingo's mempool; stop it so
	// the funding transaction owns its input.
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

	// Dingo's local-tx-submission reject reply is generic, so surface the
	// reason it logged for every transaction it refused.
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
				if len(line) > 600 {
					line = line[:600]
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

	// Protocol parameters must agree before anything is built from them.
	dingoParams, err := plutusParams(env.dingoNtc, env.magic)
	require.NoError(t, err)
	cardanoParams, err := plutusParams(env.cardanoNtc, env.magic)
	require.NoError(t, err)
	env.params = cardanoParams
	require.Equal(t, cardanoParams.CostModels, dingoParams.CostModels,
		"PlutusV3 cost models differ between dingo and cardano-node")
	require.Equal(t, cardanoParams.MaxTxExUnits, dingoParams.MaxTxExUnits)
	require.Equal(t, cardanoParams.CollateralPercentage, dingoParams.CollateralPercentage)
	require.NotEmpty(t, cardanoParams.CostModels[2], "no PlutusV3 cost model")
	t.Logf("protocol version %d, maxTxExUnits mem=%d steps=%d, "+
		"collateral %d%%, priceMem=%s priceSteps=%s, V3 cost model has %d params",
		cardanoParams.ProtocolMajorVersion(),
		cardanoParams.MaxTxExUnits.Memory, cardanoParams.MaxTxExUnits.Steps,
		cardanoParams.CollateralPercentage,
		cardanoParams.ExecutionCosts.MemPrice.ToBigRat().RatString(),
		cardanoParams.ExecutionCosts.StepPrice.ToBigRat().RatString(),
		len(cardanoParams.CostModels[2]))

	// Let the pump's in-flight transactions leave both mempools.
	time.Sleep(30 * time.Second)

	keys := loadPlutusGenesisKeys(t, keysDir)
	require.NotEmpty(t, keys, "no genesis keys")
	addrs := make([]common.Address, 0, len(keys))
	keyByAddr := map[string]*plutusKey{}
	for _, k := range keys {
		a, err := common.NewAddressFromBytes(k.address)
		require.NoError(t, err)
		addrs = append(addrs, a)
		keyByAddr[string(k.address)] = k
	}

	// Pick the largest genesis UTxO that exists on both nodes.
	dUtxos, err := plutusUTxOsByAddress(env.dingoNtc, env.magic, addrs)
	require.NoError(t, err)
	cUtxos, err := plutusUTxOsByAddress(env.cardanoNtc, env.magic, addrs)
	require.NoError(t, err)
	var fundIn plutusRef
	var fundValue uint64
	// The tx pump spends the enterprise genesis addresses and a chain
	// reorganisation can put its confirmed transactions back into a mempool,
	// so fund from the staked genesis addresses when there are any.
	fundRank := -1
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
		if rank > fundRank || (rank == fundRank && out.OutputAmount.Amount > fundValue) {
			fundRank = rank
			fundValue = out.OutputAmount.Amount
			fundIn = plutusRef{hash: id.Hash, idx: uint32(id.Idx)} //nolint:gosec
			env.key = key
		}
	}
	require.NotNil(t, env.key, "no genesis UTxO is spendable with a known key")
	t.Logf("funding from %s (%d lovelace)", fundIn, fundValue)

	// Baseline: what the two nodes already disagree on before any script
	// transaction exists (epoch 0 stake distribution semantics, for one).
	baseDingo, baseCardano, baseTip := sampleLedgerStateAtCommonPoint(
		t, h, endpoints[0], endpoints[1],
		env.dingoNtc, env.cardanoNtc, env.magic,
		^uint64(0), 2*time.Minute,
	)
	baselineDiff := nodeparity.DiffSnapshots(baseDingo.ledger, baseCardano.ledger)
	t.Logf("BASELINE before script txs at slot %d (block %d): protocol params diff=%q, "+
		"stake distribution diffs=%v, utxo diffs=%d",
		baseTip.SlotNumber, baseTip.BlockNumber, baselineDiff.ProtocolParamsDiff,
		baselineDiff.StakeDistribution, len(baselineDiff.UTxO))

	// -- Script outputs ----------------------------------------------------

	srcs := plutusScripts()
	scripts := map[string]common.PlutusV3Script{}
	hashes := map[string]common.ScriptHash{}
	for name, src := range srcs {
		scripts[name], hashes[name] = plutusScriptFromText(t, src)
		t.Logf("script %-9s hash=%s size=%d", name, hashes[name].String(), len(scripts[name]))
	}

	outputs := []plutusScriptOutputSpec{
		{"always", "always", mustHex("d87980")},
		{"always-bigbytes", "always", mustHex("d87981182f")},
		{"check", "check", mustHex("d8799f182a46706c75747573ff")},
		{"check-mismatch", "check", mustHex("d8799f182aff")},
		{"fail", "fail", mustHex("d87980")},
		{"serialise", "serialise", weirdDatum()},
		{"costly", "costly", mustHex("d87980")},
		{"costly-fee1000", "costly", mustHex("d87981183a")},
		{"costly-fee1", "costly", mustHex("d87981183b")},
		{"always-badhash", "always", mustHex("d87981182a")},
		{"always-invalid", "always", mustHex("d87981182b")},
		{"always-nocoll", "always", mustHex("d87981182c")},
		{"always-overmax", "always", mustHex("d87981182d")},
		{"fail-invalid", "fail", mustHex("d87981182e")},
	}

	scriptRefs := map[string]plutusRef{}
	var fundOutputs [][]byte
	for _, o := range outputs {
		addr := append([]byte{0x70}, hashes[o.script].Bytes()...)
		fundOutputs = append(fundOutputs, plutusOutput(addr, plutusScriptOutput, o.datum))
	}
	// Collateral outputs.
	nColl := 18
	for range nColl {
		fundOutputs = append(fundOutputs, plutusOutput(env.key.address, plutusCollateralOut, nil))
	}
	totalOut := uint64(len(outputs))*plutusScriptOutput + uint64(nColl)*plutusCollateralOut
	fundTx, fundBase := env.fundingTx(fundIn, fundValue, fundOutputs, totalOut)
	for i, o := range outputs {
		scriptRefs[o.name] = plutusRef{hash: fundBase.hash, idx: uint32(i)} //nolint:gosec
	}
	collateral := make([]plutusRef, nColl)
	for i := range collateral {
		collateral[i] = plutusRef{hash: fundBase.hash, idx: uint32(len(outputs) + i)} //nolint:gosec
	}

	v := env.submitBoth(fundTx)
	t.Logf("FUNDING tx %x: dingo %s | cardano-node %s",
		fundBase.hash, shortErr(v.dingoErr), shortErr(v.cardanoErr))
	require.NoError(t, v.dingoErr, "dingo rejected funding tx")
	require.NoError(t, v.cardanoErr, "cardano-node rejected funding tx")

	allRefs := append(append([]plutusRef{}, collateral...), func() []plutusRef {
		var r []plutusRef
		for _, o := range outputs {
			r = append(r, scriptRefs[o.name])
		}
		return r
	}()...)
	known := env.waitBoth(allRefs, true)
	env.requireSameOutputs(allRefs)
	t.Logf("funding tx confirmed on dingo and cardano-node; script UTxOs byte-identical")

	nextColl := 0
	takeColl := func() *plutusRef {
		r := collateral[nextColl]
		nextColl++
		return &r
	}
	spend := func(name string) spendSpec {
		ref := scriptRefs[name]
		return spendSpec{
			scriptIn:    ref,
			scriptValue: plutusScriptOutput,
			script:      scripts[strings.SplitN(findScript(outputs, name), "|", 2)[0]],
			redeemer:    mustHex("d87980"),
			mem:         1_000_000,
			steps:       400_000_000,
			collateral:  takeColl(),
			collValue:   plutusCollateralOut,
		}
	}
	// confirmedSpend waits for the change output of an accepted spend.
	confirmedSpend := func(tx []byte) plutusRef {
		change := plutusRef{hash: txIDFromTx(t, tx), idx: 0}
		env.waitBoth([]plutusRef{change}, true)
		return change
	}

	type step struct {
		name        string
		accept      bool
		exploratory bool // only agreement between the nodes is asserted
		build       func() []byte
		after       func(tx []byte)
	}

	var steps []step
	bigBytesAccepted := false
	maxMem := uint64(cardanoParams.MaxTxExUnits.Memory) //nolint:gosec

	steps = append(steps,
		step{name: "always: isValid=false but script succeeds",
			build: func() []byte {
				s := spend("always")
				s.invalid = true
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "always: wrong script data hash",
			build: func() []byte {
				s := spend("always-badhash")
				s.badHash = true
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "always: no collateral",
			build: func() []byte {
				s := spend("always-nocoll")
				s.collateral = nil
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "always: ExUnits above maxTxExUnits",
			build: func() []byte {
				s := spend("always-overmax")
				s.mem = maxMem + 1
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "always: definite 65-byte bytestring in redeemer (exploratory)",
			exploratory: true,
			build: func() []byte {
				s := spend("always-bigbytes")
				// A single definite-length byte string longer than 64
				// bytes is not valid Plutus Data on the wire.
				s.redeemer = cCat(mustHex("d87981"), cBytes(bytes65()))
				tx, _ := env.buildSpend(s)
				return tx
			},
			after: func(tx []byte) {
				bigBytesAccepted = true
				confirmedSpend(tx)
			}},
		step{name: "always: exact ExUnits", accept: true,
			build: func() []byte {
				s := spend("always")
				s.redeemer = mustHex("d87980")
				tx, fee, used := env.exactSpend(s, known)
				t.Logf("always: plutigo ExUnits mem=%d steps=%d fee=%d", used.Memory, used.Steps, fee)
				return tx
			},
			after: func(tx []byte) { confirmedSpend(tx) }},
		step{name: "check: redeemer != datum",
			build: func() []byte {
				s := spend("check-mismatch")
				s.redeemer = mustHex("d8799f182bff")
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "check: ctx purpose, fee and redeemer==datum (different encodings)", accept: true,
			build: func() []byte {
				s := spend("check")
				s.redeemer = mustHex("d87982182a46706c75747573")
				tx, fee, used := env.exactSpend(s, known)
				t.Logf("check: plutigo ExUnits mem=%d steps=%d fee=%d", used.Memory, used.Steps, fee)
				return tx
			},
			after: func(tx []byte) { confirmedSpend(tx) }},
		step{name: "fail: error script, isValid=true",
			build: func() []byte {
				tx, _ := env.buildSpend(spend("fail"))
				return tx
			}},
		step{name: "serialiseData: indefinite/chunked datum and redeemer", accept: true,
			build: func() []byte {
				s := spend("serialise")
				s.redeemer = weirdRedeemer()
				tx, fee, used := env.exactSpend(s, known)
				t.Logf("serialise: plutigo ExUnits mem=%d steps=%d fee=%d", used.Memory, used.Steps, fee)
				return tx
			},
			after: func(tx []byte) { confirmedSpend(tx) }},
		step{name: "costly: mem-1",
			build: func() []byte {
				s := spend("costly")
				_, _, used := env.exactSpend(s, known)
				s.mem, s.steps = uint64(used.Memory-1), uint64(used.Steps) //nolint:gosec
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "costly: steps-1",
			build: func() []byte {
				s := spend("costly")
				_, _, used := env.exactSpend(s, known)
				s.mem, s.steps = uint64(used.Memory), uint64(used.Steps-1) //nolint:gosec
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "costly: fee 1000 lovelace below minimum",
			build: func() []byte {
				s := spend("costly-fee1000")
				_, _, used := env.exactSpend(s, known)
				s.mem, s.steps = uint64(used.Memory), uint64(used.Steps) //nolint:gosec
				s.feeDelta = -1000
				tx, _ := env.buildSpend(s)
				return tx
			}},
		// cardano-node reports MinimumFee 314771 for a transaction this test
		// sizes at 314815: its fee check prices one byte (MinFeeA lovelace)
		// less than the serialised length, so the true minimum is
		// solved fee - MinFeeA and the first rejected fee is one below it.
		step{name: "costly: fee 1 lovelace below the node's minimum",
			build: func() []byte {
				s := spend("costly-fee1")
				_, _, used := env.exactSpend(s, known)
				s.mem, s.steps = uint64(used.Memory), uint64(used.Steps) //nolint:gosec
				s.feeDelta = -int64(cardanoParams.MinFeeA) - 1
				tx, _ := env.buildSpend(s)
				return tx
			}},
		step{name: "costly: exact ExUnits and exactly the minimum fee", accept: true,
			build: func() []byte {
				s := spend("costly")
				s.feeDelta = -int64(cardanoParams.MinFeeA)
				tx, fee, used := env.exactSpend(s, known)
				t.Logf("costly: plutigo ExUnits mem=%d steps=%d fee=%d", used.Memory, used.Steps, fee)
				return tx
			},
			after: func(tx []byte) { confirmedSpend(tx) }},
	)

	for _, s := range steps {
		tx := s.build()
		v := env.submitBoth(tx)
		t.Logf("STEP %q tx %x: dingo %s | cardano-node %s",
			s.name, txIDFromTx(t, tx), shortErr(v.dingoErr), shortErr(v.cardanoErr))
		dingoOK, cardanoOK := v.dingoErr == nil, v.cardanoErr == nil
		if dingoOK != cardanoOK {
			t.Errorf("DISAGREEMENT on %q: dingo accepted=%v cardano-node accepted=%v",
				s.name, dingoOK, cardanoOK)
		}
		if !s.exploratory && (dingoOK != s.accept || cardanoOK != s.accept) {
			t.Errorf("unexpected verdict on %q: want accepted=%v, dingo=%v cardano-node=%v",
				s.name, s.accept, dingoOK, cardanoOK)
		}
		if (s.accept || s.exploratory) && dingoOK && cardanoOK && s.after != nil {
			s.after(tx)
		}
	}

	// isValid=false with a failing script: both accept and take collateral.
	{
		s := spend("fail-invalid")
		coll := *s.collateral
		s.invalid = true
		tx, fee := env.buildSpend(s)
		v := env.submitBoth(tx)
		t.Logf("STEP %q tx %x (fee %d): dingo %s | cardano-node %s",
			"fail: isValid=false collateral path", txIDFromTx(t, tx), fee,
			shortErr(v.dingoErr), shortErr(v.cardanoErr))
		if (v.dingoErr == nil) != (v.cardanoErr == nil) {
			t.Errorf("DISAGREEMENT on isValid=false collateral path: dingo=%v cardano-node=%v",
				shortErr(v.dingoErr), shortErr(v.cardanoErr))
		}
		if v.dingoErr == nil && v.cardanoErr == nil {
			txid := txIDFromTx(t, tx)
			env.waitBoth([]plutusRef{coll}, false)
			ret := plutusRef{hash: txid, idx: 1}
			env.waitBoth([]plutusRef{ret}, true)
			env.requireSameOutputs([]plutusRef{ret, scriptRefs["fail-invalid"]})
			env.waitBoth([]plutusRef{{hash: txid, idx: 0}}, false)
		} else {
			t.Errorf("isValid=false collateral transaction must be accepted by both")
		}
	}

	// -- Final state comparison -------------------------------------------

	sampleTimeout := 2 * time.Minute
	dingoState, cardanoState, tip := sampleLedgerStateAtCommonPoint(
		t, h, endpoints[0], endpoints[1],
		env.dingoNtc, env.cardanoNtc, env.magic,
		^uint64(0), sampleTimeout,
	)
	diff := nodeparity.DiffSnapshots(dingoState.ledger, cardanoState.ledger)
	t.Logf("final comparison at slot %d (block %d): protocol params diff=%q, "+
		"stake distribution diffs=%v, utxo diffs=%d (+%d truncated)",
		tip.SlotNumber, tip.BlockNumber, diff.ProtocolParamsDiff,
		diff.StakeDistribution, len(diff.UTxO), diff.TruncatedUTxO)
	require.Empty(t, diff.ProtocolParamsDiff)
	require.Empty(t, diff.UTxO, "UTxO sets diverged")
	require.Zero(t, diff.TruncatedUTxO)
	if len(baselineDiff.StakeDistribution) == 0 {
		require.Empty(t, diff.StakeDistribution,
			"stake distribution matched before the script transactions but not after")
	}
	require.Equal(t, cardanoState.pots, dingoState.pots)
	t.Logf("final ledger state identical at slot %d (block %d): %d UTxOs",
		tip.SlotNumber, tip.BlockNumber, len(dingoState.ledger.UTxOEntries))

	// Unspent script outputs must still be there; spent ones gone.
	for _, name := range []string{
		"always", "check", "serialise", "costly",
	} {
		env.waitBoth([]plutusRef{scriptRefs[name]}, false)
	}
	env.waitBoth([]plutusRef{scriptRefs["always-bigbytes"]}, !bigBytesAccepted)
	for _, name := range []string{
		"check-mismatch", "fail", "fail-invalid", "costly-fee1000", "costly-fee1",
		"always-badhash", "always-invalid", "always-nocoll", "always-overmax",
	} {
		env.waitBoth([]plutusRef{scriptRefs[name]}, true)
	}
}

func mustAddrBytes(t *testing.T, out babbage.BabbageTransactionOutput) []byte {
	t.Helper()
	b, err := out.Address().Bytes()
	require.NoError(t, err)
	return b
}

func findScript(outputs []plutusScriptOutputSpec, name string) string {
	for _, o := range outputs {
		if o.name == name {
			return o.script
		}
	}
	return ""
}

// outputSummary reduces an output to its ledger-visible content, ignoring the
// legacy-array or map wire encoding the answering node chose.
func outputSummary(t *testing.T, out babbage.BabbageTransactionOutput) string {
	t.Helper()
	addr := mustAddrBytes(t, out)
	summary := fmt.Sprintf("addr=%x coin=%d", addr, out.OutputAmount.Amount)
	if assets := out.Assets(); assets != nil {
		enc, err := cbor.Encode(assets)
		require.NoError(t, err)
		summary += fmt.Sprintf(" assets=%x", enc)
	}
	if h := out.DatumHash(); h != nil {
		summary += fmt.Sprintf(" datumHash=%x", h.Bytes())
	}
	if d := out.Datum(); d != nil {
		summary += fmt.Sprintf(" inlineDatum=%x", d.Cbor())
	}
	if ref := out.ScriptRef(); ref != nil {
		summary += fmt.Sprintf(" scriptRef=%v", ref)
	}
	return summary
}
