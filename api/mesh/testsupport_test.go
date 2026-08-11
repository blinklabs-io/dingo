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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// Fixed network identity shared by every test in the package so that
// requests can be built without threading configuration through each
// helper. The magic deliberately differs from mainnetMagic so the
// default server under test uses testnet-prefixed addresses.
const (
	testNetwork      = "preview"
	testNetworkMagic = uint32(2)
	testGenesisHash  = "268ae601af9f5e0d5e0a3e8f8a3a19d0a2ac6b93" +
		"b6f5d8e3c8fbc9d1a2b3c4d5"
	testGenesisStartTimeSec = int64(1666656000)
)

// --- dependency doubles -------------------------------------------------
//
// Each double implements one of the package-local dependency interfaces
// from node_interface.go. Behavior is supplied per test through function
// fields; a nil field means "return the zero value", which keeps tests
// that only care about one dependency free of unrelated setup.

// fakeChain is a MeshChain returning a fixed tip.
type fakeChain struct {
	tip ochainsync.Tip
}

func (f *fakeChain) Tip() ochainsync.Tip { return f.tip }

// fakeDatabase is a MeshDatabase whose lookups are supplied per test.
type fakeDatabase struct {
	blockByHash    func(hash []byte) (models.Block, error)
	blockByIndex   func(idx uint64) (models.Block, error)
	txByHash       func(hash []byte) (*models.Transaction, error)
	txsByBlockHash func(hash []byte) ([]models.Transaction, error)
}

func (f *fakeDatabase) BlockByHash(
	hash []byte,
) (models.Block, error) {
	if f.blockByHash == nil {
		return models.Block{}, models.ErrBlockNotFound
	}
	return f.blockByHash(hash)
}

func (f *fakeDatabase) BlockByIndex(
	idx uint64,
) (models.Block, error) {
	if f.blockByIndex == nil {
		return models.Block{}, models.ErrBlockNotFound
	}
	return f.blockByIndex(idx)
}

func (f *fakeDatabase) GetTransactionByHash(
	hash []byte,
) (*models.Transaction, error) {
	if f.txByHash == nil {
		return nil, nil
	}
	return f.txByHash(hash)
}

func (f *fakeDatabase) GetTransactionsByBlockHash(
	hash []byte,
) ([]models.Transaction, error) {
	if f.txsByBlockHash == nil {
		return nil, nil
	}
	return f.txsByBlockHash(hash)
}

// fakeLedgerState is a MeshLedgerState with per-test behavior.
type fakeLedgerState struct {
	pparams    lcommon.ProtocolParameters
	slotToTime func(slot uint64) (time.Time, error)
	utxos      func(addr lcommon.Address) ([]models.Utxo, error)
}

func (f *fakeLedgerState) GetCurrentPParams() lcommon.ProtocolParameters {
	return f.pparams
}

func (f *fakeLedgerState) SlotToTime(
	slot uint64,
) (time.Time, error) {
	if f.slotToTime == nil {
		// Mirror the ledger's Shelley-era 1s slots so handlers that
		// do not exercise the fallback path get stable timestamps.
		return time.Unix(
			testGenesisStartTimeSec+int64(slot), 0,
		).UTC(), nil
	}
	return f.slotToTime(slot)
}

func (f *fakeLedgerState) UtxosByAddress(
	addr lcommon.Address,
) ([]models.Utxo, error) {
	if f.utxos == nil {
		return nil, nil
	}
	return f.utxos(addr)
}

// submittedTx records one accepted MeshMempool.AddTransaction call.
type submittedTx struct {
	txType  uint
	txBytes []byte
}

// fakeMempool is a MeshMempool backed by an in-memory transaction list.
// Submissions are recorded so tests can assert what reached the mempool.
type fakeMempool struct {
	mu        sync.Mutex
	txs       []mempool.MempoolTransaction
	addErr    error
	submitted []submittedTx
}

func (f *fakeMempool) AddTransaction(
	txType uint,
	txBytes []byte,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addErr != nil {
		return f.addErr
	}
	f.submitted = append(f.submitted, submittedTx{
		txType:  txType,
		txBytes: bytes.Clone(txBytes),
	})
	return nil
}

func (f *fakeMempool) GetTransaction(
	hash string,
) (mempool.MempoolTransaction, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, tx := range f.txs {
		if tx.Hash == hash {
			return tx, true
		}
	}
	return mempool.MempoolTransaction{}, false
}

func (f *fakeMempool) Transactions() []mempool.MempoolTransaction {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append(
		[]mempool.MempoolTransaction(nil), f.txs...,
	)
}

func (f *fakeMempool) submissions() []submittedTx {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]submittedTx(nil), f.submitted...)
}

// --- server construction ------------------------------------------------

// testDeps bundles the doubles handed to a test server so a test can
// reach into them after wiring.
type testDeps struct {
	chain    *fakeChain
	database *fakeDatabase
	ledger   *fakeLedgerState
	mempool  *fakeMempool
}

// newTestDeps returns doubles with no configured behavior.
func newTestDeps() *testDeps {
	return &testDeps{
		chain:    &fakeChain{},
		database: &fakeDatabase{},
		ledger:   &fakeLedgerState{},
		mempool:  &fakeMempool{},
	}
}

// serverOption mutates the ServerConfig before NewServer is called.
type serverOption func(*ServerConfig)

// newTestServer builds a Server over the supplied doubles. It fails the
// test if construction fails, so callers testing validation errors should
// call NewServer directly.
func newTestServer(
	t *testing.T,
	deps *testDeps,
	opts ...serverOption,
) *Server {
	t.Helper()
	cfg := ServerConfig{
		LedgerState:         deps.ledger,
		Database:            deps.database,
		Chain:               deps.chain,
		Mempool:             deps.mempool,
		Network:             testNetwork,
		NetworkMagic:        testNetworkMagic,
		GenesisHash:         testGenesisHash,
		GenesisStartTimeSec: testGenesisStartTimeSec,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	srv, err := NewServer(cfg)
	require.NoError(t, err)
	return srv
}

// newTestHandler returns the routed handler for a server built over the
// supplied doubles, plus the doubles themselves.
func newTestHandler(
	t *testing.T,
	deps *testDeps,
	opts ...serverOption,
) http.Handler {
	t.Helper()
	srv := newTestServer(t, deps, opts...)
	mux := http.NewServeMux()
	srv.registerRoutes(mux)
	return mux
}

// --- request helpers ----------------------------------------------------

// testNetworkID returns the NetworkIdentifier accepted by a test server.
func testNetworkID() *NetworkIdentifier {
	return &NetworkIdentifier{
		Blockchain: blockchain,
		Network:    testNetwork,
	}
}

// postJSON marshals body and posts it to path.
func postJSON(
	t *testing.T,
	h http.Handler,
	path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return postRaw(t, h, path, string(raw))
}

// postRaw posts an unmarshaled request body, for malformed-input cases.
func postRaw(
	t *testing.T,
	h http.Handler,
	path string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(
		http.MethodPost, path, bytes.NewBufferString(body),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// decodeResponse decodes a successful JSON response into T, asserting
// the 200 status first so a failure reports the endpoint's error body.
func decodeResponse[T any](
	t *testing.T,
	rec *httptest.ResponseRecorder,
) *T {
	t.Helper()
	require.Equal(
		t, http.StatusOK, rec.Code,
		"unexpected status, body: %s", rec.Body.String(),
	)
	require.Equal(
		t,
		"application/json",
		rec.Header().Get("Content-Type"),
	)
	var out T
	require.NoError(
		t, json.Unmarshal(rec.Body.Bytes(), &out),
	)
	return &out
}

// requireMeshError asserts that the response is the given Mesh error
// with the given HTTP status, and returns the decoded error so callers
// can inspect its details.
func requireMeshError(
	t *testing.T,
	rec *httptest.ResponseRecorder,
	want *Error,
	wantStatus int,
) *Error {
	t.Helper()
	require.Equal(
		t, wantStatus, rec.Code,
		"unexpected status, body: %s", rec.Body.String(),
	)
	var got Error
	require.NoError(
		t, json.Unmarshal(rec.Body.Bytes(), &got),
	)
	require.Equal(t, want.Code, got.Code)
	require.Equal(t, want.Message, got.Message)
	require.Equal(t, want.Retriable, got.Retriable)
	return &got
}

// --- fixture builders ---------------------------------------------------

// mustDecodeHex decodes a hex string or fails the test.
func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// hexString hex-encodes bytes for comparison against response fields.
func hexString(b []byte) string { return hex.EncodeToString(b) }

// testHash returns a deterministic 32-byte hash seeded by b.
func testHash(b byte) []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = b
	}
	return h
}

// testKeyHash returns a deterministic 28-byte credential hash.
func testKeyHash(b byte) []byte {
	h := make([]byte, 28)
	for i := range h {
		h[i] = b
	}
	return h
}

// testAddress builds a bech32 testnet address from raw credentials.
func testAddress(
	t *testing.T,
	addrType uint8,
	payment []byte,
	staking []byte,
) string {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		payment,
		staking,
	)
	require.NoError(t, err)
	return addr.String()
}

// testUtxo builds a UTxO owned by a key-only testnet address.
func testUtxo(
	txID []byte,
	idx uint32,
	lovelace uint64,
	paymentKey []byte,
	assets []models.Asset,
) models.Utxo {
	return models.Utxo{
		TxId:       txID,
		OutputIdx:  idx,
		Amount:     types.Uint64(lovelace),
		PaymentKey: paymentKey,
		Assets:     assets,
	}
}

// testAsset builds a native asset holding.
func testAsset(
	policyID []byte,
	name []byte,
	amount uint64,
) models.Asset {
	return models.Asset{
		PolicyId: policyID,
		Name:     name,
		Amount:   types.Uint64(amount),
	}
}

// --- transaction fixtures -----------------------------------------------

// testTxBody encodes a minimal Conway transaction body spending the
// given inputs into the given outputs. It mirrors the body shape
// produced by /construction/payloads so fixtures and the construction
// flow stay in agreement.
func testTxBody(
	t *testing.T,
	inputs []shelley.ShelleyTransactionInput,
	outputs []babbage.BabbageTransactionOutput,
	fee uint64,
) []byte {
	t.Helper()
	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			inputs,
		),
		TxOutputs: outputs,
		TxFee:     fee,
	}
	bodyCbor, err := cbor.Encode(&body)
	require.NoError(t, err)
	return bodyCbor
}

// testSignedTx wraps an encoded body in the signed-transaction envelope
// [body, witness_set, is_valid, auxiliary_data].
func testSignedTx(
	t *testing.T,
	bodyCbor []byte,
	witnesses []lcommon.VkeyWitness,
) []byte {
	t.Helper()
	signed := []any{
		cbor.RawMessage(bodyCbor),
		map[int]any{0: witnesses},
		true,
		nil,
	}
	txCbor, err := cbor.Encode(signed)
	require.NoError(t, err)
	return txCbor
}

// testOutput builds a lovelace-only transaction output.
func testOutput(
	t *testing.T,
	address string,
	lovelace uint64,
) babbage.BabbageTransactionOutput {
	t.Helper()
	addr, err := lcommon.NewAddress(address)
	require.NoError(t, err)
	return babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount: mary.MaryTransactionOutputValue{
			Amount: lovelace,
		},
	}
}

// testKeyPair returns a deterministic ed25519 key pair. The seed byte
// keeps separate signers distinguishable within a test.
func testKeyPair(
	t *testing.T,
	seed byte,
) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	seedBytes := make([]byte, ed25519.SeedSize)
	for i := range seedBytes {
		seedBytes[i] = seed
	}
	priv := ed25519.NewKeyFromSeed(seedBytes)
	pub, ok := priv.Public().(ed25519.PublicKey)
	require.True(t, ok)
	return pub, priv
}

// testSimpleSignedTx builds a signed single-input, single-output
// transaction and returns its CBOR alongside the decoded transaction.
func testSimpleSignedTx(
	t *testing.T,
	address string,
) ([]byte, gledger.Transaction) {
	t.Helper()
	pub, _ := testKeyPair(t, 0x42)
	bodyCbor := testTxBody(
		t,
		[]shelley.ShelleyTransactionInput{
			shelley.NewShelleyTransactionInput(
				hexString(testHash(0xd1)), 0,
			),
		},
		[]babbage.BabbageTransactionOutput{
			testOutput(t, address, 1_000_000),
		},
		170_000,
	)
	txCbor := testSignedTx(
		t,
		bodyCbor,
		[]lcommon.VkeyWitness{
			{
				Vkey:      pub,
				Signature: make([]byte, 64),
			},
		},
	)
	txType, err := gledger.DetermineTransactionType(txCbor)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(txType, txCbor)
	require.NoError(t, err)
	return txCbor, tx
}

// testTxBodyWithCerts encodes a Conway body carrying certificates
// alongside a single input and output, for exercising the certificate
// paths in the operation converter.
func testTxBodyWithCerts(
	t *testing.T,
	address string,
	certs []lcommon.CertificateWrapper,
) []byte {
	t.Helper()
	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{
				shelley.NewShelleyTransactionInput(
					hexString(testHash(0xd2)), 0,
				),
			},
		),
		TxOutputs: []babbage.BabbageTransactionOutput{
			testOutput(t, address, 1_000_000),
		},
		TxFee:          170_000,
		TxCertificates: certs,
	}
	bodyCbor, err := cbor.Encode(&body)
	require.NoError(t, err)
	return bodyCbor
}

// ed25519Sign signs message with an ed25519 private key.
func ed25519Sign(priv []byte, message []byte) []byte {
	return ed25519.Sign(ed25519.PrivateKey(priv), message)
}

// testPParams returns Conway protocol parameters with every rational
// field populated, so conversion to the utxorpc representation (which
// rejects nil rationals) succeeds.
func testPParams(
	minFeeA uint,
	minFeeB uint,
) *conway.ConwayProtocolParameters {
	rat := func(num, denom int64) *cbor.Rat {
		return &cbor.Rat{Rat: big.NewRat(num, denom)}
	}
	return &conway.ConwayProtocolParameters{
		MinFeeA:            minFeeA,
		MinFeeB:            minFeeB,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2_000_000,
		PoolDeposit:        500_000_000,
		MaxEpoch:           18,
		NOpt:               150,
		A0:                 rat(3, 10),
		Rho:                rat(3, 1000),
		Tau:                rat(2, 10),
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
		},
		MinPoolCost:    170_000_000,
		AdaPerUtxoByte: 4310,
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  rat(577, 10000),
			StepPrice: rat(721, 10000000),
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 14_000_000,
			Steps:  10_000_000_000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62_000_000,
			Steps:  20_000_000_000,
		},
		MaxValueSize:               5000,
		CollateralPercentage:       150,
		MaxCollateralInputs:        3,
		MinFeeRefScriptCostPerByte: rat(15, 1),
	}
}

// newRequest builds a request with no body for method assertions.
func newRequest(
	t *testing.T,
	method string,
	path string,
) *http.Request {
	t.Helper()
	return httptest.NewRequest(method, path, nil)
}

// recordRequest serves req against h and returns the recorder.
func recordRequest(
	h http.Handler,
	req *http.Request,
) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// newDiscardWriter returns a writer that drops everything written to
// it, for silencing component loggers in tests.
func newDiscardWriter() io.Writer { return io.Discard }
