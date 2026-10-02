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

package signer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSlotsPerKESPeriod = 129600
	// The operational certificate starts at period 2 and the clock is in
	// period 5, so registration is at relative period 3.
	testOpCertStartPeriod = 2
	testKESPeriod         = 5
	testRelativeKESPeriod = testKESPeriod - testOpCertStartPeriod
	testSlot              = testKESPeriod*testSlotsPerKESPeriod + 7
	testEpoch             = 10
	testWait              = 10 * time.Second
)

// testParams makes every lottery index win, so the won indexes are exactly
// 0..M-1.
var testParams = mithril.ProtocolParameters{K: 2, M: 8, PhiF: 1.0}

// testNextParams differ from testParams so that the next epoch's parameters
// are told apart from the current ones in the signed message.
var testNextParams = mithril.ProtocolParameters{K: 3, M: 8, PhiF: 1.0}

func testGenesis() *shelley.ShelleyGenesis {
	return &shelley.ShelleyGenesis{
		SlotsPerKESPeriod: testSlotsPerKESPeriod,
		MaxKESEvolutions:  62,
	}
}

type fakeLedger struct {
	opCertSequence uint64
	registered     bool
}

func (l fakeLedger) PoolRegistrationVRFKeyHash(
	[28]byte,
) ([32]byte, bool, error) {
	return [32]byte{1}, l.registered, nil
}

func (l fakeLedger) LatestOpCertSequence([28]byte) (uint64, bool, error) {
	return l.opCertSequence, l.registered, nil
}

// poolKeys holds a generated pool's key files, written with owner-only
// permissions.
type poolKeys struct {
	kes, opCert, coldVKey, stmKey string
	kesVKey, coldVKeyBytes        []byte
	opCertStart                   uint64
	opCertSignature               []byte
}

var testKESSeed = func() []byte {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	return seed
}()

// newKESSecret returns a fresh copy of the pool's KES key at period 0.
func newKESSecret(t *testing.T) (*kes.SecretKey, []byte) {
	t.Helper()
	secret, vkey, err := bursa.GetKESKeyPair(testKESSeed)
	require.NoError(t, err)
	return secret, vkey
}

// newPoolKeys generates a KES key, a cold key and an operational certificate
// starting at opCertStart, signed by the cold key as the ledger requires.
func newPoolKeys(t *testing.T, opCertStart uint64) poolKeys {
	t.Helper()
	dir := t.TempDir()
	keys := poolKeys{
		kes:      filepath.Join(dir, "kes.skey"),
		opCert:   filepath.Join(dir, "opcert.cert"),
		coldVKey: filepath.Join(dir, "cold.vkey"),
		stmKey:   filepath.Join(dir, "stm.key"),
	}
	writeKey := func(path string, file bursa.KeyFile) {
		data, err := json.Marshal(file)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, data, 0o600))
		testutil.RestrictFileToCurrentUser(t, path)
	}
	secret, kesVKey := newKESSecret(t)
	keys.kesVKey = kesVKey
	kesFile, err := bursa.GetKESSKey(secret)
	require.NoError(t, err)
	writeKey(keys.kes, kesFile)

	coldKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	coldVKey := []byte(coldKey.Public().(ed25519.PublicKey))
	keys.coldVKeyBytes = coldVKey
	keys.opCertStart = opCertStart
	writeKey(keys.coldVKey, bursa.KeyFile{
		Type:    "StakePoolVerificationKey_ed25519",
		CborHex: "5820" + hex.EncodeToString(coldVKey),
	})

	// The cold signature covers the KES key, issue number and start period.
	const issueNumber = 0
	signable := binary.BigEndian.AppendUint64(
		binary.BigEndian.AppendUint64(slices.Clone(kesVKey), issueNumber),
		opCertStart,
	)
	keys.opCertSignature = ed25519.Sign(coldKey, signable)
	opCert, err := cbor.Encode([]any{
		[]any{
			kesVKey,
			uint64(issueNumber),
			opCertStart,
			keys.opCertSignature,
		},
		coldVKey,
	})
	require.NoError(t, err)
	writeKey(keys.opCert, bursa.KeyFile{
		Type:    "NodeOperationalCertificate",
		CborHex: hex.EncodeToString(opCert),
	})
	return keys
}

// stubAggregator serves the aggregator endpoints the signer uses for a fixed
// epoch and records what the signer submits.
type stubAggregator struct {
	srv *httptest.Server

	// Set before the signer runs.
	current, next []mithril.AggregatorSigner
	stakes        map[string]uint64

	// failEpochSettings is the number of epoch-settings requests to fail.
	failEpochSettings atomic.Int32
	// signatureStatus is the status returned for the next signature
	// submissions, one per element, then 201.
	signatureStatuses chan int
	epochSettingsHits atomic.Int32
	hitsMu            sync.Mutex
	epochSettingsAt   []time.Time
	registeredHits    atomic.Int32

	registrations chan map[string]any
	signatures    chan map[string]any
}

func newStubAggregator(t *testing.T) *stubAggregator {
	t.Helper()
	stub := &stubAggregator{
		stakes:            map[string]uint64{},
		signatureStatuses: make(chan int, 8),
		registrations:     make(chan map[string]any, 8),
		signatures:        make(chan map[string]any, 8),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/epoch-settings",
		func(w http.ResponseWriter, _ *http.Request) {
			stub.epochSettingsHits.Add(1)
			stub.hitsMu.Lock()
			stub.epochSettingsAt = append(stub.epochSettingsAt, time.Now())
			stub.hitsMu.Unlock()
			if stub.failEpochSettings.Add(-1) >= 0 {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, mithril.EpochSettings{
				Epoch:          testEpoch,
				CurrentSigners: stub.current,
				NextSigners:    stub.next,
			})
		},
	)
	mux.HandleFunc(
		"/protocol-configuration/",
		func(w http.ResponseWriter, r *http.Request) {
			params := map[string]mithril.ProtocolParameters{
				"/protocol-configuration/" + strconv.Itoa(testEpoch):   testParams,
				"/protocol-configuration/" + strconv.Itoa(testEpoch+1): testNextParams,
			}
			p, ok := params[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, mithril.ProtocolConfiguration{ProtocolParameters: p})
		},
	)
	mux.HandleFunc(
		"/signers/registered/",
		func(w http.ResponseWriter, r *http.Request) {
			stub.registeredHits.Add(1)
			// Signers sign two epochs after registering: the current set
			// registered two epochs ago, the next set one epoch ago.
			var signers []mithril.AggregatorSigner
			switch r.URL.Path {
			case "/signers/registered/" + strconv.Itoa(testEpoch-2):
				signers = stub.current
			case "/signers/registered/" + strconv.Itoa(testEpoch-1):
				signers = stub.next
			default:
				http.NotFound(w, r)
				return
			}
			registeredAt, _ := strconv.ParseUint(
				strings.TrimPrefix(r.URL.Path, "/signers/registered/"), 10, 64,
			)
			out := mithril.RegisteredSigners{
				RegisteredAt: registeredAt,
				SigningAt:    registeredAt + 2,
			}
			for _, signer := range signers {
				out.Registrations = append(
					out.Registrations,
					mithril.StakeDistributionParty{
						PartyID: signer.PartyID,
						Stake:   stub.stakes[signer.PartyID],
					},
				)
			}
			writeJSON(w, out)
		},
	)
	mux.HandleFunc(
		"/register-signer",
		func(w http.ResponseWriter, r *http.Request) {
			stub.registrations <- decodeBody(t, r)
			w.WriteHeader(http.StatusCreated)
		},
	)
	mux.HandleFunc(
		"/register-signatures",
		func(w http.ResponseWriter, r *http.Request) {
			stub.signatures <- decodeBody(t, r)
			status := http.StatusCreated
			select {
			case status = <-stub.signatureStatuses:
			default:
			}
			w.WriteHeader(status)
		},
	)
	stub.srv = httptest.NewServer(mux)
	t.Cleanup(stub.srv.Close)
	return stub
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func decodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	var body map[string]any
	assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
	return body
}

// addSigners fills the stub's signer sets: the given signer among three
// others in each, all with equal stake.
func (s *stubAggregator) addSigners(
	t *testing.T,
	own *Signer,
	ownIsCurrent bool,
) {
	t.Helper()
	encoded, err := own.stmVK.Encode()
	require.NoError(t, err)
	ours := mithril.AggregatorSigner{
		PartyID:         own.PartyID(),
		VerificationKey: encoded,
	}
	s.stakes[ours.PartyID] = 1000
	build := func(includeOurs bool) []mithril.AggregatorSigner {
		var signers []mithril.AggregatorSigner
		if includeOurs {
			signers = append(signers, ours)
		}
		for i := range 3 {
			key, err := mithril.NewSTMSigningKey()
			require.NoError(t, err)
			vk, err := key.VerificationKey()
			require.NoError(t, err)
			encoded, err := vk.Encode()
			require.NoError(t, err)
			other := mithril.AggregatorSigner{
				PartyID:         fmt.Sprintf("pool-other-%d", i),
				VerificationKey: encoded,
			}
			s.stakes[other.PartyID] = 1000
			signers = append(signers, other)
		}
		return signers
	}
	s.current = build(ownIsCurrent)
	s.next = build(true)
}

type testSigner struct {
	*Signer
	keys poolKeys
	stub *stubAggregator
}

type testOptions struct {
	slot         func() (uint64, error)
	ledger       forging.LedgerView
	ownIsCurrent bool
	minBackoff   time.Duration
	maxBackoff   time.Duration
}

func newTestSigner(t *testing.T, opts testOptions) *testSigner {
	t.Helper()
	if opts.slot == nil {
		opts.slot = func() (uint64, error) { return testSlot, nil }
	}
	if opts.ledger == nil {
		opts.ledger = fakeLedger{}
	}
	if opts.minBackoff == 0 {
		opts.minBackoff, opts.maxBackoff = time.Millisecond, 5*time.Millisecond
	}
	keys := newPoolKeys(t, testOpCertStartPeriod)
	stub := newStubAggregator(t)
	s, err := New(Config{
		KESKeyPath:          keys.kes,
		OperationalCertPath: keys.opCert,
		ColdVKeyPath:        keys.coldVKey,
		STMKeyPath:          keys.stmKey,
		Genesis:             testGenesis(),
		Client: mithril.NewClient(
			stub.srv.URL,
			mithril.WithAllowInsecureHTTP(),
		),
		Slot:         opts.slot,
		Ledger:       opts.ledger,
		PromRegistry: prometheus.NewRegistry(),
		PollInterval: time.Millisecond,
		MinBackoff:   opts.minBackoff,
		MaxBackoff:   opts.maxBackoff,
	})
	require.NoError(t, err)
	stub.addSigners(t, s, opts.ownIsCurrent)
	return &testSigner{Signer: s, keys: keys, stub: stub}
}

// start runs the signer until the test ends.
func (s *testSigner) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(
			t,
			testutil.RequireReceive(t, done, testWait, "Run returns"),
		)
	})
}

func TestRunRegistersWithKESBoundVerificationKey(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: true})
	s.start(t)

	got := testutil.RequireReceive(
		t,
		s.stub.registrations,
		testWait,
		"registration",
	)

	// The KES key is evolved to the registration's relative period and
	// signs the verification key with its proof of possession. Expected
	// values come from the generated key material, not from the signer.
	secret, _ := newKESSecret(t)
	for range testRelativeKESPeriod {
		var err error
		secret, err = kes.Update(secret)
		require.NoError(t, err)
	}
	kesSignature, err := kes.Sign(
		secret,
		testRelativeKESPeriod,
		s.stmVK.Bytes(),
	)
	require.NoError(t, err)
	require.True(t, kes.VerifySignedKES(
		s.keys.kesVKey, testRelativeKESPeriod, s.stmVK.Bytes(), kesSignature,
	))
	wantKESSignature, err := mithril.EncodeKESSignature(kesSignature)
	require.NoError(t, err)
	wantOpCert, err := mithril.EncodeOperationalCertificate(
		s.keys.kesVKey, 0, s.keys.opCertStart,
		s.keys.opCertSignature, s.keys.coldVKeyBytes,
	)
	require.NoError(t, err)
	wantVK, err := s.stmVK.Encode()
	require.NoError(t, err)
	wantPartyID := lcommon.PoolId(
		lcommon.Blake2b224Hash(s.keys.coldVKeyBytes),
	).String()
	assert.Equal(t, map[string]any{
		"epoch":                      float64(testEpoch + 1),
		"party_id":                   wantPartyID,
		"verification_key":           wantVK,
		"verification_key_signature": wantKESSignature,
		"operational_certificate":    wantOpCert,
		"kes_period":                 float64(testRelativeKESPeriod),
	}, got)
	assert.Equal(t, wantPartyID, s.PartyID())
	assert.True(t, strings.HasPrefix(s.PartyID(), "pool1"))
}

// The aggregator opens its registration round for the epoch after the
// current one, the epoch the registration is recorded at, and rejects a
// registration naming any other epoch.
func TestRunRegistersForTheOpenRegistrationRound(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: true})
	s.start(t)

	got := testutil.RequireReceive(
		t,
		s.stub.registrations,
		testWait,
		"registration",
	)
	assert.Equal(t, float64(testEpoch+1), got["epoch"])
}

func TestRunSubmitsStakeDistributionSignature(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: true})
	s.start(t)

	got := testutil.RequireReceive(t, s.stub.signatures, testWait, "signature")

	// Rebuild the signed message and signature from the stub's signer sets.
	registration := func(signers []mithril.AggregatorSigner) *mithril.STMClosedRegistration {
		var parties []mithril.MithrilStakeDistributionParty
		for _, signer := range signers {
			parties = append(parties, mithril.MithrilStakeDistributionParty{
				PartyID:         signer.PartyID,
				Stake:           s.stub.stakes[signer.PartyID],
				VerificationKey: signer.VerificationKey,
			})
		}
		reg, err := mithril.NewSTMClosedRegistration(parties)
		require.NoError(t, err)
		return reg
	}
	nextAVK, err := registration(s.stub.next).AggregateVerificationKey()
	require.NoError(t, err)
	wantMessage := mithril.ProtocolMessage{MessageParts: map[string]string{
		"next_aggregate_verification_key": nextAVK,
		"next_protocol_parameters":        testNextParams.ComputeHash(),
		"current_epoch":                   strconv.Itoa(testEpoch),
	}}.ComputeHash()
	wantSignature, err := s.stmKey.Sign(
		[]byte(wantMessage), registration(s.stub.current), testParams,
	)
	require.NoError(t, err)
	wantEncoded, err := wantSignature.Encode()
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"entity_type": map[string]any{
			"MithrilStakeDistribution": float64(testEpoch),
		},
		"party_id":       s.PartyID(),
		"signature":      wantEncoded,
		"indexes":        []any{0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0},
		"signed_message": wantMessage,
	}, got)
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.metrics.signatures) == 1
	}, testWait, time.Millisecond)

	// The epoch is done: no second registration or signature follows.
	testutil.RequireNoReceive(
		t,
		s.stub.signatures,
		50*time.Millisecond,
		"second signature",
	)
	assert.Len(t, s.stub.registrations, 1)
}

func TestRoundSkipsSigningWhenNotInCurrentSigners(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: false})

	require.NoError(t, s.round(t.Context()))
	require.Len(t, s.stub.registrations, 1)
	assert.Empty(t, s.stub.signatures)
	registeredHits := s.stub.registeredHits.Load()

	// Registration and the decision not to sign are both final for the
	// epoch, so a later round only reads the epoch settings.
	require.NoError(t, s.round(t.Context()))
	assert.Len(t, s.stub.registrations, 1)
	assert.Equal(t, registeredHits, s.stub.registeredHits.Load())
	assert.Equal(t, int32(2), s.stub.epochSettingsHits.Load())
}

func TestRunRetriesFailedRoundsWithBackoff(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: true})
	s.stub.failEpochSettings.Store(3)
	s.start(t)

	testutil.RequireReceive(
		t,
		s.stub.signatures,
		testWait,
		"signature after retries",
	)
	assert.Equal(t, float64(3), promtestutil.ToFloat64(s.metrics.errors))
	assert.GreaterOrEqual(
		t,
		promtestutil.ToFloat64(s.metrics.rounds),
		float64(4),
	)
}

func TestRunDoublesTheWaitBetweenFailedRounds(t *testing.T) {
	t.Parallel()
	const minBackoff = 20 * time.Millisecond
	s := newTestSigner(t, testOptions{
		ownIsCurrent: true,
		minBackoff:   minBackoff,
		maxBackoff:   4 * minBackoff,
	})
	s.stub.failEpochSettings.Store(4)
	s.start(t)
	testutil.RequireReceive(t, s.stub.signatures, testWait, "signature")

	// A timer never fires early, so each gap is at least the wait that
	// preceded it: 1x, 2x, then 4x twice because of the cap. Only lower
	// bounds are asserted; a loaded machine can lengthen a gap but not
	// shorten it.
	s.stub.hitsMu.Lock()
	hits := slices.Clone(s.stub.epochSettingsAt)
	s.stub.hitsMu.Unlock()
	require.GreaterOrEqual(t, len(hits), 5)
	for i, want := range []time.Duration{1, 2, 4, 4} {
		assert.GreaterOrEqual(
			t,
			hits[i+1].Sub(hits[i]),
			want*minBackoff,
			"wait after failed round %d",
			i+1,
		)
	}
}

func TestRunRetriesSignatureUntilRoundOpens(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: true})
	s.stub.signatureStatuses <- http.StatusNotFound
	s.start(t)

	testutil.RequireReceive(t, s.stub.signatures, testWait, "first signature")
	testutil.RequireReceive(t, s.stub.signatures, testWait, "retried signature")
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.metrics.signatures) == 1
	}, testWait, time.Millisecond)
	// A round that is not open yet is waiting, not failing.
	assert.Zero(t, promtestutil.ToFloat64(s.metrics.errors))
}

func TestRunStopsRetryingLateSignature(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: true})
	s.stub.signatureStatuses <- http.StatusGone
	s.start(t)

	testutil.RequireReceive(t, s.stub.signatures, testWait, "signature")
	testutil.RequireNoReceive(
		t,
		s.stub.signatures,
		50*time.Millisecond,
		"retry after gone",
	)
	assert.Zero(t, promtestutil.ToFloat64(s.metrics.signatures))
	assert.Zero(t, promtestutil.ToFloat64(s.metrics.errors))
}

func TestRunReturnsWhenCancelledBeforeFirstRound(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{ownIsCurrent: true})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, s.Run(ctx))
}

func TestNewRejectsInvalidCredentials(t *testing.T) {
	t.Parallel()
	slotInPeriod := func(period uint64) func() (uint64, error) {
		return func() (uint64, error) {
			return period * testSlotsPerKESPeriod, nil
		}
	}
	tests := []struct {
		name    string
		slot    func() (uint64, error)
		ledger  forging.LedgerView
		modify  func(t *testing.T, keys poolKeys)
		wantErr string
	}{
		{
			name:    "certificate expired",
			slot:    slotInPeriod(testOpCertStartPeriod + 62),
			wantErr: "expired",
		},
		{
			name:    "certificate not yet valid",
			slot:    slotInPeriod(testOpCertStartPeriod - 1),
			wantErr: "in the future",
		},
		{
			name:    "counter behind the chain",
			ledger:  fakeLedger{registered: true, opCertSequence: 5},
			wantErr: "opcert sequence 0 invalid",
		},
		{
			name: "cold key of another pool",
			modify: func(t *testing.T, keys poolKeys) {
				envelope, err := json.Marshal(bursa.KeyFile{
					Type:    "StakePoolVerificationKey_ed25519",
					CborHex: "5820" + strings.Repeat("11", 32),
				})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(keys.coldVKey, envelope, 0o600))
			},
			wantErr: "cold verification key does not match",
		},
		{
			name: "STM key file is not a key",
			modify: func(t *testing.T, keys poolKeys) {
				require.NoError(
					t,
					os.WriteFile(keys.stmKey, []byte("zz"), 0o600),
				)
			},
			wantErr: "decode STM key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			keys := newPoolKeys(t, testOpCertStartPeriod)
			if tt.modify != nil {
				tt.modify(t, keys)
			}
			slot, ledger := tt.slot, tt.ledger
			if slot == nil {
				slot = func() (uint64, error) { return testSlot, nil }
			}
			if ledger == nil {
				ledger = fakeLedger{}
			}
			_, err := New(Config{
				KESKeyPath:          keys.kes,
				OperationalCertPath: keys.opCert,
				ColdVKeyPath:        keys.coldVKey,
				STMKeyPath:          keys.stmKey,
				Genesis:             testGenesis(),
				Client: mithril.NewClient(
					"https://example.invalid",
				),
				Slot:   slot,
				Ledger: ledger,
			})
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRegisterRejectsCertificateThatExpiredAfterLoading(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	s := newTestSigner(t, testOptions{
		ownIsCurrent: true,
		slot: func() (uint64, error) {
			// Valid when the signer loads, past the lifetime when it
			// registers.
			if calls.Add(1) == 1 {
				return testSlot, nil
			}
			return (testOpCertStartPeriod + 62) * testSlotsPerKESPeriod, nil
		},
	})

	err := s.register(t.Context(), testEpoch)
	require.ErrorContains(t, err, "expired")
	assert.Empty(t, s.stub.registrations)
}

func TestNewDefersKESPeriodCheckWhenSlotUnavailable(t *testing.T) {
	t.Parallel()
	s := newTestSigner(t, testOptions{
		slot: func() (uint64, error) { return 0, ErrSlotUnavailable },
	})
	require.NotNil(t, s)
	// A registration still needs the slot and fails without it.
	require.ErrorIs(t, s.register(t.Context(), testEpoch), ErrSlotUnavailable)
}

func TestNewRequiresDependencies(t *testing.T) {
	t.Parallel()
	_, err := New(Config{})
	require.Error(t, err)
}

func TestSTMKeyIsCreatedOnceAndReused(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "stm.key")
	created, err := loadOrCreateSTMKey(path)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	if runtime.GOOS != "windows" {
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	loaded, err := loadOrCreateSTMKey(path)
	require.NoError(t, err)
	assert.Equal(t, created.Bytes(), loaded.Bytes())

	require.NoError(t, os.WriteFile(path, []byte("not hex"), 0o600))
	_, err = loadOrCreateSTMKey(path)
	require.ErrorContains(t, err, "decode STM key")
}

func TestSTMKeyRejectsBroadPermissions(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits do not apply")
	}
	path := filepath.Join(t.TempDir(), "stm.key")
	_, err := loadOrCreateSTMKey(path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o644))
	_, err = loadOrCreateSTMKey(path)
	require.Error(t, err)
}
