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

package mithril

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var lotteryParams = ProtocolParameters{K: 5, M: 40, PhiF: 0.5}

const testAggregatorOperatorToken = "aggregator-test-operator-token-0123456789"

// aggregatorFixture is an aggregator and artifact server over one store,
// reachable over HTTP. Snapshots are produced into the same store.
type aggregatorFixture struct {
	t             *testing.T
	store         ArtifactStore
	dir           string
	aggregator    *Aggregator
	url           string
	client        *Client
	genesisPub    ed25519.PublicKey
	genesisKey    ed25519.PrivateKey
	ancPub        ed25519.PublicKey
	ancKey        ed25519.PrivateKey
	params        ProtocolParameters
	epoch         uint64
	network       string
	operatorToken string
	created       int
}

func newAggregatorFixture(
	t *testing.T,
	params ProtocolParameters,
) *aggregatorFixture {
	t.Helper()
	genesisPub, genesisKey := newSigningKey(t)
	ancPub, ancKey := newSigningKey(t)
	store, dir := newLocalStore(t)
	f := &aggregatorFixture{
		t:             t,
		store:         store,
		dir:           dir,
		genesisPub:    genesisPub,
		genesisKey:    genesisKey,
		ancPub:        ancPub,
		ancKey:        ancKey,
		params:        params,
		epoch:         10,
		network:       "preprod",
		operatorToken: testAggregatorOperatorToken,
	}
	f.start()
	return f
}

// start builds the aggregator and server over the fixture's store; calling it
// again simulates a restart.
func (f *aggregatorFixture) start() {
	agg, err := NewAggregator(context.Background(), AggregatorConfig{
		Network:           f.network,
		Epoch:             f.epoch,
		Parameters:        f.params,
		GenesisSigningKey: f.genesisKey,
		Store:             f.store,
		OperatorToken:     f.operatorToken,
	})
	require.NoError(f.t, err)
	f.aggregator = agg
	srv := newMithrilTestServer(f.t, ServerConfig{
		Store:      f.store,
		Aggregator: agg,
		Logger:     slog.New(slog.DiscardHandler),
	})
	f.url = srv.URL
	f.client = NewClient(srv.URL, WithAllowInsecureHTTP())
}

// newSnapshot produces a snapshot of a database with the given number of
// immutable trios, created after every earlier one.
func (f *aggregatorFixture) newSnapshot(trios int) *CardanoDatabaseSnapshot {
	return f.snapshotOf(newCardanoDB(f.t, trios))
}

func (f *aggregatorFixture) snapshotOf(db string) *CardanoDatabaseSnapshot {
	f.t.Helper()
	cfg := newSnapshotConfig(f.t, db, f.store, f.ancKey)
	cfg.CreatedAt = snapshotCreatedAt.Add(
		time.Duration(f.created) * time.Hour,
	)
	f.created++
	snap, err := CreateSnapshot(context.Background(), cfg)
	require.NoError(f.t, err)
	return snap
}

func (f *aggregatorFixture) post(path string, body any) (int, string) {
	return f.postWithOperatorToken(path, body, f.operatorToken)
}

func (f *aggregatorFixture) postUnauthenticated(
	path string,
	body any,
) (int, string) {
	return f.postWithOperatorToken(path, body, "")
}

func (f *aggregatorFixture) postWithOperatorToken(
	path string,
	body any,
	token string,
) (int, string) {
	f.t.Helper()
	data, err := json.Marshal(body)
	require.NoError(f.t, err)
	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, f.url+path,
		bytes.NewReader(data),
	)
	require.NoError(f.t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(f.t, err)
	t := f.t
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp.StatusCode, string(getBody(t, resp))
}

func (f *aggregatorFixture) closeRegistrations() (int, string) {
	f.t.Helper()
	return f.post("/close-registrations", nil)
}

func (f *aggregatorFixture) register(s *testSTMSigner) (int, string) {
	return f.registerWithToken(s, f.operatorToken)
}

func (f *aggregatorFixture) registerUnauthenticated(
	s *testSTMSigner,
) (int, string) {
	return f.registerWithToken(s, "")
}

func (f *aggregatorFixture) registerWithToken(
	s *testSTMSigner,
	token string,
) (int, string) {
	reg := s.registration()
	return f.postWithOperatorToken("/register-signer", registerSignerRequest{
		Epoch:   f.epoch,
		PartyID: reg.PartyID,
		VerificationKey: hex.EncodeToString(
			append(bytes.Clone(reg.VerificationKey), reg.ProofOfPossession...),
		),
		Stake: reg.Stake,
	}, token)
}

func (f *aggregatorFixture) registerAll(signers []*testSTMSigner) {
	f.t.Helper()
	for _, s := range signers {
		code, body := f.register(s)
		require.Equal(f.t, http.StatusCreated, code, body)
	}
}

func (f *aggregatorFixture) pending() (int, *pendingCertificate) {
	f.t.Helper()
	f.aggregator.mu.Lock()
	needsClose := f.aggregator.sealed == nil && len(f.aggregator.regs) > 0
	f.aggregator.mu.Unlock()
	if needsClose {
		code, body := f.closeRegistrations()
		if code != http.StatusNoContent {
			f.t.Fatalf("close registrations: status %d: %s", code, body)
		}
	}
	resp := get(f.t, f.url+"/certificate-pending")
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	var pc pendingCertificate
	require.NoError(f.t, json.Unmarshal(getBody(f.t, resp), &pc))
	return resp.StatusCode, &pc
}

// signature builds a signer's signature for the pending message. ok is false
// when the signer wins no lottery index.
func (f *aggregatorFixture) signature(
	s *testSTMSigner,
	pc *pendingCertificate,
) (registerSignatureRequest, bool) {
	f.t.Helper()
	avk, err := parseSTMAggregateVerificationKey(pc.AggregateVerificationKey)
	require.NoError(f.t, err)
	for idx, signer := range pc.Signers {
		if signer.PartyID != s.partyID {
			continue
		}
		sig, ok := s.singleSignature(
			[]byte(pc.SignedMessage), avk, pc.ProtocolParameters,
			uint64(idx), //nolint:gosec // slice position
		)
		return registerSignatureRequest{
			PartyID:       s.partyID,
			SignedMessage: pc.SignedMessage,
			Signature:     hex.EncodeToString(encodeSTMSingleSignature(sig)),
		}, ok
	}
	f.t.Fatalf("signer %s is not in the pending message", s.partyID)
	return registerSignatureRequest{}, false
}

func (f *aggregatorFixture) certificateHash(hash string) string {
	f.t.Helper()
	resp := get(f.t, f.url+"/artifact/cardano-database/"+hash)
	require.Equal(f.t, http.StatusOK, resp.StatusCode)
	var snap CardanoDatabaseSnapshot
	require.NoError(f.t, json.Unmarshal(getBody(f.t, resp), &snap))
	return snap.CertificateHash
}

// signUntilCertified submits signatures one signer at a time until the
// snapshot carries a certificate, and returns that certificate hash.
func (f *aggregatorFixture) signUntilCertified(
	hash string,
	signers []*testSTMSigner,
) string {
	f.t.Helper()
	status, pc := f.pending()
	require.Equal(f.t, http.StatusOK, status)
	for _, s := range signers {
		req, ok := f.signature(s, pc)
		if !ok {
			continue
		}
		code, body := f.post("/register-signatures", req)
		require.Equal(f.t, http.StatusCreated, code, body)
		if cert := f.certificateHash(hash); cert != "" {
			return cert
		}
	}
	f.t.Fatal("quorum was not reached by the available signers")
	return ""
}

func (f *aggregatorFixture) verifyChain(
	certHash string,
) *CertificateChainVerificationResult {
	f.t.Helper()
	result, err := VerifyCertificateChainWithMode(
		context.Background(), f.client, certHash, "", VerificationModeSTM,
	)
	require.NoError(f.t, err)
	require.NoError(f.t, VerifyGenesisCertificateSignature(
		result.GenesisCertificate, hex.EncodeToString(f.genesisPub),
	))
	return result
}

func testSigners(n int, stake uint64) []*testSTMSigner {
	out := make([]*testSTMSigner, n)
	for i := range out {
		out[i] = newTestSTMSigner(i+1, stake)
	}
	return out
}

func TestAggregatorCertifiesSnapshotWithVerifiableChain(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	snap := f.newSnapshot(2)

	status, _ := f.pending()
	require.Equal(t, http.StatusNoContent, status, "no registrations yet")
	f.registerAll(signers)

	certHash := f.signUntilCertified(snap.Hash, signers)

	result := f.verifyChain(certHash)
	require.Len(t, result.Certificates, 2, "snapshot certificate and genesis")
	leaf := result.LeafCertificate
	require.Equal(t, certHash, leaf.Hash)
	require.Equal(t, f.epoch, leaf.Epoch)
	require.Equal(
		t, snap.MerkleRoot,
		leaf.ProtocolMessage.MessageParts["cardano_database_merkle_root"],
	)
	require.Equal(t, f.network, leaf.Metadata.Network)
	require.Equal(t, f.params, leaf.Metadata.Parameters)
	require.Len(t, leaf.Metadata.Signers, len(signers))
	beacon := leaf.SignedEntityType.CardanoDatabase()
	require.NotNil(t, beacon)
	require.Equal(t, snap.Beacon, *beacon)
	require.Equal(t, f.epoch-1, result.GenesisCertificate.Epoch)

	// Once certified there is nothing left to sign.
	status, _ = f.pending()
	require.Equal(t, http.StatusNoContent, status)
}

func TestAggregatorChainsSuccessiveCertificates(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	first := f.newSnapshot(2)
	firstHash := f.signUntilCertified(first.Hash, signers)
	second := f.newSnapshot(3)
	secondHash := f.signUntilCertified(second.Hash, signers)

	result := f.verifyChain(secondHash)
	require.Len(t, result.Certificates, 3)
	require.Equal(t, firstHash, result.Certificates[1].Hash)
	require.NotEqual(t, firstHash, secondHash)
}

func TestAggregatorSignsOldestPendingSnapshotFirst(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	older := f.newSnapshot(2)
	newer := f.newSnapshot(3)

	_, pc := f.pending()
	require.NotNil(t, pc)
	require.Contains(
		t, string(pc.SignedEntityType),
		`"immutable_file_number":1`,
	)
	f.signUntilCertified(older.Hash, signers)
	_, pc = f.pending()
	require.NotNil(t, pc)
	require.Contains(
		t, string(pc.SignedEntityType),
		`"immutable_file_number":2`,
	)
	f.signUntilCertified(newer.Hash, signers)
}

func TestAggregatorDoesNotCertifyBelowQuorum(t *testing.T) {
	t.Parallel()
	// Needing more indices than one signer can ever win keeps a lone signer
	// below quorum.
	f := newAggregatorFixture(t, ProtocolParameters{K: 40, M: 40, PhiF: 0.5})
	signers := testSigners(3, 100)
	snap := f.newSnapshot(2)
	f.registerAll(signers)
	_, pc := f.pending()
	req, ok := f.signature(signers[0], pc)
	require.True(t, ok)
	code, body := f.post("/register-signatures", req)
	require.Equal(t, http.StatusCreated, code, body)

	require.Empty(t, f.certificateHash(snap.Hash))
}

func TestAggregatorRegistrationValidation(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	a, b := newTestSTMSigner(1, 100), newTestSTMSigner(2, 100)
	good := a.registration()
	goodKey := hex.EncodeToString(
		append(bytes.Clone(good.VerificationKey), good.ProofOfPossession...),
	)
	forgedKey := hex.EncodeToString(
		append(bytes.Clone(good.VerificationKey), b.proofOfPossession()...),
	)

	cases := map[string]registerSignerRequest{
		"wrong epoch": {
			Epoch:           f.epoch + 1,
			PartyID:         "p",
			VerificationKey: goodKey,
			Stake:           1,
		},
		"zero stake": {
			Epoch:           f.epoch,
			PartyID:         "p",
			VerificationKey: goodKey,
			Stake:           0,
		},
		"empty party": {
			Epoch:           f.epoch,
			PartyID:         "",
			VerificationKey: goodKey,
			Stake:           1,
		},
		"unsafe party": {
			Epoch:           f.epoch,
			PartyID:         "../x",
			VerificationKey: goodKey,
			Stake:           1,
		},
		"not hex": {
			Epoch:           f.epoch,
			PartyID:         "p",
			VerificationKey: "zz",
			Stake:           1,
		},
		"short key": {
			Epoch:           f.epoch,
			PartyID:         "p",
			VerificationKey: goodKey[:100],
			Stake:           1,
		},
		"foreign possession": {
			Epoch:           f.epoch,
			PartyID:         "p",
			VerificationKey: forgedKey,
			Stake:           1,
		},
	}
	for name, req := range cases {
		code, body := f.post("/register-signer", req)
		require.Equal(t, http.StatusBadRequest, code, "%s: %s", name, body)
	}
	// A maximal registration fits, and leaves no room for any other stake.
	code, body := f.post("/register-signer", registerSignerRequest{
		Epoch: f.epoch, PartyID: "p", VerificationKey: goodKey,
		Stake: ^uint64(0),
	})
	require.Equal(t, http.StatusCreated, code, body)
	code, body = f.post("/register-signer", registerSignerRequest{
		Epoch: f.epoch, PartyID: "q",
		VerificationKey: hex.EncodeToString(append(
			bytes.Clone(b.verificationKey()), b.proofOfPossession()...,
		)),
		Stake: 1,
	})
	require.Equal(t, http.StatusBadRequest, code, body)

	code, _ = f.post("/register-signer", "not an object")
	require.Equal(t, http.StatusBadRequest, code)
}

func TestAggregatorReRegistrationRequiresOperator(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	s := newTestSTMSigner(1, 100)
	code, body := f.register(s)
	require.Equal(t, http.StatusCreated, code, body)
	s.stake = 250
	code, body = f.registerUnauthenticated(s)
	require.Equal(t, http.StatusUnauthorized, code, body)
	code, body = f.registerWithToken(s, f.operatorToken+"wrong")
	require.Equal(t, http.StatusUnauthorized, code, body)
	code, body = f.closeRegistrations()
	require.Equal(t, http.StatusNoContent, code, body)
	status, pc := f.pending()
	require.Equal(t, http.StatusOK, status)
	require.NotNil(t, pc)
	require.Len(t, pc.Signers, 1)
	require.Equal(t, uint64(100), pc.Signers[0].Stake)
}

func TestAggregatorOperatorCanUpdateRegistration(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	s := newTestSTMSigner(1, 100)
	code, body := f.register(s)
	require.Equal(t, http.StatusCreated, code, body)
	s.stake = 250
	code, body = f.register(s)
	require.Equal(t, http.StatusCreated, code, body)

	code, body = f.closeRegistrations()
	require.Equal(t, http.StatusNoContent, code, body)
	_, pc := f.pending()
	require.NotNil(t, pc)
	require.Len(t, pc.Signers, 1)
	require.Equal(t, uint64(250), pc.Signers[0].Stake)
}

func TestAggregatorPartyCannotReplaceVerificationKey(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	original := newTestSTMSigner(1, 100)
	code, body := f.register(original)
	require.Equal(t, http.StatusCreated, code, body)

	replacement := newTestSTMSigner(2, 250)
	replacement.partyID = original.partyID
	code, body = f.register(replacement)
	require.Equal(t, http.StatusConflict, code, body)

	code, body = f.closeRegistrations()
	require.Equal(t, http.StatusNoContent, code, body)
	_, pending := f.pending()
	require.NotNil(t, pending)
	require.Len(t, pending.Signers, 1)
	key, err := hex.DecodeString(pending.Signers[0].VerificationKey)
	require.NoError(t, err)
	require.Equal(t, original.verificationKey(), key)
}

func TestAggregatorPendingReadDoesNotCloseRegistration(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	code, body := f.register(newTestSTMSigner(1, 100))
	require.Equal(t, http.StatusCreated, code, body)

	resp := get(t, f.url+"/certificate-pending")
	// The former behavior returned an opened certificate (200); the assertion
	// below pins that reading it must not freeze the signer set.
	require.Contains(t, []int{http.StatusNoContent, http.StatusOK}, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	code, body = f.register(newTestSTMSigner(2, 100))
	require.Equal(t, http.StatusCreated, code, body,
		"pending read must leave registration open")
}

func TestAggregatorInvalidSignatureDoesNotCloseRegistration(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	code, body := f.register(newTestSTMSigner(1, 100))
	require.Equal(t, http.StatusCreated, code, body)
	code, body = f.postUnauthenticated(
		"/register-signatures", registerSignatureRequest{},
	)
	require.Equal(t, http.StatusConflict, code, body)

	code, body = f.register(newTestSTMSigner(2, 100))
	require.Equal(t, http.StatusCreated, code, body,
		"invalid signature request must leave registration open")
}

func TestAggregatorRegistrationClosureRequiresOperator(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	code, body := f.register(newTestSTMSigner(1, 100))
	require.Equal(t, http.StatusCreated, code, body)
	code, body = f.postUnauthenticated("/close-registrations", nil)
	require.Equal(t, http.StatusUnauthorized, code, body)
	code, body = f.register(newTestSTMSigner(2, 100))
	require.Equal(t, http.StatusCreated, code, body,
		"unauthorized closure must leave registration open")
	code, body = f.closeRegistrations()
	require.Equal(t, http.StatusNoContent, code, body)

	status, pending := f.pending()
	require.Equal(t, http.StatusOK, status)
	require.Len(t, pending.Signers, 2)
}

func TestAggregatorRefusesRegistrationOnceClosed(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	code, body := f.register(newTestSTMSigner(1, 100))
	require.Equal(t, http.StatusCreated, code, body)
	status, _ := f.pending() // closes registration
	require.Equal(t, http.StatusOK, status)

	code, body = f.register(newTestSTMSigner(2, 100))
	require.Equal(t, http.StatusConflict, code, body)
}

func postCode(f *aggregatorFixture, req registerSignatureRequest) int {
	f.t.Helper()
	code, _ := f.post("/register-signatures", req)
	return code
}

func TestAggregatorSignatureValidation(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, ProtocolParameters{K: 40, M: 40, PhiF: 0.5})
	signers := testSigners(2, 100)
	outsider := newTestSTMSigner(9, 100)
	snap := f.newSnapshot(2)

	code, body := f.post(
		"/register-signatures", registerSignatureRequest{PartyID: "x"},
	)
	require.Equal(t, http.StatusConflict, code, "nothing open yet: %s", body)

	f.registerAll(signers)
	_, pc := f.pending()
	good, ok := f.signature(signers[0], pc)
	require.True(t, ok)

	mutate := func(fn func(*registerSignatureRequest)) registerSignatureRequest {
		r := good
		fn(&r)
		return r
	}
	staleMessage := mutate(func(r *registerSignatureRequest) {
		r.SignedMessage = "00"
	})
	require.Equal(
		t, http.StatusConflict, postCode(f, staleMessage), "stale message",
	)

	unknown := mutate(func(r *registerSignatureRequest) {
		r.PartyID = outsider.partyID
	})
	require.Equal(
		t, http.StatusBadRequest, postCode(f, unknown), "unregistered party",
	)

	notHex := mutate(func(r *registerSignatureRequest) { r.Signature = "zz" })
	require.Equal(t, http.StatusBadRequest, postCode(f, notHex), "not hex")

	truncated := mutate(func(r *registerSignatureRequest) {
		r.Signature = good.Signature[:20]
	})
	require.Equal(
		t, http.StatusBadRequest, postCode(f, truncated), "truncated",
	)

	// A valid signature presented as another registered party fails the
	// signer-index and key checks.
	other, ok := f.signature(signers[1], pc)
	require.True(t, ok)
	swapped := mutate(func(r *registerSignatureRequest) {
		r.Signature = other.Signature
	})
	require.Equal(
		t, http.StatusBadRequest, postCode(f, swapped),
		"signature of another party",
	)

	// A valid signature that names the wrong signer index would be filed
	// under another party's registration in the aggregate.
	decoded, err := hex.DecodeString(good.Signature)
	require.NoError(t, err)
	claimed, err := parseSTMSingleSignatureBytes(decoded, stmMaxLotteryIndices)
	require.NoError(t, err)
	claimed.SignerIndex++
	misindexed := mutate(func(r *registerSignatureRequest) {
		r.Signature = hex.EncodeToString(encodeSTMSingleSignature(*claimed))
	})
	require.Equal(
		t, http.StatusBadRequest, postCode(f, misindexed), "wrong signer index",
	)

	// Trailing bytes after a well-formed signature are refused.
	trailing := mutate(func(r *registerSignatureRequest) {
		r.Signature += "00"
	})
	require.Equal(
		t, http.StatusBadRequest, postCode(f, trailing), "trailing bytes",
	)

	// A signature over a different message is refused.
	avk, err := parseSTMAggregateVerificationKey(pc.AggregateVerificationKey)
	require.NoError(t, err)
	wrongMsg, ok := signers[0].singleSignature(
		[]byte("other"), avk, pc.ProtocolParameters, 0,
	)
	require.True(t, ok)
	forged := mutate(func(r *registerSignatureRequest) {
		r.Signature = hex.EncodeToString(encodeSTMSingleSignature(wrongMsg))
	})
	require.Equal(
		t, http.StatusBadRequest, postCode(f, forged), "wrong message",
	)

	require.Equal(t, http.StatusCreated, postCode(f, good))
	require.Empty(
		t, f.certificateHash(snap.Hash), "rejected signatures must not certify",
	)
}

func TestAggregatorSurvivesRestart(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	first := f.newSnapshot(2)
	firstHash := f.signUntilCertified(first.Hash, signers)

	f.start()
	second := f.newSnapshot(3)
	secondHash := f.signUntilCertified(second.Hash, signers)

	result := f.verifyChain(secondHash)
	require.Len(t, result.Certificates, 3)
	require.Equal(t, firstHash, result.Certificates[1].Hash,
		"the restarted aggregator must chain to its last certificate")
}

func TestAggregatorKeepsClosedRegistrationAcrossRestart(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	snap := f.newSnapshot(2)
	status, before := f.pending() // closes registration
	require.Equal(t, http.StatusOK, status)

	f.start()
	status, after := f.pending()
	require.Equal(t, http.StatusOK, status,
		"the closed signer set must survive a restart")
	require.Equal(
		t, before.AggregateVerificationKey, after.AggregateVerificationKey,
	)

	certHash := f.signUntilCertified(snap.Hash, signers)
	require.Len(t, f.verifyChain(certHash).Certificates, 2,
		"the restart must not issue a second genesis certificate")
}

func TestNewAggregatorRejectsChangedParametersAfterClose(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.newSnapshot(2)
	code, body := f.register(newTestSTMSigner(1, 100))
	require.Equal(t, http.StatusCreated, code, body)
	status, _ := f.pending()
	require.Equal(t, http.StatusOK, status)

	_, err := NewAggregator(context.Background(), AggregatorConfig{
		Network:           f.network,
		Epoch:             f.epoch,
		Parameters:        ProtocolParameters{K: 1, M: 2, PhiF: 1},
		GenesisSigningKey: f.genesisKey,
		Store:             f.store,
		OperatorToken:     f.operatorToken,
	})
	require.ErrorContains(t, err, "parameters")
}

func TestNewAggregatorValidatesConfig(t *testing.T) {
	t.Parallel()
	_, priv := newSigningKey(t)
	newBase := func() AggregatorConfig {
		store, _ := newLocalStore(t)
		return AggregatorConfig{
			Network:           "preview",
			Epoch:             5,
			Parameters:        lotteryParams,
			GenesisSigningKey: priv,
			Store:             store,
			OperatorToken:     testAggregatorOperatorToken,
		}
	}
	_, err := NewAggregator(context.Background(), newBase())
	require.NoError(t, err)

	for name, mutate := range map[string]func(*AggregatorConfig){
		"epoch zero":  func(c *AggregatorConfig) { c.Epoch = 0 },
		"no key":      func(c *AggregatorConfig) { c.GenesisSigningKey = nil },
		"bad network": func(c *AggregatorConfig) { c.Network = "../x" },
		"zero k":      func(c *AggregatorConfig) { c.Parameters.K = 0 },
		"zero m":      func(c *AggregatorConfig) { c.Parameters.M = 0 },
		"k greater than m": func(c *AggregatorConfig) {
			c.Parameters.K = c.Parameters.M + 1
		},
		"bad phi":  func(c *AggregatorConfig) { c.Parameters.PhiF = 1.5 },
		"no store": func(c *AggregatorConfig) { c.Store = nil },
		"short operator token": func(c *AggregatorConfig) {
			c.OperatorToken = "short"
		},
	} {
		cfg := newBase()
		mutate(&cfg)
		_, err := NewAggregator(context.Background(), cfg)
		require.Error(t, err, name)
	}
}

func TestAggregatorIgnoresSnapshotsOfOtherNetworks(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	f.network = "preview" // newSnapshotConfig produces preprod snapshots
	f.start()
	f.newSnapshot(2)
	code, body := f.register(newTestSTMSigner(1, 100))
	require.Equal(t, http.StatusCreated, code, body)

	resp := get(t, f.url+"/certificate-pending")
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	code, body = f.register(newTestSTMSigner(2, 100))
	require.Equal(t, http.StatusCreated, code,
		"nothing was opened, so registration must still be open: %s", body)
}

func TestAggregatorServesTheCertifiedStakeDistribution(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(3, 100)
	f.registerAll(signers)
	snap := f.newSnapshot(2)

	list, err := f.client.ListMithrilStakeDistributions(context.Background())
	require.NoError(t, err)
	require.Empty(t, list, "registration is still open")

	certHash := f.signUntilCertified(snap.Hash, signers)
	genesis := f.verifyChain(certHash).GenesisCertificate

	list, err = f.client.ListMithrilStakeDistributions(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, genesis.Hash, list[0].CertificateHash)
	assert.Equal(t, genesis.Epoch, list[0].Epoch)

	dist, err := f.client.GetMithrilStakeDistribution(
		context.Background(), list[0].Hash,
	)
	require.NoError(t, err)
	assert.Equal(t, genesis.Epoch, dist.Epoch)
	require.Len(t, dist.Signers, len(signers))
	for _, party := range dist.Signers {
		_, err := party.VerificationKeyBytes()
		require.NoError(t, err)
	}

	resp := get(
		t,
		f.url+"/artifact/mithril-stake-distribution/"+strings.Repeat("0", 64),
	)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestAggregatorCertifiedSnapshotVerifiesAndSyncs runs an aggregator, signers
// and a syncing client against one server: the snapshot is certified by the
// signers, a bootstrap that verifies the certificate chain and the signed
// ancillary manifest accepts it, and Sync imports it.
func TestAggregatorCertifiedSnapshotVerifiesAndSyncs(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	snap := f.snapshotOf(newSyncableDB(t))
	signers := testSigners(4, 100)
	f.registerAll(signers)
	certHash := f.signUntilCertified(snap.Hash, signers)
	f.verifyChain(certHash)

	result, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:                  f.network,
		Backend:                  BackendV2,
		AggregatorURL:            f.url,
		AllowInsecureHTTP:        true,
		DownloadDir:              t.TempDir(),
		VerifyCertificateChain:   true,
		GenesisVerificationKey:   hex.EncodeToString(f.genesisPub),
		AncillaryVerificationKey: hex.EncodeToString(f.ancPub),
		Logger:                   slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	require.Equal(t, snap.Hash, result.Snapshot.Digest)
	require.Equal(t, certHash, result.Snapshot.CertificateHash)
	require.True(t, result.AncillaryVerified)

	synced, err := Sync(context.Background(), SyncConfig{
		Network:           f.network,
		DataDir:           t.TempDir(),
		StorageMode:       "core",
		Backend:           BackendV2,
		AggregatorURL:     f.url,
		AllowInsecureHTTP: true,
		VerifyCertChain:   false,
		Logger:            slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1000), synced.LedgerSlot)
}

func TestBootstrapRejectsSnapshotCertifiedByAnotherGenesisKey(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	snap := f.snapshotOf(newSyncableDB(t))
	signers := testSigners(4, 100)
	f.registerAll(signers)
	f.signUntilCertified(snap.Hash, signers)
	otherPub, _ := newSigningKey(t)

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:                  f.network,
		Backend:                  BackendV2,
		AggregatorURL:            f.url,
		AllowInsecureHTTP:        true,
		DownloadDir:              t.TempDir(),
		VerifyCertificateChain:   true,
		GenesisVerificationKey:   hex.EncodeToString(otherPub),
		AncillaryVerificationKey: hex.EncodeToString(f.ancPub),
		Logger:                   slog.New(slog.DiscardHandler),
	})
	require.ErrorContains(t, err, "genesis certificate verification failed")
}

// A verifying client takes the newest listed snapshot, so an aggregator lists
// only snapshots that already carry a certificate.
func TestAggregatorListsOnlyCertifiedSnapshots(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	certified := f.newSnapshot(2)
	certHash := f.signUntilCertified(certified.Hash, signers)
	f.newSnapshot(3)

	items, err := f.client.ListCardanoDatabaseSnapshots(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, certified.Hash, items[0].Hash)
	require.Equal(t, certHash, items[0].CertificateHash)
}

// Producing a snapshot again from an unchanged database yields the same hash;
// the certificate it already carries must survive.
func TestCreateSnapshotKeepsCertificateOfReproducedSnapshot(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	db := newCardanoDB(t, 2)
	snap := f.snapshotOf(db)
	certHash := f.signUntilCertified(snap.Hash, signers)

	again := f.snapshotOf(db)
	require.Equal(t, snap.Hash, again.Hash)
	require.Equal(t, certHash, f.certificateHash(snap.Hash))
	status, _ := f.pending()
	require.Equal(t, http.StatusNoContent, status)
}

// A snapshot pruned while it is open for signing is dropped from signing
// rather than certified, which would recreate its metadata without archives.
func TestAggregatorDropsSnapshotPrunedWhileOpen(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	older := f.newSnapshot(2)
	_, stale := f.pending()
	require.NotNil(t, stale)
	newer := f.newSnapshot(3)
	removed, err := PruneSnapshots(context.Background(), f.store, 1)
	require.NoError(t, err)
	require.Equal(t, []string{older.Hash}, removed)

	for _, s := range signers {
		if req, ok := f.signature(s, stale); ok {
			code, body := f.post("/register-signatures", req)
			require.Equal(t, http.StatusConflict, code, body)
		}
	}
	require.Equal(t, []string{newer.Hash}, snapshotHashes(t, f.store))
	_, pc := f.pending()
	require.NotNil(t, pc)
	require.Contains(
		t, string(pc.SignedEntityType), `"immutable_file_number":2`,
	)
	f.signUntilCertified(newer.Hash, signers)
}
