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
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

// budgetChain serves a structural certificate chain of length certs whose
// last certificate is genesis. It returns the leaf hash and the number of
// body bytes served so far.
func budgetChain(
	t *testing.T,
	length int,
	signers int,
) (*Client, string, *atomic.Int64) {
	t.Helper()
	certs := make([]Certificate, length)
	for i := length - 1; i >= 0; i-- {
		cert := Certificate{
			Epoch: 0,
			Metadata: CertificateMetadata{
				Network:     "preprod",
				Version:     "0.1.0",
				Parameters:  ProtocolParameters{K: 1, M: 2, PhiF: 0.5},
				InitiatedAt: "2026-02-10T00:00:00Z",
				SealedAt:    "2026-02-10T00:01:00Z",
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{"current_epoch": "0"},
			},
			AggregateVerificationKey: "avk",
			MultiSignature:           "sig",
		}
		for s := range signers {
			cert.Metadata.Signers = append(
				cert.Metadata.Signers,
				StakeDistributionParty{PartyID: fmt.Sprintf("p%d", s), Stake: 1},
			)
		}
		if i == length-1 {
			cert.MultiSignature = ""
			cert.GenesisSignature = "genesis_sig"
			finalizeTestCertificate(t, &cert)
			cert.PreviousHash = cert.Hash
		} else {
			cert.PreviousHash = certs[i+1].Hash
		}
		finalizeTestCertificate(t, &cert)
		certs[i] = cert
	}
	byHash := make(map[string][]byte, length)
	for _, c := range certs {
		body, err := json.Marshal(c)
		require.NoError(t, err)
		byHash[c.Hash] = body
	}
	served := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		body, ok := byHash[strings.TrimPrefix(r.URL.Path, "/certificate/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		served.Add(int64(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return NewClient(server.URL, WithAllowInsecureHTTP()), certs[0].Hash, served
}

func TestVerifyCertificateChainLengthCap(t *testing.T) {
	t.Parallel()

	const length = 12
	client, leaf, _ := budgetChain(t, length, 0)
	run := func(maxCertificates int) error {
		budget := newCertificateChainBudget()
		budget.maxCertificates = maxCertificates
		_, err := verifyCertificateChain(
			context.Background(), client, leaf, "",
			VerificationModeStructural, budget,
		)
		return err
	}

	require.NoError(t, run(length), "a chain exactly at the cap is accepted")
	err := run(length - 1)
	require.ErrorIs(t, err, errCertificateChainBudget)
	require.Contains(t, err.Error(), "maximum depth")
}

func TestVerifyCertificateChainCumulativeByteBudget(t *testing.T) {
	t.Parallel()

	client, leaf, served := budgetChain(t, 10, 0)
	run := func(maxBytes int64) error {
		budget := newCertificateChainBudget()
		budget.maxBytes = maxBytes
		_, err := verifyCertificateChain(
			context.Background(), client, leaf, "",
			VerificationModeStructural, budget,
		)
		return err
	}

	require.NoError(t, run(maxCertificateChainBytes))
	total := served.Swap(0)
	require.Greater(t, total, int64(0))

	require.NoError(t, run(total), "exactly the chain's size is accepted")
	served.Store(0)
	// Every certificate is far below the per-certificate cap; only the sum
	// crosses the budget.
	err := run(total - 1)
	require.ErrorIs(t, err, errCertificateChainBudget)
	require.Contains(t, err.Error(), "exceeds")
}

func TestVerifyCertificateChainSignerCountCap(t *testing.T) {
	t.Parallel()

	client, leaf, _ := budgetChain(t, 1, stmMaxSigners)
	_, err := VerifyCertificateChainWithMode(
		context.Background(), client, leaf, "", VerificationModeStructural,
	)
	require.NoError(t, err, "signers exactly at the cap are accepted")

	client, leaf, _ = budgetChain(t, 1, stmMaxSigners+1)
	_, err = VerifyCertificateChainWithMode(
		context.Background(), client, leaf, "", VerificationModeStructural,
	)
	require.ErrorIs(t, err, errCertificateChainBudget)
	require.Contains(t, err.Error(), "signers")
}

func TestCertificateMetadataSignerCountCapDuringDecode(t *testing.T) {
	t.Parallel()

	metadata := func(signers int) []byte {
		items := make([]string, signers)
		for i := range items {
			items[i] = `{}`
		}
		return []byte(`{"signers":[` + strings.Join(items, ",") + `]}`)
	}

	var got CertificateMetadata
	require.NoError(
		t,
		json.Unmarshal(metadata(stmMaxSigners), &got),
		"a signer array exactly at the cap is accepted",
	)
	require.Len(t, got.Signers, stmMaxSigners)

	err := json.Unmarshal(metadata(stmMaxSigners+1), &got)
	require.ErrorIs(t, err, errCertificateChainBudget)
	require.Contains(t, err.Error(), "certificate signers")
}

func TestSignedEntityTypeRejectsSecondKeyBeforeItsValue(t *testing.T) {
	t.Parallel()

	entity := SignedEntityType{
		raw: json.RawMessage(`{"CardanoDatabase":{},"oversized":`),
	}
	_, err := entity.Kind()
	require.ErrorContains(t, err, "exactly one key")
}

func TestGetCertificateBodyLimit(t *testing.T) {
	t.Parallel()

	serve := func(body string) *Client {
		server := httptest.NewServer(http.HandlerFunc(func(
			w http.ResponseWriter, _ *http.Request,
		) {
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(server.Close)
		return NewClient(server.URL, WithAllowInsecureHTTP())
	}
	body := func(n int) string {
		const frame = `{"hash":""}`
		return `{"hash":"` + strings.Repeat("a", n-len(frame)) + `"}`
	}

	_, size, err := serve(body(64)).getCertificateWithLimit(
		context.Background(), "h", 64,
	)
	require.NoError(t, err, "a body exactly at the limit is accepted")
	require.Equal(t, int64(64), size)

	_, _, err = serve(body(65)).getCertificateWithLimit(
		context.Background(), "h", 64,
	)
	require.ErrorIs(t, err, errCertificateChainBudget)

	_, err = serve(body(maxCertificateBytes+1)).GetCertificate(
		context.Background(), "h",
	)
	require.ErrorIs(t, err, errCertificateChainBudget)
}

// budgetSignature encodes a binary aggregate signature with the given
// signature count, lottery indices per signature and batch-proof values.
func budgetSignature(sigs, indicesPer, values int) string {
	u64 := func(v int) []byte {
		return binary.BigEndian.AppendUint64(nil, uint64(v))
	}
	out := append([]byte{0}, u64(sigs)...)
	for range sigs {
		single := u64(indicesPer)
		for i := range indicesPer {
			single = append(single, u64(i)...)
		}
		single = append(single, make([]byte, 48)...)
		single = append(single, u64(0)...)
		entry := append(u64(stmClosedRegistrationEntrySize),
			make([]byte, stmClosedRegistrationEntrySize)...)
		entry = append(entry, u64(len(single))...)
		entry = append(entry, single...)
		out = append(out, u64(len(entry))...)
		out = append(out, entry...)
	}
	out = append(out, u64(values)...)
	out = append(out, u64(0)...)
	out = append(out, make([]byte, values*32)...)
	return hex.EncodeToString(out)
}

func TestParseSTMAggregateSignatureCountCaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sigs    int
		indices int
		values  int
		wantErr string
	}{
		{"signers at cap", stmMaxSigners, 0, 0, ""},
		{"signers over cap", stmMaxSigners + 1, 0, 0, "signatures"},
		{"indices at cap", 1, stmMaxLotteryIndices, 0, ""},
		{"indices over cap", 1, stmMaxLotteryIndices + 1, 0, "lottery indices"},
		{"aggregate indices at cap", 2, stmMaxLotteryIndices / 2, 0, ""},
		{
			"aggregate indices over cap",
			2, stmMaxLotteryIndices/2 + 1, 0, "lottery indices",
		},
		{"path values at cap", 0, 0, stmMaxBatchPathValues, ""},
		{"path values over cap", 0, 0, stmMaxBatchPathValues + 1, "batch proof"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sig, err := parseSTMAggregateSignature(
				budgetSignature(tt.sigs, tt.indices, tt.values),
			)
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.NoError(t, checkSTMAggregateSignatureCounts(sig))
				return
			}
			require.ErrorIs(t, err, errCertificateChainBudget)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestVerifySTMSignatureRejectsCumulativeIndicesBeforeVerification(
	t *testing.T,
) {
	t.Parallel()

	// Each signature is within the per-signature bound; only the total is
	// over. The verification key bytes are zero, so any verification would
	// fail with a decode error instead of the budget error.
	perSig := stmMaxLotteryIndices / 2
	encoded := budgetSignature(3, perSig, 0)
	err := verifySTMSignature(
		make([]byte, 16),
		hex.EncodeToString([]byte(stmGoldenAggregateVerificationKeyJSON)),
		encoded,
		ProtocolParameters{K: 5, M: 10, PhiF: 0.8},
		newCertificateChainBudget(),
	)
	require.ErrorIs(t, err, errCertificateChainBudget)
	require.Contains(t, err.Error(), "lottery indices")
}

func TestVerifySTMSignatureWorkBudget(t *testing.T) {
	t.Parallel()

	msg := make([]byte, 16)
	avk := hex.EncodeToString([]byte(stmGoldenAggregateVerificationKeyJSON))
	sigHex := hex.EncodeToString([]byte(stmGoldenAggregateSignatureJSON))
	params := ProtocolParameters{K: 5, M: 10, PhiF: 0.8}
	parsed, err := parseSTMAggregateSignature(sigHex)
	require.NoError(t, err)
	work := stmAggregateSignatureWork(parsed)
	require.Greater(t, work, uint64(0))

	t.Run("cumulative across certificates", func(t *testing.T) {
		t.Parallel()
		budget := newCertificateChainBudget()
		budget.maxWork = 2 * work
		for range 2 {
			require.NoError(
				t, verifySTMSignature(msg, avk, sigHex, params, budget),
			)
		}
		err := verifySTMSignature(msg, avk, sigHex, params, budget)
		require.ErrorIs(t, err, errCertificateChainBudget)
		require.Contains(t, err.Error(), "work")
	})

	t.Run("rejected before verification", func(t *testing.T) {
		t.Parallel()
		budget := newCertificateChainBudget()
		budget.maxWork = work - 1
		// A wrong message would otherwise fail as "lottery lost".
		wrong := make([]byte, 16)
		wrong[0] = 1
		err := verifySTMSignature(wrong, avk, sigHex, params, budget)
		require.ErrorIs(t, err, errCertificateChainBudget)
		require.NotContains(t, err.Error(), "lottery lost")
	})
}

func TestParseSTMAggregateVerificationKeyRequiresFixedRoot(t *testing.T) {
	t.Parallel()

	jsonAVK := stmAggregateVerificationKey{
		MTCommitment: stmMerkleTreeBatchCommitment{
			Root:     make([]byte, blake2b.Size256+1),
			NrLeaves: 1,
		},
		TotalStake: 1,
	}
	jsonBytes, err := json.Marshal(jsonAVK)
	require.NoError(t, err)
	_, err = parseSTMAggregateVerificationKey(hex.EncodeToString(jsonBytes))
	require.ErrorContains(t, err, "invalid Merkle root length")

	binaryAVK := binary.BigEndian.AppendUint64(nil, 1)
	binaryAVK = append(binaryAVK, make([]byte, blake2b.Size256+1)...)
	binaryAVK = binary.BigEndian.AppendUint64(binaryAVK, 1)
	_, err = parseSTMAggregateVerificationKey(hex.EncodeToString(binaryAVK))
	require.ErrorContains(
		t,
		err,
		"invalid aggregate verification key payload length",
	)
}

func TestVerifyCertificateChainChargesSTMWorkToChainBudget(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile(
		filepath.Join("testdata", "v2", "cardano_database_certificate.json"),
	)
	require.NoError(t, err)
	var cert Certificate
	require.NoError(t, json.Unmarshal(body, &cert))
	require.NoError(t, verifySTMCertificate(&cert, newCertificateChainBudget()))
	parsed, err := parseSTMAggregateSignature(cert.MultiSignature)
	require.NoError(t, err)
	work := stmAggregateSignatureWork(parsed)

	server := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if strings.TrimPrefix(r.URL.Path, "/certificate/") != cert.Hash {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	client := NewClient(server.URL, WithAllowInsecureHTTP())
	run := func(spent uint64) error {
		budget := newCertificateChainBudget()
		budget.work = spent
		_, err := verifyCertificateChain(
			context.Background(), client, cert.Hash, "",
			VerificationModeSTM, budget,
		)
		return err
	}

	// The certificate verifies on its own, so the walk only stops at the
	// missing parent while the budget still covers this certificate.
	err = run(maxCertificateChainWork - work)
	require.Error(t, err)
	require.NotErrorIs(t, err, errCertificateChainBudget)
	require.Contains(t, err.Error(), cert.PreviousHash)

	// Work already charged by earlier certificates of the same walk leaves
	// too little for this one.
	err = run(maxCertificateChainWork - work + 1)
	require.ErrorIs(t, err, errCertificateChainBudget)
	require.Contains(t, err.Error(), "work")
}

// budgetJSONSignature rewrites the golden JSON aggregate signature so that
// it carries sigs copies of its first signature, indices lottery indices
// on that signature and values batch-proof values.
func budgetJSONSignature(t *testing.T, sigs, indices, values int) string {
	t.Helper()
	var agg map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(
		[]byte(stmGoldenAggregateSignatureJSON), &agg,
	))
	var entries [][]json.RawMessage
	require.NoError(t, json.Unmarshal(agg["signatures"], &entries))
	first := entries[0]
	if indices > 0 {
		var sig map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(first[0], &sig))
		idx := make([]int, indices)
		for i := range idx {
			idx[i] = i
		}
		sig["indexes"] = mustJSON(t, idx)
		first = []json.RawMessage{mustJSON(t, sig), first[1]}
	}
	out := make([][]json.RawMessage, sigs)
	for i := range out {
		out[i] = first
	}
	agg["signatures"] = mustJSON(t, out)
	if values > 0 {
		var proof map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(agg["batch_proof"], &proof))
		vals := make([][]int, values)
		for i := range vals {
			vals[i] = make([]int, 32)
		}
		proof["values"] = mustJSON(t, vals)
		agg["batch_proof"] = mustJSON(t, proof)
	}
	return hex.EncodeToString(mustJSON(t, agg))
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestVerifySTMSignatureRejectsOverCapJSONSignature(t *testing.T) {
	t.Parallel()

	msg := make([]byte, 16)
	avk := hex.EncodeToString([]byte(stmGoldenAggregateVerificationKeyJSON))
	params := ProtocolParameters{K: 5, M: 10, PhiF: 0.8}
	tests := []struct {
		name    string
		sigs    int
		indices int
		values  int
		wantErr string
	}{
		{"signers", stmMaxSigners + 1, 0, 0, "signatures"},
		{"lottery indices", 1, stmMaxLotteryIndices + 1, 0, "lottery indices"},
		{"batch proof values", 1, 0, stmMaxBatchPathValues + 1, "batch proof"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded := budgetJSONSignature(t, tt.sigs, tt.indices, tt.values)
			_, err := parseSTMAggregateSignature(encoded)
			require.ErrorIs(t, err, errCertificateChainBudget)
			require.Contains(t, err.Error(), tt.wantErr)

			err = verifySTMSignature(
				msg, avk, encoded, params, newCertificateChainBudget(),
			)
			require.ErrorIs(t, err, errCertificateChainBudget)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestProtocolMessagePartsCap(t *testing.T) {
	t.Parallel()

	parts := func(n int) string {
		entries := make([]string, n)
		for i := range entries {
			entries[i] = fmt.Sprintf(`"k%d":"v"`, i)
		}
		return `{"protocol_message":{"message_parts":{` +
			strings.Join(entries, ",") + `}}}`
	}

	var cert Certificate
	require.NoError(t, json.Unmarshal(
		[]byte(parts(maxProtocolMessageParts)), &cert,
	), "parts exactly at the cap are accepted")
	require.Len(t, cert.ProtocolMessage.MessageParts, maxProtocolMessageParts)
	require.Equal(t, "v", cert.ProtocolMessage.MessageParts["k0"])

	err := json.Unmarshal([]byte(parts(maxProtocolMessageParts+1)), &cert)
	require.ErrorIs(t, err, errCertificateChainBudget)

	var msg ProtocolMessage
	require.NoError(t, json.Unmarshal([]byte(`{"message_parts":null}`), &msg))
	require.Nil(t, msg.MessageParts)
	require.NoError(t, json.Unmarshal([]byte(`{}`), &msg))
	require.Nil(t, msg.MessageParts)
	require.Error(t, json.Unmarshal([]byte(`{"message_parts":[]}`), &msg))
	require.Error(t, json.Unmarshal([]byte(`{"message_parts":{"a":1}}`), &msg))
}
