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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientSignerReads(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/epoch-settings":
				_, _ = io.WriteString(
					w,
					`{"epoch":7,"current_signers":[{"party_id":"pool1a","verification_key":"aa","kes_period":3}],"next_signers":[{"party_id":"pool1b","verification_key":"bb"}]}`,
				)
			case "/protocol-configuration/7":
				_, _ = io.WriteString(
					w,
					`{"protocol_parameters":{"k":5,"m":100,"phi_f":0.7}}`,
				)
			case "/signers/registered/5":
				_, _ = io.WriteString(
					w,
					`{"registered_at":5,"signing_at":7,"registrations":[{"party_id":"pool1a","stake":42}]}`,
				)
			default:
				http.NotFound(w, r)
			}
		},
	))
	defer srv.Close()
	client := NewClient(srv.URL, WithAllowInsecureHTTP())

	settings, err := client.GetEpochSettings(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(7), settings.Epoch)
	assert.Equal(
		t,
		[]AggregatorSigner{
			{PartyID: "pool1a", VerificationKey: "aa", KESPeriod: 3},
		},
		settings.CurrentSigners,
	)
	assert.Equal(t, "pool1b", settings.NextSigners[0].PartyID)

	config, err := client.GetProtocolConfiguration(t.Context(), 7)
	require.NoError(t, err)
	assert.Equal(
		t,
		ProtocolParameters{K: 5, M: 100, PhiF: 0.7},
		config.ProtocolParameters,
	)

	registered, err := client.GetRegisteredSigners(t.Context(), 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), registered.SigningAt)
	assert.Equal(
		t,
		[]StakeDistributionParty{{PartyID: "pool1a", Stake: 42}},
		registered.Registrations,
	)

	_, err = client.GetProtocolConfiguration(t.Context(), 8)
	var statusErr *HTTPStatusError
	require.ErrorAs(t, err, &statusErr)
	assert.Equal(t, http.StatusNotFound, statusErr.StatusCode)
}

func TestClientSignerWrites(t *testing.T) {
	t.Parallel()
	type received struct {
		path        string
		contentType string
		body        map[string]any
	}
	got := make(chan received, 2)
	status := http.StatusCreated
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			got <- received{r.URL.Path, r.Header.Get("Content-Type"), body}
			w.WriteHeader(status)
		},
	))
	defer srv.Close()
	client := NewClient(srv.URL, WithAllowInsecureHTTP())

	require.NoError(t, client.RegisterSigner(t.Context(), 9, AggregatorSigner{
		PartyID:                  "pool1a",
		VerificationKey:          "aa",
		VerificationKeySignature: "bb",
		OperationalCertificate:   "cc",
		KESPeriod:                4,
	}))
	req := <-got
	assert.Equal(t, "/register-signer", req.path)
	assert.Equal(t, "application/json", req.contentType)
	assert.Equal(t, map[string]any{
		"epoch":                      float64(9),
		"party_id":                   "pool1a",
		"verification_key":           "aa",
		"verification_key_signature": "bb",
		"operational_certificate":    "cc",
		"kes_period":                 float64(4),
	}, req.body)

	registration := SingleSignatureRegistration{
		EntityType:    MithrilStakeDistributionEntityType(9),
		PartyID:       "pool1a",
		Signature:     "dd",
		Indexes:       []uint64{1, 5},
		SignedMessage: "ee",
	}
	status = http.StatusAccepted
	require.NoError(
		t,
		client.RegisterSingleSignature(t.Context(), registration),
	)
	req = <-got
	assert.Equal(t, "/register-signatures", req.path)
	assert.Equal(t, map[string]any{
		"entity_type": map[string]any{
			"MithrilStakeDistribution": float64(9),
		},
		"party_id":       "pool1a",
		"signature":      "dd",
		"indexes":        []any{float64(1), float64(5)},
		"signed_message": "ee",
	}, req.body)

	status = http.StatusGone
	err := client.RegisterSingleSignature(t.Context(), registration)
	var statusErr *HTTPStatusError
	require.ErrorAs(t, err, &statusErr)
	assert.Equal(t, http.StatusGone, statusErr.StatusCode)
	<-got

	// A status the call does not list is a failure even when it is 2xx.
	status = http.StatusOK
	require.Error(t, client.RegisterSigner(t.Context(), 9, AggregatorSigner{}))
	<-got
}
