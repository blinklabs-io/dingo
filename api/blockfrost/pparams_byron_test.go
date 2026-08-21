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

package blockfrost

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A freshly constructed LedgerState has never loaded protocol parameters, so
// GetCurrentPParams returns nil — the same thing it genuinely reports during a
// Byron prefix, where there is no protocol-parameter CBOR to load.

// TestCurrentProtocolParams_ByronEraSentinel pins the adapter-level contract:
// Byron-era unavailability surfaces as a sentinel callers can branch on, not
// as an opaque string that every caller has to treat as an internal fault.
func TestCurrentProtocolParams_ByronEraSentinel(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)
	require.Nil(
		t,
		adapter.ledgerState.GetCurrentPParams(),
		"precondition: ledger reports no current pparams",
	)

	info, err := adapter.CurrentProtocolParams()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProtocolParamsUnavailable)
	assert.Equal(
		t,
		ProtocolParamsInfo{},
		info,
		"no Shelley-shaped substitute alongside the error",
	)
}

// TestHandleLatestEpochParams_ByronEraNotFound is the behavior change an
// operator sees. A Byron prefix is an expected point in a from-genesis sync,
// so GET /epochs/latest/parameters must not report 500 Internal Server Error
// — that reads as a node fault and trips alerting. 404 matches the
// ErrEpochNotFound precedent already established for absent epoch data.
func TestHandleLatestEpochParams_ByronEraNotFound(t *testing.T) {
	mock := &mockNode{paramsErr: ErrProtocolParamsUnavailable}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/latest/parameters",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestEpochParams(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Not Found", resp["error"])
}

// TestHandleLatestEpochParams_OtherErrorsStillInternal keeps the Byron carve-
// out narrow: a genuine conversion or storage failure must still be a 500.
func TestHandleLatestEpochParams_OtherErrorsStillInternal(t *testing.T) {
	mock := &mockNode{paramsErr: assert.AnError}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/latest/parameters",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestEpochParams(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// TestProtocolParamsForSlot_ByronEraSentinel covers the certificate-deposit
// path's fallback. With no epoch row for the slot it consults the current
// pparams, and a Byron prefix leaves that nil.
func TestProtocolParamsForSlot_ByronEraSentinel(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	pparams, err := adapter.protocolParamsForSlot(0)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProtocolParamsUnavailable)
	assert.Nil(t, pparams)
}

// TestDrepInactivityPeriod_UnavailableIsNotZero is the silent-conversion fix.
// Returning a bare 0 for absent parameters is indistinguishable from a chain
// that genuinely configured drep_activity to 0, and drepStatus then derives
// expiry epochs from a value nobody set.
func TestDrepInactivityPeriod_UnavailableIsNotZero(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	period, ok := adapter.drepInactivityPeriod()

	assert.False(
		t,
		ok,
		"absent protocol parameters must not report a configured value",
	)
	assert.Zero(t, period)
}

// TestDrepInactivityFromPParams_DistinguishesUnavailableFromZero proves the
// second return reports availability rather than merely restating the first:
// a Conway chain that genuinely sets drep_activity to 0 reports (0, true),
// which is what the old bare-uint64 signature could not express.
func TestDrepInactivityFromPParams_DistinguishesUnavailableFromZero(
	t *testing.T,
) {
	for _, tc := range []struct {
		name       string
		pparams    lcommon.ProtocolParameters
		wantPeriod uint64
		wantOK     bool
	}{
		{
			name:    "byron reports unavailable",
			pparams: nil,
			wantOK:  false,
		},
		{
			name: "conway configured zero",
			pparams: &conway.ConwayProtocolParameters{
				DRepInactivityPeriod: 0,
			},
			wantPeriod: 0,
			wantOK:     true,
		},
		{
			name: "conway configured nonzero",
			pparams: &conway.ConwayProtocolParameters{
				DRepInactivityPeriod: 20,
			},
			wantPeriod: 20,
			wantOK:     true,
		},
		{
			name: "dijkstra configured nonzero",
			pparams: &dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					DRepInactivityPeriod: 31,
				},
			},
			wantPeriod: 31,
			wantOK:     true,
		},
		{
			name:    "pre-conway era has no drep semantics",
			pparams: &shelley.ShelleyProtocolParameters{},
			wantOK:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			period, ok := drepInactivityFromPParams(tc.pparams)

			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantPeriod, period)
		})
	}
}
