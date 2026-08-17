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
	"errors"
	"net/http"
	"testing"
	"time"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestNetworkList(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postJSON(t, h, "/network/list", MetadataRequest{})

	resp := decodeResponse[NetworkListResponse](t, rec)
	require.Equal(
		t,
		[]*NetworkIdentifier{
			{Blockchain: "cardano", Network: testNetwork},
		},
		resp.NetworkIdentifiers,
	)
}

// TestNetworkListMalformedBody documents that /network/list rejects a
// body that is not a JSON object rather than treating it as empty.
func TestNetworkListMalformedBody(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postRaw(t, h, "/network/list", "{not json")

	requireMeshError(
		t, rec, ErrInvalidRequest, http.StatusBadRequest,
	)
}

func TestNetworkOptions(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postJSON(t, h, "/network/options", NetworkRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	})

	resp := decodeResponse[NetworkOptionsResponse](t, rec)
	require.NotNil(t, resp.Version)
	require.Equal(t, rosettaVersion, resp.Version.RosettaVersion)
	require.Equal(t, nodeVersion, resp.Version.NodeVersion)
	require.NotNil(t, resp.Allow)
	require.Equal(
		t, OperationTypes(), resp.Allow.OperationTypes,
	)
	require.Equal(
		t, OperationStatuses(), resp.Allow.OperationStatuses,
	)
	require.Len(t, resp.Allow.Errors, len(AllErrors()))
	// Clients rely on these flags to decide which calls to make:
	// /account/balance honors a block_identifier, while mempool coins
	// are not implemented.
	require.True(t, resp.Allow.HistoricalBalanceLookup)
	require.False(t, resp.Allow.MempoolCoins)
}

// TestNetworkOptionsAdvertisesEveryError guards the contract that
// /network/options enumerates every error a client can encounter, so
// stable codes stay discoverable.
func TestNetworkOptionsAdvertisesEveryError(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postJSON(t, h, "/network/options", NetworkRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	})

	resp := decodeResponse[NetworkOptionsResponse](t, rec)
	advertised := make(map[int32]*Error, len(resp.Allow.Errors))
	for _, e := range resp.Allow.Errors {
		advertised[e.Code] = e
	}
	for _, want := range AllErrors() {
		got, ok := advertised[want.Code]
		require.True(
			t, ok, "error code %d not advertised", want.Code,
		)
		require.Equal(t, want.Message, got.Message)
		require.Equal(t, want.Description, got.Description)
		require.Equal(t, want.Retriable, got.Retriable)
	}
}

func TestNetworkStatus(t *testing.T) {
	deps := newTestDeps()
	tipHash := testHash(0xab)
	deps.chain.tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(4242, tipHash),
		BlockNumber: 99,
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/network/status", NetworkRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	})

	resp := decodeResponse[NetworkStatusResponse](t, rec)
	require.Equal(
		t,
		&BlockIdentifier{
			Index: 99,
			Hash:  hexString(tipHash),
		},
		resp.CurrentBlockIdentifier,
	)
	require.Equal(
		t,
		&BlockIdentifier{Index: 0, Hash: testGenesisHash},
		resp.GenesisBlockIdentifier,
	)
	require.Equal(
		t,
		time.Unix(testGenesisStartTimeSec+4242, 0).UnixMilli(),
		resp.CurrentBlockTimestamp,
	)
	require.NotNil(t, resp.SyncStatus)
	require.NotNil(t, resp.SyncStatus.Synced)
	require.True(t, *resp.SyncStatus.Synced)
	require.NotNil(t, resp.Peers)
	require.Empty(t, resp.Peers)
}

// TestNetworkStatusSlotToTimeFallback covers the path where the epoch
// cache is not yet populated: the handler must still return a timestamp
// derived from genesis rather than failing the request.
func TestNetworkStatusSlotToTimeFallback(t *testing.T) {
	deps := newTestDeps()
	deps.ledger.slotToTime = func(uint64) (time.Time, error) {
		return time.Time{}, errors.New("epoch cache empty")
	}
	deps.chain.tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(120, testHash(0x01)),
		BlockNumber: 7,
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/network/status", NetworkRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	})

	resp := decodeResponse[NetworkStatusResponse](t, rec)
	require.Equal(
		t,
		(testGenesisStartTimeSec+120)*1000,
		resp.CurrentBlockTimestamp,
	)
}

// TestNetworkValidationRejectsUnknownNetwork covers every endpoint that
// carries a network identifier: an identifier for another chain or
// network must fail with a stable 404 rather than being ignored.
func TestNetworkValidationRejectsUnknownNetwork(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	for _, path := range networkValidatedRoutes() {
		t.Run(path, func(t *testing.T) {
			for _, id := range []*NetworkIdentifier{
				{Blockchain: "bitcoin", Network: testNetwork},
				{Blockchain: blockchain, Network: "mainnet"},
			} {
				rec := postJSON(t, h, path, NetworkRequest{
					networkIdentifierField: networkIdentifierField{
						NetworkIdentifier: id,
					},
				})
				requireMeshError(
					t,
					rec,
					ErrNetworkNotSupported,
					http.StatusNotFound,
				)
			}
		})
	}
}

// TestNetworkValidationRequiresIdentifier asserts a missing
// network_identifier is rejected before any handler-specific work.
func TestNetworkValidationRequiresIdentifier(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	for _, path := range networkValidatedRoutes() {
		t.Run(path, func(t *testing.T) {
			rec := postRaw(t, h, path, `{}`)
			requireMeshError(
				t,
				rec,
				ErrInvalidRequest,
				http.StatusBadRequest,
			)
		})
	}
}

// TestNetworkValidationRejectsMalformedBody asserts malformed JSON is
// reported as an invalid request on every network-validated route.
func TestNetworkValidationRejectsMalformedBody(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	for _, path := range networkValidatedRoutes() {
		t.Run(path, func(t *testing.T) {
			rec := postRaw(t, h, path, "{")
			requireMeshError(
				t,
				rec,
				ErrInvalidRequest,
				http.StatusBadRequest,
			)
		})
	}
}

// networkValidatedRoutes lists every route that runs the shared
// network-identifier validation, so the checks above stay in step with
// registerRoutes.
func networkValidatedRoutes() []string {
	return []string{
		"/network/options",
		"/network/status",
		"/block",
		"/block/transaction",
		"/account/balance",
		"/account/coins",
		"/mempool",
		"/mempool/transaction",
		"/construction/derive",
		"/construction/preprocess",
		"/construction/metadata",
		"/construction/payloads",
		"/construction/combine",
		"/construction/parse",
		"/construction/hash",
		"/construction/submit",
	}
}
