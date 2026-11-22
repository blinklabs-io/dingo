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

package blockfrost

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Account keys and addresses from the deriveAddress fixtures of blockfrost-js
// (test/fixtures/utils/helpers.ts), which Blockfrost's xpub endpoint calls.
const (
	xpubFixtureA = "7ec9738746cb4708df52a455b43aa3fdee8955abaf37f68ffc79bb84fbf9e1b3" +
		"9d77e2deb9749faf890ff8326d350ed3fd0e4aa271b35cad063692af87102152"
	xpubFixtureB = "6d17587575a3b4f0f86ebad3977e8f7e4981faa863eccf5c1467065c74fe3435" +
		"943769446dd290d103fb3d360128e86de4b47faea73ffb0900c94c6a61ef9ea2"
)

func TestDeriveXpubAddress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		xpub        string
		role, index uint32
		network     uint8
		want        string
	}{
		{xpubFixtureA, 0, 1, lcommon.AddressNetworkMainnet, "addr1qy535472n2ctu3x55v03zmm9jnz54grqu3sueap9pnk4xys49ucjdfty5p5qlw5qe28v9k988stffc2g0hx2xx86a2dq5u58qk"},
		{xpubFixtureA, 2, 0, lcommon.AddressNetworkMainnet, "stake1uy2j7vfx54j2q6q0h2qv4rkzmznnc955u9y8mn9rrraw4xspqg6tp"},
		{xpubFixtureB, 0, 0, lcommon.AddressNetworkMainnet, "addr1q8u5ktsj5zsmhvwv0ep9zuhfu39x3wyt9wxjnsn3cagsyy59ckxhkvuc5xj49rw6zrp443wlygmhv8gwcu38jk6ms6usrmcafl"},
		{xpubFixtureB, 2, 1, lcommon.AddressNetworkMainnet, "stake1uyq2806du47kxcvr2504vuvmngauy5mhv3d45d2mukek3ugj46jvz"},
		{xpubFixtureA, 1, 3, lcommon.AddressNetworkTestnet, "addr_test1qqtpyxyh5j023fq88xhj862guwrymhwjadt5czqvumpv02s49ucjdfty5p5qlw5qe28v9k988stffc2g0hx2xx86a2dqhvnrk6"},
	} {
		xpub, err := hex.DecodeString(tc.xpub)
		require.NoError(t, err)
		got, err := deriveXpubAddress(xpub, tc.role, tc.index, tc.network)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "role %d index %d", tc.role, tc.index)
	}

	xpub, err := hex.DecodeString(xpubFixtureA)
	require.NoError(t, err)
	_, err = deriveXpubAddress(xpub, 0, 1<<31, lcommon.AddressNetworkMainnet)
	require.ErrorIs(t, err, errXpubHardenedIndex)
}

func TestHandleXpubAddressRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	handler := newTestBlockfrost(&mockNode{xpubAddress: "addr1x"}).handler()
	for _, tc := range []struct {
		name, path, message string
	}{
		{"short xpub", "/abcd/0/0", "Invalid or malformed xpub format. Has to be hex of length 128."},
		{"non-hex xpub", "/" + strings.Repeat("zz", 64) + "/0/0", "Invalid or malformed xpub format. Has to be hex of length 128."},
		{"role not a number", "/" + xpubFixtureA + "/x/0", "Missing, out of range or malformed role."},
		{"negative index", "/" + xpubFixtureA + "/0/-1", "Missing, out of range or malformed index."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(
				http.MethodGet,
				"/api/v0/utils/addresses/xpub"+tc.path,
				nil,
			))
			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), tc.message)
		})
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(
		http.MethodGet,
		"/api/v0/utils/addresses/xpub/"+xpubFixtureA+"/0/3",
		nil,
	))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(
		t,
		`{"xpub":"`+xpubFixtureA+`","role":0,"index":3,"address":"addr1x"}`,
		recorder.Body.String(),
	)
}

func TestHandleMetadataLabelsNamesCIP10Labels(t *testing.T) {
	t.Parallel()
	node := &mockNode{
		metadataLabels: []MetadataLabelInfo{
			{Label: 721, Count: 5},
			{Label: 1990, Count: 1},
		},
		metadataLabelsTotal: 2,
	}
	recorder := httptest.NewRecorder()
	newTestBlockfrost(node).handler().ServeHTTP(
		recorder,
		httptest.NewRequest(
			http.MethodGet,
			"/api/v0/metadata/txs/labels?count=2&order=desc",
			nil,
		),
	)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `[
		{"label":"721","cip10":"CIP-0025 - NFT Metadata Standard","count":"5"},
		{"label":"1990","cip10":null,"count":"1"}
	]`, recorder.Body.String())
	require.Equal(t, 2, node.metadataLabelsParams.Count)
	require.Equal(t, PaginationOrderDesc, node.metadataLabelsParams.Order)
}

// The root operation is /api/v0/; the bare prefix redirects to it.
func TestRootUnderPrefixRedirectsBarePrefix(t *testing.T) {
	t.Parallel()
	recorder := httptest.NewRecorder()
	newTestBlockfrost(&mockNode{}).handler().ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/api/v0", nil),
	)
	require.Equal(t, http.StatusTemporaryRedirect, recorder.Code)
	require.Equal(t, "/api/v0/", recorder.Header().Get("Location"))
}

func TestHandleHealthClockReportsMilliseconds(t *testing.T) {
	t.Parallel()
	handler := newTestBlockfrost(&mockNode{}).handler()
	for _, path := range []string{"/health/clock", "/api/v0/health/clock"} {
		before := time.Now().UnixMilli()
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(
			recorder,
			httptest.NewRequest(http.MethodGet, path, nil),
		)
		after := time.Now().UnixMilli()
		require.Equal(t, http.StatusOK, recorder.Code, path)
		var resp HealthClockResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp), path)
		require.GreaterOrEqual(t, resp.ServerTime, before, path)
		require.LessOrEqual(t, resp.ServerTime, after, path)
	}
}
