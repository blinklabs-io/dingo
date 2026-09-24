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
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSecureMithrilHTTPClientClearsTransportBypasses(t *testing.T) {
	t.Parallel()

	dialCalled := false
	dial := func(context.Context, string, string) (net.Conn, error) {
		dialCalled = true
		return nil, errors.New("custom dial should not be used")
	}
	dialNoContext := func(string, string) (net.Conn, error) {
		dialCalled = true
		return nil, errors.New("custom dial should not be used")
	}
	client, err := secureMithrilHTTPClient(&http.Client{
		Transport: &http.Transport{
			Proxy:          http.ProxyFromEnvironment,
			Dial:           dialNoContext,
			DialContext:    dial,
			DialTLS:        dialNoContext,
			DialTLSContext: dial,
		},
	}, false)
	require.NoError(t, err)

	transport, ok := client.Transport.(*restrictedHTTPTransport)
	require.True(t, ok)
	require.Nil(t, transport.Proxy)
	require.Nil(t, transport.Dial)
	require.Nil(t, transport.DialTLS)
	require.Nil(t, transport.DialTLSContext)
	_, err = transport.DialContext(
		context.Background(),
		"tcp",
		"127.0.0.1:443",
	)
	require.ErrorContains(t, err, "not allowed")
	require.False(t, dialCalled)
}

func TestMithrilRedirectPolicyRejectsPrivateDestination(t *testing.T) {
	t.Parallel()

	req, err := http.NewRequest(
		http.MethodGet,
		"https://127.0.0.1/snapshot.tar.zst",
		nil,
	)
	require.NoError(t, err)
	via, err := http.NewRequest(
		http.MethodGet,
		"https://artifacts.example/snapshot.tar.zst",
		nil,
	)
	require.NoError(t, err)

	err = mithrilRedirectPolicy(nil, false)(req, []*http.Request{via})
	require.ErrorContains(t, err, "not allowed")
}

func TestMithrilRedirectPolicyAllowsExplicitLocalDestination(t *testing.T) {
	t.Parallel()

	req, err := http.NewRequest(
		http.MethodGet,
		"http://127.0.0.1/snapshot.tar.zst",
		nil,
	)
	require.NoError(t, err)
	via, err := http.NewRequest(
		http.MethodGet,
		"http://127.0.0.1/old-snapshot.tar.zst",
		nil,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		mithrilRedirectPolicy(nil, true)(req, []*http.Request{via}),
	)
}
