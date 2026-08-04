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

// gRPC reflection tests: both reflection wire versions must advertise every
// service this listener serves, in both API versions. The v1alpha reflection
// service is an older reflection wire protocol, not an older API surface, so a
// v1alpha client must still discover the v1beta services.

package utxorpc

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	//nolint:staticcheck // SA1019: exercising the v1alpha reflection wire
	// protocol requires its own request/response messages.
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// newReflectionTestServer serves the production routing table over h2c.
func newReflectionTestServer(t *testing.T) (*httptest.Server, *http.Client) {
	t.Helper()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
	})
	srv := httptest.NewUnstartedServer(u.newServeMux())
	srv.Config.Protocols = unencryptedHTTP2Protocols()
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, newConnectH2CClient()
}

// listServices drives one ServerReflectionInfo bidi stream and returns the
// advertised service names. Req/Res are the reflection message types for the
// wire version under test.
func listServices[Req, Res any](
	t *testing.T,
	httpClient *http.Client,
	baseURL, procedure string,
	newRequest func() *Req,
	names func(*Res) []string,
) []string {
	t.Helper()
	client := connect.NewClient[Req, Res](
		httpClient,
		baseURL+procedure,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream := client.CallBidiStream(ctx)
	require.NoError(t, stream.Send(newRequest()))
	require.NoError(t, stream.CloseRequest())
	res, err := stream.Receive()
	require.NoError(t, err)
	require.NoError(t, stream.CloseResponse())
	return names(res)
}

// TestReflection_V1ListsBothApiVersions asserts the grpc.reflection.v1
// ServerReflection service advertises every served service.
func TestReflection_V1ListsBothApiVersions(t *testing.T) {
	srv, httpClient := newReflectionTestServer(t)
	got := listServices(
		t,
		httpClient,
		srv.URL,
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
		func() *reflectionv1.ServerReflectionRequest {
			return &reflectionv1.ServerReflectionRequest{
				MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{},
			}
		},
		func(res *reflectionv1.ServerReflectionResponse) []string {
			out := []string{}
			for _, svc := range res.GetListServicesResponse().GetService() {
				out = append(out, svc.GetName())
			}
			return out
		},
	)
	assert.ElementsMatch(t, servedServiceNames(), got)
}

// TestReflection_V1AlphaListsBothApiVersions is the regression test for the
// v1alpha reflector being registered with only the v1alpha service names: a
// client speaking the older reflection wire protocol could not discover any
// v1beta service.
func TestReflection_V1AlphaListsBothApiVersions(t *testing.T) {
	srv, httpClient := newReflectionTestServer(t)
	got := listServices(
		t,
		httpClient,
		srv.URL,
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
		func() *reflectionv1alpha.ServerReflectionRequest {
			return &reflectionv1alpha.ServerReflectionRequest{
				MessageRequest: &reflectionv1alpha.ServerReflectionRequest_ListServices{},
			}
		},
		func(res *reflectionv1alpha.ServerReflectionResponse) []string {
			out := []string{}
			for _, svc := range res.GetListServicesResponse().GetService() {
				out = append(out, svc.GetName())
			}
			return out
		},
	)
	assert.ElementsMatch(t, servedServiceNames(), got)
	// Spell out the beta half of the contract so a future edit that drops the
	// beta names from the reflector fails here with a clear reason.
	for _, want := range []string{
		"utxorpc.v1beta.query.QueryService",
		"utxorpc.v1beta.submit.SubmitService",
		"utxorpc.v1beta.sync.SyncService",
		"utxorpc.v1beta.watch.WatchService",
	} {
		assert.Containsf(t, got, want,
			"v1alpha reflection must advertise %s", want)
	}
}

// TestServedServiceNames_CoversBothApiVersions pins the served set itself: both
// API versions of all four services, and nothing else.
func TestServedServiceNames_CoversBothApiVersions(t *testing.T) {
	assert.ElementsMatch(t, []string{
		"utxorpc.v1alpha.query.QueryService",
		"utxorpc.v1beta.query.QueryService",
		"utxorpc.v1alpha.submit.SubmitService",
		"utxorpc.v1beta.submit.SubmitService",
		"utxorpc.v1alpha.sync.SyncService",
		"utxorpc.v1beta.sync.SyncService",
		"utxorpc.v1alpha.watch.WatchService",
		"utxorpc.v1beta.watch.WatchService",
	}, servedServiceNames())
}
