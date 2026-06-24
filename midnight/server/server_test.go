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

package server_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/midnight"
	"github.com/blinklabs-io/dingo/midnight/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
)

// freePort returns a currently-free TCP port on the loopback interface.
func freePort(t *testing.T) uint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return uint(port) //nolint:gosec // port is always in range
}

// startTestServer starts a server on a free loopback port and returns its
// dial address. The server is stopped on test cleanup.
func startTestServer(t *testing.T) string {
	t.Helper()
	port := freePort(t)
	srv, err := server.New(server.Config{
		Host: "127.0.0.1",
		Port: port,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})

	return net.JoinHostPort("127.0.0.1", strconv.FormatUint(uint64(port), 10))
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// The stub service must answer every RPC with codes.Unimplemented.
func TestStubServiceReturnsUnimplemented(t *testing.T) {
	addr := startTestServer(t)
	client := midnight.NewMidnightStateClient(dial(t, addr))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.GetLatestBlock(ctx, &midnight.LatestBlockRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

// The health service must report SERVING for both the overall server and the
// MidnightState service by name.
func TestHealthCheckServing(t *testing.T) {
	addr := startTestServer(t)
	hc := healthpb.NewHealthClient(dial(t, addr))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, svc := range []string{"", midnight.MidnightState_ServiceDesc.ServiceName} {
		resp, err := hc.Check(
			ctx,
			&healthpb.HealthCheckRequest{Service: svc},
		)
		require.NoError(t, err, "service %q", svc)
		require.Equal(
			t,
			healthpb.HealthCheckResponse_SERVING,
			resp.GetStatus(),
			"service %q",
			svc,
		)
	}
}

// Reflection must be enabled and advertise the MidnightState service.
func TestReflectionListsService(t *testing.T) {
	addr := startTestServer(t)
	client := reflectionpb.NewServerReflectionClient(dial(t, addr))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.ServerReflectionInfo(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{
			ListServices: "*",
		},
	}))
	resp, err := stream.Recv()
	require.NoError(t, err)

	var names []string
	for _, svc := range resp.GetListServicesResponse().GetService() {
		names = append(names, svc.GetName())
	}
	require.Contains(t, names, midnight.MidnightState_ServiceDesc.ServiceName)
}

// New rejects a half-configured TLS pair (cert without key, or key without
// cert).
func TestNewRequiresBothTLSPaths(t *testing.T) {
	_, err := server.New(server.Config{TLSCertFilePath: "cert.pem"})
	require.Error(t, err)
	_, err = server.New(server.Config{TLSKeyFilePath: "key.pem"})
	require.Error(t, err)
}

// Cancelling the context passed to Start must shut the server down.
func TestShutdownOnContextCancel(t *testing.T) {
	port := freePort(t)
	srv, err := server.New(server.Config{
		Host: "127.0.0.1",
		Port: port,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, srv.Start(ctx))
	addr := net.JoinHostPort("127.0.0.1", strconv.FormatUint(uint64(port), 10))

	// While running, the stub answers Unimplemented.
	client := midnight.NewMidnightStateClient(dial(t, addr))
	callCtx, callCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer callCancel()
	_, err = client.GetLatestBlock(callCtx, &midnight.LatestBlockRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	// Cancellation triggers graceful shutdown; once stopped a fresh call no
	// longer reaches the handler (transport error, not Unimplemented).
	cancel()
	testutil.WaitForCondition(t, func() bool {
		conn, dialErr := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if dialErr != nil {
			return true
		}
		defer conn.Close()
		c := midnight.NewMidnightStateClient(conn)
		probeCtx, probeCancel := context.WithTimeout(
			context.Background(),
			200*time.Millisecond,
		)
		defer probeCancel()
		_, probeErr := c.GetLatestBlock(probeCtx, &midnight.LatestBlockRequest{})
		return status.Code(probeErr) != codes.Unimplemented
	}, 5*time.Second, "server did not shut down after context cancel")
}

// Stop is idempotent and safe to call when the server was never started or
// already stopped.
func TestStopIdempotent(t *testing.T) {
	srv, err := server.New(server.Config{})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, srv.Stop(ctx))
	require.NoError(t, srv.Stop(ctx))
}
