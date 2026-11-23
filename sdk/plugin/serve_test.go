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

package plugin

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/sdk/plugin/internal/pluginrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type servedPlugin struct {
	stdin  *io.PipeWriter
	client pluginrpc.ControlClient
	conn   *grpc.ClientConn
	done   chan error
	token  string
}

// startServed runs serve against a pipe standing in for the node's standard
// input and returns a client connected to its socket.
func startServed(t *testing.T, impls ...Implementation) *servedPlugin {
	t.Helper()
	// Not under t.TempDir: its path can exceed the Unix socket limit.
	dir, err := os.MkdirTemp("", "sp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "p.sock")
	pr, pw := io.Pipe()
	sp := &servedPlugin{stdin: pw, done: make(chan error, 1), token: "secret"}
	go func() {
		sp.done <- serve(pr, Manifest{Name: "unit", Version: "9"}, impls)
	}()
	t.Cleanup(func() { _ = pw.Close() })
	line, err := json.Marshal(handshake{
		ProtocolVersion: ProtocolVersion,
		SocketPath:      socket,
		Token:           sp.token,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pw.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(
		"unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	sp.conn = conn
	sp.client = pluginrpc.NewControlClient(conn)
	return sp
}

func (sp *servedPlugin) ctx(t *testing.T, token string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, TokenHeader, token)
}

func (sp *servedPlugin) waitServed(t *testing.T) error {
	t.Helper()
	select {
	case err := <-sp.done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not return")
		return nil
	}
}

var widgetService = grpc.ServiceDesc{
	ServiceName: "example.Widget",
	HandlerType: (*any)(nil),
}

func testImpl(capability string) Implementation {
	return Implementation{
		Capability: capability,
		Service:    &widgetService,
		Impl:       struct{}{},
	}
}

func TestServeNegotiatesAndAuthenticates(t *testing.T) {
	t.Parallel()
	sp := startServed(t, testImpl("example.widget/v3"))

	nonce := []byte("0123456789abcdef")
	resp, err := sp.client.Negotiate(
		sp.ctx(t, sp.token),
		&pluginrpc.NegotiateRequest{Nonce: nonce},
		grpc.WaitForReady(true),
	)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}
	mac := hmac.New(sha256.New, []byte(sp.token))
	mac.Write(nonce)
	if !hmac.Equal(resp.GetProof(), mac.Sum(nil)) {
		t.Fatal("proof is not the HMAC of the nonce under the token")
	}
	if resp.GetProtocolVersion() != ProtocolVersion ||
		resp.GetPluginName() != "unit" ||
		resp.GetPluginVersion() != "9" ||
		len(resp.GetCapabilities()) != 1 ||
		resp.GetCapabilities()[0] != "example.widget/v3" {
		t.Fatalf("unexpected negotiation response: %v", resp)
	}

	for _, token := range []string{"", "wrong"} {
		_, err := sp.client.Ping(sp.ctx(t, token), &pluginrpc.PingRequest{})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf(
				"ping with token %q: got %v, want Unauthenticated",
				token,
				err,
			)
		}
	}
	if _, err := sp.client.Ping(
		sp.ctx(t, sp.token),
		&pluginrpc.PingRequest{},
	); err != nil {
		t.Fatalf("authenticated ping: %v", err)
	}
}

func TestServeStopsOnStopRequest(t *testing.T) {
	t.Parallel()
	sp := startServed(t, testImpl("example.widget/v0"))
	if _, err := sp.client.Stop(
		sp.ctx(t, sp.token),
		&pluginrpc.StopRequest{},
		grpc.WaitForReady(true),
	); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := sp.waitServed(t); err != nil {
		t.Fatalf("serve after Stop: %v", err)
	}
}

func TestServeStopsWhenInputCloses(t *testing.T) {
	t.Parallel()
	sp := startServed(t, testImpl("example.widget/v0"))
	if _, err := sp.client.Ping(
		sp.ctx(t, sp.token),
		&pluginrpc.PingRequest{},
		grpc.WaitForReady(true),
	); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := sp.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sp.waitServed(t); err != nil {
		t.Fatalf("serve after input closed: %v", err)
	}
}

func TestServeRejectsBadInput(t *testing.T) {
	t.Parallel()
	good := testImpl("example.widget/v0")
	for name, tc := range map[string]struct {
		input string
		impls []Implementation
	}{
		"no handshake": {"", []Implementation{good}},
		"not json":     {"hello\n", []Implementation{good}},
		"no token": {
			`{"socket_path":"/tmp/x"}` + "\n",
			[]Implementation{good},
		},
		"no version suffix": {
			`{"socket_path":"/tmp/x","token":"t"}` + "\n",
			[]Implementation{testImpl("widget")},
		},
		"no service": {
			`{"socket_path":"/tmp/x","token":"t"}` + "\n",
			[]Implementation{{Capability: "example.widget/v0"}},
		},
		"duplicate capability": {
			`{"socket_path":"/tmp/x","token":"t"}` + "\n",
			[]Implementation{good, good},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := serve(stringsReader(tc.input), Manifest{}, tc.impls)
			if err == nil {
				t.Fatal("serve accepted bad input")
			}
		})
	}
}

func stringsReader(s string) io.Reader { return strings.NewReader(s) }

func TestServeNegotiatesWithoutTokenButGuardsEveryOtherCall(t *testing.T) {
	t.Parallel()
	sp := startServed(t, Implementation{
		Capability: "grpc.health/v1",
		Service:    &healthpb.Health_ServiceDesc,
		Impl:       health.NewServer(),
	})

	nonce := []byte("fedcba9876543210")
	resp, err := sp.client.Negotiate(
		sp.ctx(t, ""),
		&pluginrpc.NegotiateRequest{Nonce: nonce},
		grpc.WaitForReady(true),
	)
	if err != nil {
		t.Fatalf("negotiate without token: %v", err)
	}
	mac := hmac.New(sha256.New, []byte(sp.token))
	mac.Write(nonce)
	if !hmac.Equal(resp.GetProof(), mac.Sum(nil)) {
		t.Fatal("proof is not the HMAC of the nonce under the token")
	}

	healthClient := healthpb.NewHealthClient(sp.conn)
	calls := map[string]func(context.Context) error{
		"ping": func(ctx context.Context) error {
			_, err := sp.client.Ping(ctx, &pluginrpc.PingRequest{})
			return err
		},
		"stop": func(ctx context.Context) error {
			_, err := sp.client.Stop(ctx, &pluginrpc.StopRequest{})
			return err
		},
		"capability unary": func(ctx context.Context) error {
			_, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
			return err
		},
		"capability stream": func(ctx context.Context) error {
			stream, err := healthClient.Watch(
				ctx,
				&healthpb.HealthCheckRequest{},
			)
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		},
	}
	for name, call := range calls {
		if err := call(sp.ctx(t, "")); status.Code(
			err,
		) != codes.Unauthenticated {
			t.Fatalf(
				"%s without token: got %v, want Unauthenticated",
				name,
				err,
			)
		}
	}
	if _, err := healthClient.Check(
		sp.ctx(t, sp.token),
		&healthpb.HealthCheckRequest{},
	); err != nil {
		t.Fatalf("capability call with token: %v", err)
	}
}
