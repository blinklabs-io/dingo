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

// Package plugin is how an out-of-process Dingo plugin serves its
// capabilities to the node.
//
// The node launches the plugin executable and writes one JSON line to its
// standard input: the control protocol version, the Unix socket path to serve
// on, and a one-time token. Serve listens on that socket, answers the node's
// Negotiate, Ping and Stop calls, and rejects any call that does not carry the
// token. A plugin implements each capability as an ordinary gRPC service
// defined by that capability's contract.
//
// Contract versions are matched exactly while the sdk is below 1.0: a plugin
// reports the capabilities it implements as "<name>/v<N>", and the node
// refuses to start one whose version differs from the one it resolves.
package plugin

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/blinklabs-io/dingo/sdk/plugin/internal/pluginrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ProtocolVersion is the control protocol version this module speaks. The node
// and the plugin must agree on it exactly.
const ProtocolVersion uint32 = 0

// TokenHeader is the gRPC metadata key that carries the launch token on every
// call to the plugin.
const TokenHeader = "dingo-plugin-token"

// maxHandshakeBytes bounds the handshake line so a misbehaving parent cannot
// make the plugin buffer without limit.
const maxHandshakeBytes = 8192

// Manifest identifies a plugin to the node.
type Manifest struct {
	// Name is the plugin's own name, used in logs and errors.
	Name string
	// Version is the plugin's build version, informational only.
	Version string
}

// Implementation is one capability a plugin serves.
type Implementation struct {
	// Capability is the contract the service implements, "<name>/v<N>".
	Capability string
	// Service is the generated gRPC service descriptor of that contract.
	Service *grpc.ServiceDesc
	// Impl is the server value implementing Service.
	Impl any
}

// handshake is the line the node writes to the plugin's standard input.
type handshake struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	SocketPath      string `json:"socket_path"`
	Token           string `json:"token"`
}

// Serve runs a plugin until the node asks it to stop, closes its standard
// input, or serving fails. It reads the handshake from standard input, so it
// must be called from the process the node launched, before anything else
// consumes standard input.
func Serve(manifest Manifest, impls ...Implementation) error {
	return serve(os.Stdin, manifest, impls)
}

func serve(in io.Reader, manifest Manifest, impls []Implementation) error {
	capabilities, err := validateImplementations(impls)
	if err != nil {
		return err
	}
	reader := bufio.NewReaderSize(io.LimitReader(in, maxHandshakeBytes), 512)
	hs, err := readHandshake(reader)
	if err != nil {
		return err
	}
	listener, err := net.Listen("unix", hs.SocketPath)
	if err != nil {
		return fmt.Errorf("listen on plugin socket: %w", err)
	}
	// The launching node owns the runtime directory and removes the socket
	// file; Serve only closes the listener.
	defer listener.Close()

	control := &controlServer{
		manifest:     manifest,
		capabilities: capabilities,
		token:        []byte(hs.Token),
		stop:         make(chan stopMode, 1),
	}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(control.authUnary),
		grpc.StreamInterceptor(control.authStream),
	)
	pluginrpc.RegisterControlServer(srv, control)
	for _, impl := range impls {
		srv.RegisterService(impl.Service, impl.Impl)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()
	go func() {
		// The node holds the pipe open for the life of the process, so end of
		// input means the node is gone and nothing is waiting for a drain.
		_, _ = io.Copy(io.Discard, in)
		control.requestStop(stopImmediate)
	}()

	select {
	case mode := <-control.stop:
		if mode == stopImmediate {
			srv.Stop()
		} else {
			srv.GracefulStop()
		}
		return nil
	case err := <-serveErr:
		return fmt.Errorf("serve plugin socket: %w", err)
	}
}

func validateImplementations(impls []Implementation) ([]string, error) {
	capabilities := make([]string, 0, len(impls))
	seen := make(map[string]struct{}, len(impls))
	for _, impl := range impls {
		name, version, ok := strings.Cut(impl.Capability, "/v")
		if !ok || name == "" || version == "" {
			return nil, fmt.Errorf(
				"plugin capability %q must be written <name>/v<N>",
				impl.Capability,
			)
		}
		if impl.Service == nil || impl.Impl == nil {
			return nil, fmt.Errorf(
				"plugin capability %q has no service or implementation",
				impl.Capability,
			)
		}
		if _, dup := seen[impl.Capability]; dup {
			return nil, fmt.Errorf(
				"plugin capability %q is implemented twice",
				impl.Capability,
			)
		}
		seen[impl.Capability] = struct{}{}
		capabilities = append(capabilities, impl.Capability)
	}
	return capabilities, nil
}

func readHandshake(r *bufio.Reader) (handshake, error) {
	var hs handshake
	line, err := r.ReadBytes('\n')
	if err != nil && (!errors.Is(err, io.EOF) || len(line) == 0) {
		return hs, fmt.Errorf("read plugin handshake: %w", err)
	}
	if err := json.Unmarshal(line, &hs); err != nil {
		return hs, fmt.Errorf("decode plugin handshake: %w", err)
	}
	if hs.SocketPath == "" || hs.Token == "" {
		return hs, errors.New("plugin handshake lacks a socket path or token")
	}
	return hs, nil
}

type stopMode int

const (
	stopGraceful stopMode = iota
	stopImmediate
)

type controlServer struct {
	pluginrpc.UnimplementedControlServer
	manifest     Manifest
	capabilities []string
	token        []byte
	stop         chan stopMode
	stopOnce     sync.Once
}

func (c *controlServer) requestStop(mode stopMode) {
	c.stopOnce.Do(func() { c.stop <- mode })
}

func (c *controlServer) authorized(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, got := range md.Get(TokenHeader) {
		if subtle.ConstantTimeCompare([]byte(got), c.token) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "plugin token missing or wrong")
}

func (c *controlServer) authUnary(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if err := c.authorized(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (c *controlServer) authStream(
	srv any,
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if err := c.authorized(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (c *controlServer) Negotiate(
	_ context.Context,
	req *pluginrpc.NegotiateRequest,
) (*pluginrpc.NegotiateResponse, error) {
	mac := hmac.New(sha256.New, c.token)
	mac.Write(req.GetNonce())
	return &pluginrpc.NegotiateResponse{
		ProtocolVersion: ProtocolVersion,
		PluginName:      c.manifest.Name,
		PluginVersion:   c.manifest.Version,
		Capabilities:    c.capabilities,
		Proof:           mac.Sum(nil),
	}, nil
}

func (c *controlServer) Ping(
	context.Context,
	*pluginrpc.PingRequest,
) (*pluginrpc.PingResponse, error) {
	return &pluginrpc.PingResponse{}, nil
}

func (c *controlServer) Stop(
	context.Context,
	*pluginrpc.StopRequest,
) (*pluginrpc.StopResponse, error) {
	c.requestStop(stopGraceful)
	return &pluginrpc.StopResponse{}, nil
}
