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

package txpump

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// dialTimeout is the maximum time to wait for a connection to the node.
const dialTimeout = 10 * time.Second

// NodeClient wraps an Ouroboros N2C connection for transaction submission.
type NodeClient struct {
	conn   *ouroboros.Connection
	addr   string
	proto  string
	magic  uint32
	logger *slog.Logger
}

// NewNodeClient creates a new NodeClient and connects it to the node at addr
// using the given network magic. The addr parameter is treated as a Unix
// socket path when it begins with "/", and as a TCP host:port otherwise.
//
// The caller is responsible for calling Close when done.
func NewNodeClient(
	addr string,
	magic uint32,
	logger *slog.Logger,
) (*NodeClient, error) {
	if logger == nil {
		logger = slog.Default()
	}

	proto := protoFromAddr(addr)

	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false), // N2C
		ouroboros.WithLogger(logger),
	)
	if err != nil {
		return nil, fmt.Errorf("ouroboros.New: %w", err)
	}

	if err := conn.DialTimeout(proto, addr, dialTimeout); err != nil {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("dial %s %s: %w", proto, addr, err)
	}

	return &NodeClient{
		conn:   conn,
		addr:   addr,
		proto:  proto,
		magic:  magic,
		logger: logger,
	}, nil
}

// SubmitTx submits a raw CBOR-encoded transaction to the node using the
// LocalTxSubmission mini-protocol.  eraID should be conwayEraID (6) for
// Conway transactions.
func (c *NodeClient) SubmitTx(eraID uint16, txBytes []byte) error {
	lts := c.conn.LocalTxSubmission()
	if lts == nil {
		return errors.New("LocalTxSubmission protocol not available on this connection")
	}
	return lts.Client.SubmitTx(eraID, txBytes)
}

// Close shuts down the underlying Ouroboros connection.
func (c *NodeClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Addr returns the address this client is connected to.
func (c *NodeClient) Addr() string {
	return c.addr
}

// protoFromAddr returns "unix" for paths starting with "/" and "tcp"
// otherwise.
func protoFromAddr(addr string) string {
	if strings.HasPrefix(addr, "/") {
		return "unix"
	}
	return "tcp"
}
