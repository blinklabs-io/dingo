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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
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
	if c == nil {
		return errors.New("SubmitTx called on nil NodeClient")
	}
	if c.conn == nil {
		return fmt.Errorf(
			"node %s: SubmitTx eraID=%d: connection is nil",
			c.addr, eraID,
		)
	}
	lts := c.conn.LocalTxSubmission()
	if lts == nil {
		return fmt.Errorf(
			"node %s: LocalTxSubmission protocol not available",
			c.addr,
		)
	}
	if lts.Client == nil {
		return fmt.Errorf(
			"node %s: SubmitTx eraID=%d: LocalTxSubmission client is nil",
			c.addr, eraID,
		)
	}
	if err := lts.Client.SubmitTx(eraID, txBytes); err != nil {
		return fmt.Errorf("node %s: SubmitTx eraID=%d: %w", c.addr, eraID, err)
	}
	return nil
}

// ReconcileWallet reads the controlled UTxO snapshot and pending transaction
// presence at the node. Missing outputs are a successful empty result; query
// errors are returned so callers can leave wallet state unchanged.
// LSQ and transaction-monitor snapshots are acquired independently.
func (c *NodeClient) ReconcileWallet(
	addresses [][]byte,
	txIDs []string,
) (snapshot []UTxO, presence map[string]bool, retErr error) {
	if c == nil || c.conn == nil {
		return nil, nil, errors.New(
			"ReconcileWallet called without a connection",
		)
	}
	lsq := c.conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, nil, fmt.Errorf(
			"node %s: LocalStateQuery protocol not available",
			c.addr,
		)
	}
	addrs := make([]ledger.Address, 0, len(addresses))
	for _, raw := range addresses {
		addr, err := common.NewAddressFromBytes(raw)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"node %s: decode wallet address: %w",
				c.addr,
				err,
			)
		}
		addrs = append(addrs, addr)
	}
	presence = make(map[string]bool, len(txIDs))
	monitor := c.conn.LocalTxMonitor()
	if monitor == nil || monitor.Client == nil {
		return nil, nil, fmt.Errorf(
			"node %s: LocalTxMonitor protocol not available",
			c.addr,
		)
	}
	if err := monitor.Client.Acquire(); err != nil {
		return nil, nil, fmt.Errorf(
			"node %s: acquire tx monitor: %w",
			c.addr,
			err,
		)
	}
	defer func() {
		if err := monitor.Client.Release(); err != nil && retErr == nil {
			snapshot = nil
			presence = nil
			retErr = fmt.Errorf("node %s: release tx monitor: %w", c.addr, err)
		}
	}()
	for _, txID := range txIDs {
		rawID, err := hex.DecodeString(txID)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"node %s: decode pending tx %s: %w",
				c.addr,
				txID,
				err,
			)
		}
		present, err := monitor.Client.HasTx(rawID)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"node %s: check pending tx %s: %w",
				c.addr,
				txID,
				err,
			)
		}
		presence[txID] = present
	}
	// Observe transaction presence before acquiring the authoritative UTxO
	// snapshot. A transaction can confirm between the two observations; the
	// monitor result must describe the state at or before the LSQ snapshot so a
	// confirmed spend cannot resurrect its source input.
	if err := lsq.Client.AcquireVolatileTip(); err != nil {
		return nil, nil, fmt.Errorf(
			"node %s: acquire volatile tip: %w",
			c.addr,
			err,
		)
	}
	defer func() {
		if err := lsq.Client.Release(); err != nil && retErr == nil {
			snapshot = nil
			presence = nil
			retErr = fmt.Errorf(
				"node %s: release local state query: %w",
				c.addr,
				err,
			)
		}
	}()
	result, err := lsq.Client.GetUTxOByAddress(addrs)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"node %s: query wallet UTxOs: %w",
			c.addr,
			err,
		)
	}
	snapshot = make([]UTxO, 0, len(result.Results))
	for id, output := range result.Results {
		raw, err := output.Address().Bytes()
		if err != nil {
			return nil, nil, fmt.Errorf(
				"node %s: encode wallet address: %w",
				c.addr,
				err,
			)
		}
		amount := output.Amount()
		snapshot = append(
			snapshot,
			UTxO{
				TxHash: id.Hash.String(),
				//nolint:gosec // ledger index is bounded by protocol
				Index:   uint32(id.Idx),
				Amount:  amount.Uint64(),
				address: raw,
			},
		)
	}
	return snapshot, presence, nil
}

// Close shuts down the underlying Ouroboros connection.
func (c *NodeClient) Close() error {
	if c == nil {
		return nil
	}
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
