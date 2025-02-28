// Copyright 2024 Blink Labs Software
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

package connmanager

import (
	"context"
	"fmt"
	"net"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

func (c *ConnectionManager) CreateOutboundConn(
	address string,
) (*ouroboros.Connection, error) {
	t := otel.Tracer("")
	if t != nil {
		_, span := t.Start(context.TODO(), "create outbound connection")
		defer span.End()
		span.SetAttributes(
			attribute.String("peer.address", address),
		)
	}

	var clientAddr net.Addr
	dialer := net.Dialer{
		Timeout: 10 * time.Second,
	}
	if c.config.OutboundSourcePort > 0 {
		// Setup connection to use our listening port as the source port
		// This is required for peer sharing to be useful
		clientAddr, _ = net.ResolveTCPAddr(
			"tcp",
			fmt.Sprintf(":%d", c.config.OutboundSourcePort),
		)
		dialer.LocalAddr = clientAddr
		dialer.Control = socketControl
	}
	c.config.Logger.Debug(
		"establishing TCP connection to: "+address,
		"role", "client",
	)
	tmpConn, err := dialer.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	// Build connection options
	connOpts := []ouroboros.ConnectionOptionFunc{
		ouroboros.WithConnection(tmpConn),
		ouroboros.WithLogger(c.config.Logger),
	}
	connOpts = append(
		connOpts,
		c.config.OutboundConnOpts...,
	)
	// Setup Ouroboros connection
	c.config.Logger.Debug(
		"establishing ouroboros protocol to "+address,
		"role", "client",
	)
	oConn, err := ouroboros.NewConnection(
		connOpts...,
	)
	if err != nil {
		return nil, err
	}
	c.config.Logger.Info(
		"connected ouroboros to "+address,
		"role", "client",
	)
	c.config.Logger.Debug(
		"peer address mapping: address: "+address,
		"role", "client",
		"connection_id", oConn.Id().String(),
	)
	c.AddConnection(oConn)
	return oConn, nil
}
