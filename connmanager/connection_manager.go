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
	"io"
	"log/slog"
	"sync"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// ConnectionManagerConnClosedFunc is a function that takes a connection ID and an optional error
type ConnectionManagerConnClosedFunc func(ouroboros.ConnectionId, error)

type ConnectionManager struct {
	config           ConnectionManagerConfig
	connections      map[ouroboros.ConnectionId]*ouroboros.Connection
	connectionsMutex sync.Mutex
}

type ConnectionManagerConfig struct {
	Logger             *slog.Logger
	EventBus           *event.EventBus
	ConnClosedFunc     ConnectionManagerConnClosedFunc
	Listeners          []ListenerConfig
	OutboundConnOpts   []ouroboros.ConnectionOptionFunc
	OutboundSourcePort uint
}

func NewConnectionManager(cfg ConnectionManagerConfig) *ConnectionManager {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "connmanager")
	return &ConnectionManager{
		config: cfg,
		connections: make(
			map[ouroboros.ConnectionId]*ouroboros.Connection,
		),
	}
}

func (c *ConnectionManager) Start() error {
	if err := c.startListeners(); err != nil {
		return err
	}
	return nil
}

func (c *ConnectionManager) AddConnection(conn *ouroboros.Connection) {
	connId := conn.Id()
	c.connectionsMutex.Lock()
	c.connections[connId] = conn
	c.connectionsMutex.Unlock()
	go func() {
		err := <-conn.ErrorChan()
		// Remove connection
		c.RemoveConnection(connId)
		// Generate event
		if c.config.EventBus != nil {
			c.config.EventBus.Publish(
				ConnectionClosedEventType,
				event.NewEvent(
					ConnectionClosedEventType,
					ConnectionClosedEvent{
						ConnectionId: connId,
						Error:        err,
					},
				),
			)
		}
		// Call configured connection closed callback func
		if c.config.ConnClosedFunc != nil {
			c.config.ConnClosedFunc(connId, err)
		}
	}()
}

func (c *ConnectionManager) RemoveConnection(connId ouroboros.ConnectionId) {
	c.connectionsMutex.Lock()
	delete(c.connections, connId)
	c.connectionsMutex.Unlock()
}

func (c *ConnectionManager) GetConnectionById(
	connId ouroboros.ConnectionId,
) *ouroboros.Connection {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	return c.connections[connId]
}
