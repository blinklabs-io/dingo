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

package connmanager

import (
	"log/slog"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

type trustedNtCActivity struct {
	mu               sync.Mutex
	conn             *handshakeDeadlineConn
	timeout          time.Duration
	active           int
	writes           map[uint16]uint64
	pendingResponses map[uint16]uint64
	logger           *slog.Logger
}

type trustedNtCActivityToken struct {
	activity   *trustedNtCActivity
	protocolID uint16
	writeCount uint64
	once       sync.Once
}

func (a *trustedNtCActivity) attach(
	conn *handshakeDeadlineConn,
	timeout time.Duration,
	logger *slog.Logger,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.conn = conn
	a.timeout = timeout
	a.logger = logger
	for protocolID, count := range conn.attachActivity(a) {
		a.writes[protocolID] += count
		pending := a.pendingResponses[protocolID]
		if count >= pending {
			delete(a.pendingResponses, protocolID)
		} else {
			a.pendingResponses[protocolID] = pending - count
		}
	}
	return a.updateDeadlineLocked()
}

func (a *trustedNtCActivity) begin(
	protocolID uint16,
) *trustedNtCActivityToken {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active++
	token := &trustedNtCActivityToken{
		activity:   a,
		protocolID: protocolID,
		writeCount: a.writes[protocolID],
	}
	a.applyDeadlineLocked()
	return token
}

func (t *trustedNtCActivityToken) end(expectResponse bool) {
	t.once.Do(func() {
		a := t.activity
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.active > 0 {
			a.active--
		}
		if expectResponse && a.writes[t.protocolID] == t.writeCount {
			a.pendingResponses[t.protocolID]++
		}
		a.applyDeadlineLocked()
	})
}

func (a *trustedNtCActivity) readProgress() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applyDeadlineLocked()
}

func (a *trustedNtCActivity) responseProgress(protocolID uint16) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.writes[protocolID]++
	if pending := a.pendingResponses[protocolID]; pending > 1 {
		a.pendingResponses[protocolID] = pending - 1
	} else if pending == 1 {
		delete(a.pendingResponses, protocolID)
	}
	a.applyDeadlineLocked()
}

func (a *trustedNtCActivity) applyDeadlineLocked() {
	if err := a.updateDeadlineLocked(); err != nil {
		a.logger.Warn(
			"trusted NtC deadline update failed",
			"error",
			err,
		)
		closeConnAndLog(
			a.logger,
			a.conn,
			"trusted NtC close after deadline failure failed",
		)
	}
}

func (a *trustedNtCActivity) updateDeadlineLocked() error {
	if a.conn == nil {
		return nil
	}
	deadline := time.Time{}
	if a.active == 0 && len(a.pendingResponses) == 0 {
		deadline = time.Now().Add(a.timeout)
	}
	return a.conn.setReadDeadlineCap(deadline)
}

// BeginTrustedNtCActivity suspends the trusted local NtC idle deadline while
// one server mini-protocol request is active. The returned function completes
// that request and must be called exactly once.
func (c *ConnectionManager) BeginTrustedNtCActivity(
	connID ouroboros.ConnectionId,
	protocolID uint16,
) func() {
	return c.beginTrustedNtCActivity(connID, protocolID, true)
}

// BeginTrustedNtCOneWayActivity suspends the idle deadline while handling a
// request that has no protocol response, then re-arms it when work completes.
func (c *ConnectionManager) BeginTrustedNtCOneWayActivity(
	connID ouroboros.ConnectionId,
	protocolID uint16,
) func() {
	return c.beginTrustedNtCActivity(connID, protocolID, false)
}

func (c *ConnectionManager) beginTrustedNtCActivity(
	connID ouroboros.ConnectionId,
	protocolID uint16,
	expectResponse bool,
) func() {
	c.trustedNtCActivityMutex.Lock()
	if _, closed := c.trustedNtCClosedActivities[connID]; closed {
		c.trustedNtCActivityMutex.Unlock()
		return func() {}
	}
	activity := c.trustedNtCActivities[connID]
	if activity == nil {
		activity = &trustedNtCActivity{
			writes:           make(map[uint16]uint64),
			pendingResponses: make(map[uint16]uint64),
		}
		c.trustedNtCActivities[connID] = activity
	}
	c.trustedNtCActivityMutex.Unlock()
	token := activity.begin(protocolID)
	return func() { token.end(expectResponse) }
}

func (c *ConnectionManager) registerTrustedNtCActivity(
	connID ouroboros.ConnectionId,
	owner *ouroboros.Connection,
	conn *handshakeDeadlineConn,
	timeout time.Duration,
	logger *slog.Logger,
) error {
	c.trustedNtCActivityMutex.Lock()
	delete(c.trustedNtCClosedActivities, connID)
	activity := c.trustedNtCActivities[connID]
	if activity == nil {
		activity = &trustedNtCActivity{
			writes:           make(map[uint16]uint64),
			pendingResponses: make(map[uint16]uint64),
		}
		c.trustedNtCActivities[connID] = activity
	}
	c.trustedNtCActivityOwners[connID] = owner
	c.trustedNtCActivityMutex.Unlock()
	return activity.attach(conn, timeout, logger)
}

func (c *ConnectionManager) unregisterTrustedNtCActivity(
	connID ouroboros.ConnectionId,
	owner *ouroboros.Connection,
) {
	c.trustedNtCActivityMutex.Lock()
	defer c.trustedNtCActivityMutex.Unlock()
	if c.trustedNtCActivityOwners[connID] != owner {
		return
	}
	delete(c.trustedNtCActivityOwners, connID)
	delete(c.trustedNtCActivities, connID)
	c.trustedNtCClosedActivities[connID] = owner
}

func (c *ConnectionManager) forgetClosedTrustedNtCActivity(
	connID ouroboros.ConnectionId,
	owner *ouroboros.Connection,
) {
	c.trustedNtCActivityMutex.Lock()
	defer c.trustedNtCActivityMutex.Unlock()
	if c.trustedNtCClosedActivities[connID] == owner {
		delete(c.trustedNtCClosedActivities, connID)
	}
}
