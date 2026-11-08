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

package kesagent

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// FailoverClient signs through an ordered list of remote KES agents, one at
// a time. It starts with the first address. A sign that fails for any reason
// other than the agent refusing it moves the client to the next address for
// the next attempt; the failed sign itself is never retried on another agent,
// so one sign reaches one agent within its SignTimeout. Prepare, called
// shortly before each leader slot, admits the current agent, moving on in
// order to the first one that admits this node, and then admits the next one
// in the background, so a move at the slot finds that agent's session open.
// It is safe for concurrent use.
type FailoverClient struct {
	clients   []*RemoteClient
	addresses []string
	logger    *slog.Logger
	metrics   *Metrics

	mu     sync.Mutex
	active int
}

// NewFailoverClient builds one RemoteClient per address from cfg, whose
// Address is ignored. It does not dial.
func NewFailoverClient(
	addresses []string,
	cfg RemoteConfig,
) (*FailoverClient, error) {
	if len(addresses) == 0 {
		return nil, errors.New("kesagent: remote agent address list is empty")
	}
	f := &FailoverClient{
		clients:   make([]*RemoteClient, 0, len(addresses)),
		addresses: append([]string(nil), addresses...),
		logger:    cfg.Logger,
		metrics:   cfg.Metrics,
	}
	if f.logger == nil {
		f.logger = slog.Default()
	}
	for _, address := range addresses {
		c := cfg
		c.Address = address
		client, err := NewRemoteClient(c)
		if err != nil {
			for _, built := range f.clients {
				_ = built.Close()
			}
			return nil, err
		}
		f.clients = append(f.clients, client)
	}
	f.logger.Info(
		"KES agent link: remote agents in order",
		"component", "kesagent",
		"addresses", f.addresses,
		"active", f.addresses[0],
	)
	f.metrics.setActiveAgent(f.addresses, 0)
	return f, nil
}

// Active returns the address the next sign goes to.
func (f *FailoverClient) Active() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addresses[f.active]
}

func (f *FailoverClient) current() (int, *RemoteClient) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active, f.clients[f.active]
}

// moveFrom makes the address after from the active one, unless another
// caller already moved away from from. reason is logged.
func (f *FailoverClient) moveFrom(from int, reason error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active != from {
		return
	}
	f.setActiveLocked((from+1)%len(f.clients), reason)
}

func (f *FailoverClient) setActiveLocked(to int, reason error) {
	if to == f.active {
		return
	}
	from := f.active
	f.active = to
	f.metrics.incAgentFailover()
	f.metrics.setActiveAgent(f.addresses, to)
	f.logger.Info(
		"KES agent link: moving to the next remote agent",
		"component", "kesagent",
		"from", f.addresses[from],
		"to", f.addresses[to],
		"reason", reason,
	)
}

// Sign signs through the active agent. A failure other than an agent
// refusal moves the client to the next address for the next attempt.
func (f *FailoverClient) Sign(period uint64, message []byte) ([]byte, error) {
	idx, client := f.current()
	sig, err := client.Sign(period, message)
	if err != nil && len(f.clients) > 1 &&
		!errors.Is(err, ErrAgentRefused) && !errors.Is(err, ErrClosed) {
		f.moveFrom(idx, err)
	}
	return sig, err
}

// Prepare admits the active agent, or the first agent after it in order
// that admits this node, which then becomes the active one. It then admits
// the next agent in the background, so a later move needs no new session at
// the slot.
func (f *FailoverClient) Prepare() error {
	start, _ := f.current()
	n := len(f.clients)
	var errs []error
	for i := range n {
		idx := (start + i) % n
		err := f.clients[idx].Prepare()
		if err == nil {
			if idx != start {
				f.mu.Lock()
				f.setActiveLocked(idx, errors.Join(errs...))
				f.mu.Unlock()
			}
			f.prepareStandby(idx)
			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", f.addresses[idx], err))
	}
	return errors.Join(errs...)
}

// prepareStandby admits the agent after idx in the background. Its result is
// only logged: the active agent is what this slot uses.
func (f *FailoverClient) prepareStandby(idx int) {
	if len(f.clients) < 2 {
		return
	}
	next := (idx + 1) % len(f.clients)
	standby, address := f.clients[next], f.addresses[next]
	go func() {
		if err := standby.Prepare(); err != nil {
			f.logger.Debug(
				"standby remote KES agent not admitted",
				"component", "kesagent",
				"address", address,
				"error", err,
			)
		}
	}()
}

// CheckReady reports nil when any agent was reachable and admitted this node
// on its last attempt. It never waits on the network; each agent's
// CheckReady starts its own background dial when it has no fresh session.
func (f *FailoverClient) CheckReady() error {
	start, _ := f.current()
	n := len(f.clients)
	var errs []error
	for i := range n {
		idx := (start + i) % n
		err := f.clients[idx].CheckReady()
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrClosed) {
			return err
		}
		errs = append(errs, fmt.Errorf("%s: %w", f.addresses[idx], err))
	}
	return errors.Join(errs...)
}

// Close closes every agent's session. It is safe to call more than once.
func (f *FailoverClient) Close() error {
	for _, c := range f.clients {
		_ = c.Close()
	}
	return nil
}
