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

package ouroboros

import (
	"errors"
	"sync"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// errObserverPromoted ends a wait a connection's callback entered as an
// observer once the connection holds an ingress slot.
var errObserverPromoted = errors.New("observer promoted to ingress")

// observerWakes lets a promotion reach the roll-forward callback its
// connection parked as an observer. Such a callback is held on something that
// has nothing to do with the connection's role, so without the wake it would
// keep waiting on the connection's behalf after the connection became the
// header source.
type observerWakes struct {
	mu    sync.Mutex
	waits map[ouroboros.ConnectionId]chan struct{}
}

// register returns a channel closed by wake(connId), and the function that
// releases the registration. A connection runs one callback at a time, so a
// later registration replaces an earlier one.
func (w *observerWakes) register(
	connId ouroboros.ConnectionId,
) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	w.mu.Lock()
	if w.waits == nil {
		w.waits = make(map[ouroboros.ConnectionId]chan struct{})
	}
	w.waits[connId] = ch
	w.mu.Unlock()
	return ch, func() {
		w.mu.Lock()
		if w.waits[connId] == ch {
			delete(w.waits, connId)
		}
		w.mu.Unlock()
	}
}

// wake releases the wait registered for connId, if any.
func (w *observerWakes) wake(connId ouroboros.ConnectionId) {
	w.mu.Lock()
	ch, ok := w.waits[connId]
	delete(w.waits, connId)
	w.mu.Unlock()
	if ok {
		close(ch)
	}
}
