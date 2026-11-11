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

package chainsync

import (
	"sync"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// promotionWaits releases waits registered for a tracked client when it moves
// from observability-only to an eligible slot. State fires it from the one
// method every promotion goes through, so a promotion no caller of the
// ouroboros package made, such as a root handed a slot another client freed,
// still reaches a roll-forward callback the connection parked as an observer.
type promotionWaits struct {
	mu    sync.Mutex
	waits map[ouroboros.ConnectionId]chan struct{}
}

func (w *promotionWaits) register(
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

func (w *promotionWaits) wake(connId ouroboros.ConnectionId) {
	w.mu.Lock()
	ch, ok := w.waits[connId]
	delete(w.waits, connId)
	w.mu.Unlock()
	if ok {
		close(ch)
	}
}

// AwaitClientPromotion returns a channel that is closed when connId moves
// from observability-only to an eligible slot, and the function that releases
// the registration. A connection runs one roll-forward callback at a time, so
// a later registration replaces an earlier one. The caller must check the
// client's role after registering: a promotion that landed before the
// registration closes nothing.
func (s *State) AwaitClientPromotion(
	connId ouroboros.ConnectionId,
) (<-chan struct{}, func()) {
	return s.promotionWaits.register(connId)
}
