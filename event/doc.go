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

// Package event provides Dingo's EventBus: an in-process publish/
// subscribe primitive that lets components communicate without
// holding references to each other.
//
// Components never call each other directly across package
// boundaries. Instead, they publish typed events to the bus and
// subscribe to event types they care about. This keeps package
// dependencies acyclic and lets tests exercise individual components
// in isolation.
//
// # Publishing
//
//	eventBus.Publish(
//	    chain.ChainForkEventType,
//	    event.NewEvent(chain.ChainForkEventType, chain.ChainForkEvent{...}),
//	)
//
// Use PublishAsync for events that do not need to be delivered
// synchronously with the publisher's call stack.
//
// # Subscribing
//
//	eventBus.SubscribeFunc(chain.ChainForkEventType, func(evt event.Event) {
//	    e, ok := evt.Data.(chain.ChainForkEvent)
//	    if !ok { return }
//	    // handle e
//	})
//
// The bus runs a pool of async worker goroutines (default 4) to
// dispatch subscribers. Subscriber callbacks must be non-blocking; if
// a callback needs to do real work, push it onto its own goroutine.
// A slow subscriber can backpressure the bus and delay delivery of
// unrelated events.
//
// Event type constants live alongside the package that owns the
// event: ChainForkEventType in chain, ChainSwitchEventType in
// chainselection, PeerEligibilityChangedEventType in peergov, etc.
package event
