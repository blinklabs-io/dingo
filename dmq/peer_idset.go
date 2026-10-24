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

package dmq

import "sync"

// idSet is a bounded set of message IDs. Once full, adding an ID forgets the
// oldest one, so the set follows recent traffic without growing with it.
type idSet struct {
	mu   sync.Mutex
	max  int
	set  map[[32]byte]struct{}
	ring [][32]byte
	next int
}

func newIDSet(maxIDs int) *idSet {
	return &idSet{
		max: max(maxIDs, 1),
		set: make(map[[32]byte]struct{}),
	}
}

// idKey converts a message ID to a map key. It reports false for anything
// that is not a 32-byte ID.
func idKey(id []byte) ([32]byte, bool) {
	var key [32]byte
	if len(id) != len(key) {
		return key, false
	}
	copy(key[:], id)
	return key, true
}

func (s *idSet) add(id []byte) {
	key, ok := idKey(id)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.set[key]; dup {
		return
	}
	if len(s.ring) < s.max {
		s.ring = append(s.ring, key)
	} else {
		delete(s.set, s.ring[s.next])
		s.ring[s.next] = key
		s.next = (s.next + 1) % s.max
	}
	s.set[key] = struct{}{}
}

func (s *idSet) has(id []byte) bool {
	key, ok := idKey(id)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found := s.set[key]
	return found
}
