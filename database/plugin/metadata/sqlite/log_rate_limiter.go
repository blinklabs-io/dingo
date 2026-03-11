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

package sqlite

import (
	"log/slog"
	"sync"
	"time"
)

const repeatedWarnInterval = 30 * time.Second

type repeatedWarnState struct {
	lastLog    time.Time
	suppressed uint64
}

type repeatedWarnLimiter struct {
	mu     sync.Mutex
	states map[string]*repeatedWarnState
}

func newRepeatedWarnLimiter() *repeatedWarnLimiter {
	return &repeatedWarnLimiter{
		states: make(map[string]*repeatedWarnState),
	}
}

func (l *repeatedWarnLimiter) warn(
	logger *slog.Logger,
	key string,
	msg string,
	args ...any,
) {
	if logger == nil {
		return
	}

	now := time.Now()

	l.mu.Lock()
	state, ok := l.states[key]
	if !ok {
		state = &repeatedWarnState{}
		l.states[key] = state
	}
	if !state.lastLog.IsZero() && now.Sub(state.lastLog) < repeatedWarnInterval {
		state.suppressed++
		l.mu.Unlock()
		return
	}
	suppressed := state.suppressed
	state.lastLog = now
	state.suppressed = 0
	l.mu.Unlock()

	if suppressed > 0 {
		args = append(args, "suppressed_count", suppressed)
	}
	logger.Warn(msg, args...)
}
