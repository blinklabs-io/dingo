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

package testutil

import (
	"runtime"
	"strings"
)

// initialGoroutineDumpBytes is only a starting size. AllGoroutineStacks grows
// from here until the dump fits, so this value affects allocation count and
// never correctness.
const initialGoroutineDumpBytes = 1 << 16

// AllGoroutineStacks returns the runtime's complete dump of every goroutine
// stack.
//
// runtime.Stack silently truncates to the buffer it is given, so a fixed-size
// buffer makes a stack-scanning assertion depend on how many goroutines happen
// to be alive. A test that passes under -run and fails in a full parallel
// package run is the usual symptom: the frame the condition looks for is past
// the cut. Sizing the buffer by hand only moves the threshold, so this grows
// until runtime.Stack reports it wrote less than the buffer holds, which is
// how it signals a complete dump.
func AllGoroutineStacks() string {
	buf := make([]byte, initialGoroutineDumpBytes)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// GoroutineParkedIn reports whether some goroutine's stack contains every one
// of frames. All frames must appear in the same goroutine's stack, so callers
// can pin both the parking primitive and the call path that reached it rather
// than matching either alone anywhere in the process.
func GoroutineParkedIn(frames ...string) bool {
	if len(frames) == 0 {
		return false
	}
	for stack := range strings.SplitSeq(AllGoroutineStacks(), "\n\ngoroutine ") {
		matched := true
		for _, frame := range frames {
			if !strings.Contains(stack, frame) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
