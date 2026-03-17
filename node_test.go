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

package dingo

import (
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunStallCheckerTickRecoversAndAllowsFutureTicks(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	var ticks atomic.Int32

	assert.NotPanics(t, func() {
		n.runStallCheckerTick(func() {
			ticks.Add(1)
			panic("boom")
		})
	})

	n.runStallCheckerTick(func() {
		ticks.Add(1)
	})

	assert.Equal(t, int32(2), ticks.Load())
}

func TestRunStallCheckerLoopRecoversAndSupportsRestart(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	var attempts atomic.Int32

	assert.NotPanics(t, func() {
		for {
			recovered := n.runStallCheckerLoop(func() {
				if attempts.Add(1) == 1 {
					panic("boom")
				}
			})
			if !recovered {
				return
			}
		}
	})

	assert.Equal(t, int32(2), attempts.Load())
}
