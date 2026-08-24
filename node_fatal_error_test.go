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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCancelForFatalMakesShutdownReturnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{ctx: ctx, cancel: cancel}
	want := errors.New("strict parity mismatch")

	n.cancelForFatal(want)

	require.ErrorIs(t, n.waitForShutdown(), want)
}

func TestParentCancellationRemainsCleanShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{ctx: ctx, cancel: cancel}

	cancel()

	require.NoError(t, n.waitForShutdown())
}

func TestFatalDuringStartupOverridesCancellationError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{ctx: ctx, cancel: cancel}
	want := errors.New("strict parity mismatch during startup")

	n.cancelForFatal(want)

	require.ErrorIs(t, n.resolveRunError(context.Canceled), want)
}
