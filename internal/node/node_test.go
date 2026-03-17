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

package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWaitForSignalOrErrorPrefersQueuedError(t *testing.T) {
	t.Parallel()

	signalCtx, signalCtxStop := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	expectedErr := errors.New("metrics server: bind failed")

	errChan <- expectedErr
	signalCtxStop()

	err, signaled := waitForSignalOrError(signalCtx, errChan)
	if signaled {
		t.Fatal("expected queued error to win over signal shutdown")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestWaitForSignalOrErrorReturnsSignalWithoutQueuedError(t *testing.T) {
	t.Parallel()

	signalCtx, signalCtxStop := context.WithCancel(context.Background())
	errChan := make(chan error, 1)

	signalCtxStop()

	err, signaled := waitForSignalOrError(signalCtx, errChan)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !signaled {
		t.Fatal("expected signal shutdown when no error is queued")
	}
}

func TestShutdownNodeResourcesAggregatesErrors(t *testing.T) {
	t.Parallel()

	metricsErr := errors.New("metrics failed")
	nodeErr := errors.New("node failed")

	err := shutdownNodeResources(
		func(context.Context) error {
			return metricsErr
		},
		func() error {
			return nodeErr
		},
		5*time.Second,
	)
	if err == nil {
		t.Fatal("expected shutdown error")
	}
	if !errors.Is(err, metricsErr) {
		t.Fatalf("expected metrics shutdown error to be joined: %v", err)
	}
	if !errors.Is(err, nodeErr) {
		t.Fatalf("expected node stop error to be joined: %v", err)
	}
	if !strings.Contains(err.Error(), "metrics server shutdown: metrics failed") {
		t.Fatalf("expected metrics shutdown context in error: %v", err)
	}
	if !strings.Contains(err.Error(), "node stop: node failed") {
		t.Fatalf("expected node stop context in error: %v", err)
	}
}

func TestShutdownNodeResourcesReturnsNilWithoutErrors(t *testing.T) {
	t.Parallel()

	err := shutdownNodeResources(
		func(context.Context) error {
			return nil
		},
		func() error {
			return nil
		},
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("expected nil shutdown error, got %v", err)
	}
}
