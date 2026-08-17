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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/config"
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

// A bind failure on a non-essential observability listener (metrics or pprof)
// must be logged and non-fatal: it returns instead of blocking, and never
// signals an error that would take down an otherwise-healthy node.
func TestServeAuxiliaryListenerBindFailureIsNonFatal(t *testing.T) {
	t.Parallel()

	// Occupy a port so the auxiliary listener cannot bind.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy port: %s", err)
	}
	defer occupied.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	srv := &http.Server{
		Addr:              occupied.Addr().String(),
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: time.Second,
	}

	done := make(chan struct{})
	go func() {
		serveAuxiliaryListener("metrics", srv, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal(
			"serveAuxiliaryListener did not return on bind failure; " +
				"a non-essential listener must not block or be fatal",
		)
	}
	// Read after the goroutine finished, so no concurrent buffer access.
	if logged := buf.String(); !strings.Contains(logged, "metrics") {
		t.Fatalf(
			"expected a log mentioning the metrics listener, got: %q",
			logged,
		)
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
		nil,
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
	if !strings.Contains(
		err.Error(),
		"metrics server shutdown: metrics failed",
	) {
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
		nil,
		func() error {
			return nil
		},
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("expected nil shutdown error, got %v", err)
	}
}

// TestBuildDingoConfigWiresAPIConfig asserts that a loaded
// internal/config.Config's api.tls/api.auth policy (as set via YAML/env/CLI)
// actually reaches the dingo.Config that Run() hands to dingo.New() --
// regression test for the top-level API security defaults (dingo#2998)
// being silently dropped because Run's real composition call never invoked
// dingo.WithAPIConfig.
func TestBuildDingoConfigWiresAPIConfig(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		API: config.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/shared/cert.pem"),
				KeyFilePath:  new("/shared/key.pem"),
			},
			Auth: apiconfig.AuthPolicy{
				Mode:  new("token"),
				Token: new("shared-secret"),
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	got := built.APIConfig()
	if got.TLS.Mode == nil || *got.TLS.Mode != "server" {
		t.Fatalf("expected api.tls.mode to flow through, got %+v", got.TLS)
	}
	if got.TLS.CertFilePath == nil ||
		*got.TLS.CertFilePath != "/shared/cert.pem" {
		t.Fatalf(
			"expected api.tls.certFilePath to flow through, got %+v",
			got.TLS,
		)
	}
	if got.Auth.Mode == nil || *got.Auth.Mode != "token" {
		t.Fatalf("expected api.auth.mode to flow through, got %+v", got.Auth)
	}
	if got.Auth.Token == nil || *got.Auth.Token != "shared-secret" {
		t.Fatalf(
			"expected api.auth.token to flow through, got %+v",
			got.Auth,
		)
	}
}
