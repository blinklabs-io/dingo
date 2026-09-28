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

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestUnaryInterceptor_RecordsCountAndDurationByMethod verifies a successful
// RPC increments requestsTotal and observes requestDuration, both labelled
// with the bare method name (not the full "/service/Method" path).
func TestUnaryInterceptor_RecordsCountAndDurationByMethod(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newServerMetrics(reg)
	info := &grpc.UnaryServerInfo{
		FullMethod: "/midnight_state.MidnightState/GetLatestBlock",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	resp, err := m.unaryInterceptor(context.Background(), nil, info, handler)

	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			m.requestsTotal.WithLabelValues("GetLatestBlock"),
		),
	)
	assert.Equal(
		t,
		1,
		testutil.CollectAndCount(m.requestDuration),
	)
}

// TestUnaryInterceptor_RecordsFailedRequestsToo verifies a handler error still
// counts as a request under requestsTotal and is propagated back unchanged.
func TestUnaryInterceptor_RecordsFailedRequestsToo(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newServerMetrics(reg)
	info := &grpc.UnaryServerInfo{
		FullMethod: "/midnight_state.MidnightState/GetAssetCreates",
	}
	wantErr := errors.New("boom")
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, wantErr
	}

	_, err := m.unaryInterceptor(context.Background(), nil, info, handler)

	require.ErrorIs(t, err, wantErr)
	// A failed RPC still counts as a request -- the label set has no
	// success/error dimension, matching the issue's requested cardinality.
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			m.requestsTotal.WithLabelValues("GetAssetCreates"),
		),
	)
}

// TestNewServerMetrics_NilRegistryIsSafe verifies the interceptor stays usable
// (no panic) when constructed with a nil Prometheus registry.
func TestNewServerMetrics_NilRegistryIsSafe(t *testing.T) {
	m := newServerMetrics(nil)
	info := &grpc.UnaryServerInfo{
		FullMethod: "/midnight_state.MidnightState/GetEpochNonce",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}
	// Must not panic when there is no registry to register against.
	_, err := m.unaryInterceptor(context.Background(), nil, info, handler)
	require.NoError(t, err)
}
