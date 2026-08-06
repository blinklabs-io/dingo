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
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
)

// serverMetrics holds the Prometheus instruments for the MidnightState gRPC
// server.
type serverMetrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
}

// newServerMetrics registers the server's request counters against reg. reg
// may be nil (promauto skips registration but still returns usable
// instruments), or it may be node.go's rebuildableRegisterer wrapper -- a
// live restore/truncate reconstructs the server via New, and
// node_lifecycle.go's unregisterAll() runs first, so the re-registration
// here never collides with the previous instance's collectors (see
// metrics_registerer.go).
func newServerMetrics(reg prometheus.Registerer) *serverMetrics {
	factory := promauto.With(reg)
	return &serverMetrics{
		requestsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dingo_midnight_grpc_requests_total",
			Help: "total MidnightState gRPC requests, by method",
		}, []string{"method"}),
		requestDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "dingo_midnight_grpc_request_duration_seconds",
			Help: "MidnightState gRPC request latency, by method",
		}, []string{"method"}),
	}
}

// unaryInterceptor records request count and latency for every unary
// MidnightState RPC (the service has no streaming methods).
func (m *serverMetrics) unaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	method := path.Base(info.FullMethod)
	start := time.Now()
	resp, err := handler(ctx, req)
	m.requestsTotal.WithLabelValues(method).Inc()
	m.requestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	return resp, err
}
