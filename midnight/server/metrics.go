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
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/midnight"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
)

// midnightStateMethodPrefix matches info.FullMethod for MidnightState's own
// RPCs. Start's grpc.UnaryInterceptor wraps every *unary* call on the
// *grpc.Server, and the health service registered alongside MidnightState
// (see Start) has one unary method, Check, that reaches this same
// interceptor chain -- so without this filter a health probe would show up
// in these metrics as if it were MidnightState API traffic. Reflection's
// ServerReflectionInfo is bidirectional-streaming, not unary, so it never
// reaches grpc.UnaryInterceptor at all regardless of this filter; only
// health's Check needed excluding here.
var midnightStateMethodPrefix = "/" + midnight.MidnightState_ServiceDesc.ServiceName + "/"

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
// MidnightState RPC (the service has no streaming methods). The health
// service's unary Check method shares this interceptor chain but is passed
// straight to handler, unrecorded -- see midnightStateMethodPrefix.
func (m *serverMetrics) unaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if !strings.HasPrefix(info.FullMethod, midnightStateMethodPrefix) {
		return handler(ctx, req)
	}
	method := path.Base(info.FullMethod)
	start := time.Now()
	resp, err := handler(ctx, req)
	m.requestsTotal.WithLabelValues(method).Inc()
	m.requestDuration.WithLabelValues(method).
		Observe(time.Since(start).Seconds())
	return resp, err
}
