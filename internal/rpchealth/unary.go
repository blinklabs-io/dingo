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

// Package rpchealth restricts gRPC health services to bounded unary calls.
package rpchealth

import (
	"context"

	"connectrpc.com/grpchealth"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type unaryChecker struct {
	grpchealth.Checker
}

// UnaryChecker hides a checker's optional Watch method, so grpchealth rejects
// Health/Watch with Unimplemented instead of retaining a stream until the
// client disconnects.
func UnaryChecker(checker grpchealth.Checker) grpchealth.Checker {
	return unaryChecker{Checker: checker}
}

type unaryServer struct {
	healthpb.UnimplementedHealthServer
	delegate healthpb.HealthServer
}

// UnaryServer delegates Check and List while leaving Watch unimplemented.
func UnaryServer(server healthpb.HealthServer) healthpb.HealthServer {
	return &unaryServer{delegate: server}
}

func (s *unaryServer) Check(
	ctx context.Context,
	req *healthpb.HealthCheckRequest,
) (*healthpb.HealthCheckResponse, error) {
	return s.delegate.Check(ctx, req)
}

func (s *unaryServer) List(
	ctx context.Context,
	req *healthpb.HealthListRequest,
) (*healthpb.HealthListResponse, error) {
	return s.delegate.List(ctx, req)
}
