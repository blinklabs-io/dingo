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
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxConcurrentMidnightRPCs = 16
	midnightRPCTimeout        = 2 * time.Minute
)

func (s *Server) admissionInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if !strings.HasPrefix(info.FullMethod, midnightStateMethodPrefix) {
		return handler(ctx, req)
	}
	select {
	case s.rpcAdmission <- struct{}{}:
		defer func() { <-s.rpcAdmission }()
	default:
		return nil, status.Error(
			codes.ResourceExhausted,
			"Midnight RPC capacity reached",
		)
	}
	ctx, cancel := context.WithTimeout(ctx, midnightRPCTimeout)
	defer cancel()
	return handler(ctx, req)
}
