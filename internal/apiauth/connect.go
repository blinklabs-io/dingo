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

package apiauth

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"
)

// errInvalidCredential is returned to a Connect/gRPC caller as
// connect.CodeUnauthenticated. It never embeds the presented or expected
// credential.
var errInvalidCredential = errors.New(
	"missing or invalid Authorization credential",
)

// Interceptor returns a connect.Interceptor that requires every unary and
// streaming call to present a credential verifier accepts in the standard
// "Authorization: Bearer <token>" request header, failing closed with
// connect.CodeUnauthenticated otherwise. A nil verifier (authentication
// disabled) returns a no-op interceptor -- Interceptor(nil) is a
// documented no-op, matching Verifier's own "nil means disabled" and
// Middleware's own contract.
//
// This is the same Verifier.Verify credential check http.go's Middleware
// uses; only the transport this file reads the credential from (Connect/
// gRPC request headers instead of a net/http request's headers) differs.
func Interceptor(verifier *Verifier) connect.Interceptor {
	return &connectInterceptor{verifier: verifier}
}

type connectInterceptor struct {
	verifier *Verifier
}

func (i *connectInterceptor) authorize(header http.Header) error {
	if i.verifier == nil {
		return nil
	}
	credential := bearerToken(header.Get("Authorization"))
	if !i.verifier.Verify(credential) {
		return connect.NewError(
			connect.CodeUnauthenticated,
			errInvalidCredential,
		)
	}
	return nil
}

// WrapUnary authenticates every unary call before it reaches the real
// handler.
func (i *connectInterceptor) WrapUnary(
	next connect.UnaryFunc,
) connect.UnaryFunc {
	return func(
		ctx context.Context,
		req connect.AnyRequest,
	) (connect.AnyResponse, error) {
		if err := i.authorize(req.Header()); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// WrapStreamingClient is a pass-through no-op: it governs outgoing calls
// this process makes as a Connect client, which this interceptor is never
// installed on -- API providers only use it server-side, to gate incoming
// requests.
func (i *connectInterceptor) WrapStreamingClient(
	next connect.StreamingClientFunc,
) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler is WrapUnary's server-streaming counterpart.
func (i *connectInterceptor) WrapStreamingHandler(
	next connect.StreamingHandlerFunc,
) connect.StreamingHandlerFunc {
	return func(
		ctx context.Context,
		conn connect.StreamingHandlerConn,
	) error {
		if err := i.authorize(conn.RequestHeader()); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}
