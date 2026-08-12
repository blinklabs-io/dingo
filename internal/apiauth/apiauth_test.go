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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewVerifierDisabled(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{})
	require.NoError(t, err)
	assert.Nil(t, v)
	// A nil Verifier always succeeds -- this is the documented
	// "authentication disabled" behavior.
	assert.True(t, v.Verify("anything"))
	assert.True(t, v.Verify(""))
}

func TestNewVerifierInlineToken(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{
		Enabled: true,
		Token:   "shared-secret",
	})
	require.NoError(t, err)
	require.NotNil(t, v)
	assert.True(t, v.Verify("shared-secret"))
	assert.False(t, v.Verify("wrong"))
	assert.False(t, v.Verify(""))
}

func TestNewVerifierTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("file-secret\n"), 0o600))
	v, err := NewVerifier(apiconfig.EffectiveAuth{
		Enabled:       true,
		TokenFilePath: path,
	})
	require.NoError(t, err)
	require.NotNil(t, v)
	// Trailing whitespace/newline in the file must be trimmed.
	assert.True(t, v.Verify("file-secret"))
}

func TestNewVerifierMissingTokenFile(t *testing.T) {
	_, err := NewVerifier(apiconfig.EffectiveAuth{
		Enabled:       true,
		TokenFilePath: filepath.Join(t.TempDir(), "missing"),
	})
	require.Error(t, err)
}

func TestNewVerifierEmptyTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("  \n"), 0o600))
	_, err := NewVerifier(apiconfig.EffectiveAuth{
		Enabled:       true,
		TokenFilePath: path,
	})
	require.Error(t, err)
}

func TestBearerToken(t *testing.T) {
	assert.Equal(t, "abc123", bearerToken("Bearer abc123"))
	assert.Equal(t, "abc123", bearerToken("bearer abc123"))
	assert.Equal(t, "", bearerToken(""))
	assert.Equal(t, "", bearerToken("Basic abc123"))
	assert.Equal(t, "", bearerToken("Bearer"))
}

func TestMiddlewareNilVerifierIsNoOp(t *testing.T) {
	called := false
	handler := Middleware(nil)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { called = true },
	))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.True(t, called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestMiddlewareRejectsMissingCredential(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{Enabled: true, Token: "t"})
	require.NoError(t, err)
	called := false
	handler := Middleware(v)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { called = true },
	))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.False(t, called)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("WWW-Authenticate"))
}

func TestMiddlewareAcceptsBearerCredential(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{Enabled: true, Token: "t"})
	require.NoError(t, err)
	called := false
	handler := Middleware(v)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { called = true },
	))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.True(t, called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestMiddlewareRejectsWrongBearerCredential(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{Enabled: true, Token: "t"})
	require.NoError(t, err)
	handler := Middleware(v)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {},
	))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestMiddlewareAliasHeaderBlockfrostProjectId(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{Enabled: true, Token: "t"})
	require.NoError(t, err)
	called := false
	handler := Middleware(v, WithAliasHeader("project_id"))(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { called = true },
	))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("project_id", "t")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.True(t, called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestMiddlewareBearerTakesPrecedenceOverAlias(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{Enabled: true, Token: "t"})
	require.NoError(t, err)
	handler := Middleware(v, WithAliasHeader("project_id"))(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {},
	))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer t")
	req.Header.Set("project_id", "wrong")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestConnectInterceptorNilVerifierIsNoOp(t *testing.T) {
	interceptor := Interceptor(nil)
	req := connect.NewRequest(&struct{}{})
	called := false
	unary := interceptor.WrapUnary(
		func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
			called = true
			return connect.NewResponse(&struct{}{}), nil
		},
	)
	_, err := unary(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, called)
}

func TestConnectInterceptorRejectsMissingCredential(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{Enabled: true, Token: "t"})
	require.NoError(t, err)
	interceptor := Interceptor(v)
	req := connect.NewRequest(&struct{}{})
	called := false
	unary := interceptor.WrapUnary(
		func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
			called = true
			return connect.NewResponse(&struct{}{}), nil
		},
	)
	_, err = unary(context.Background(), req)
	require.Error(t, err)
	assert.False(t, called)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeUnauthenticated, connectErr.Code())
}

func TestConnectInterceptorAcceptsValidCredential(t *testing.T) {
	v, err := NewVerifier(apiconfig.EffectiveAuth{Enabled: true, Token: "t"})
	require.NoError(t, err)
	interceptor := Interceptor(v)
	req := connect.NewRequest(&struct{}{})
	req.Header().Set("Authorization", "Bearer t")
	called := false
	unary := interceptor.WrapUnary(
		func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
			called = true
			return connect.NewResponse(&struct{}{}), nil
		},
	)
	_, err = unary(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, called)
}

func TestConnectInterceptorWrapStreamingClientIsPassthrough(t *testing.T) {
	interceptor := Interceptor(nil)
	called := false
	client := interceptor.WrapStreamingClient(
		func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
			called = true
			return nil
		},
	)
	client(context.Background(), connect.Spec{})
	assert.True(t, called)
}
