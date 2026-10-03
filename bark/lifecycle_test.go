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

package bark

import (
	"context"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	lifecyclev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/lifecycle"
	lifecycleconnect "github.com/blinklabs-io/bark/proto/v1alpha1/lifecycle/lifecyclev1alpha1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

type shutdownCall struct {
	restart bool
	timeout time.Duration
}

// fakeNode records the shutdowns bark hands it and answers with a fixed
// result, so a test can tell which RPCs reached the node at all.
type fakeNode struct {
	mu       sync.Mutex
	calls    []shutdownCall
	deadline time.Time
	err      error
}

func (f *fakeNode) RequestShutdown(
	restart bool,
	timeout time.Duration,
) (time.Duration, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, shutdownCall{restart: restart, timeout: timeout})
	if f.err != nil {
		return 0, time.Time{}, f.err
	}
	return 7 * time.Second, f.deadline, nil
}

func (f *fakeNode) LifecycleStatus() *lifecyclev1alpha1.GetStatusResponse {
	return &lifecyclev1alpha1.GetStatusResponse{
		State:   lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_RUNNING,
		Health:  lifecyclev1alpha1.HealthStatus_HEALTH_STATUS_HEALTHY,
		Version: "test-version",
	}
}

func (f *fakeNode) recorded() []shutdownCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]shutdownCall(nil), f.calls...)
}

// TestLifecycleProceduresCoverEveryGeneratedMethod fails when a
// LifecycleService RPC is added to the generated descriptor without being
// classified, so it cannot be served with the wrong authorization stage.
func TestLifecycleProceduresCoverEveryGeneratedMethod(t *testing.T) {
	t.Parallel()

	svc := lifecyclev1alpha1.File_v1alpha1_lifecycle_lifecycle_proto.
		Services().ByName("LifecycleService")
	require.NotNil(t, svc)
	methods := svc.Methods()
	require.Positive(t, methods.Len())
	for i := range methods.Len() {
		procedure := "/" + string(svc.FullName()) + "/" +
			string(methods.Get(i).Name())
		isDestructive := destructiveLifecycleProcedures[procedure]
		isReadOnly := readOnlyLifecycleProcedures[procedure]
		assert.Truef(t, isDestructive != isReadOnly,
			"procedure %q must be classified as exactly one of destructive "+
				"or read-only", procedure)
	}
}

func TestStartRejectsNodeControlWithoutAuthentication(t *testing.T) {
	t.Parallel()

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	_, _, caCertPath := writeTestCA(t)
	base := BarkConfig{
		DB:   newTestDB(t),
		Node: &fakeNode{},
		Host: "127.0.0.1",
	}
	for name, mutate := range map[string]func(*BarkConfig){
		"client CA": func(c *BarkConfig) {
			c.TlsCertFilePath = serverCertPath
			c.TlsKeyFilePath = serverKeyPath
			c.OperatorCertificateFingerprints = []string{
				"0000000000000000000000000000000000000000000000000000000000000000",
			}
		},
		"TLS": func(c *BarkConfig) {
			c.TlsClientCAFilePath = caCertPath
			c.OperatorCertificateFingerprints = []string{
				"0000000000000000000000000000000000000000000000000000000000000000",
			}
		},
		"operator allowlist": func(c *BarkConfig) {
			c.TlsCertFilePath = serverCertPath
			c.TlsKeyFilePath = serverKeyPath
			c.TlsClientCAFilePath = caCertPath
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			b, err := NewBark(cfg)
			require.NoError(t, err)
			require.Error(t, b.Start(t.Context()))
			require.Empty(t, b.Addr())
		})
	}
}

func TestLifecycleServiceOverRealHTTP(t *testing.T) {
	t.Parallel()

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	ca, caKey, caCertPath := writeTestCA(t)
	operatorCert, operatorKey := writeTestClientCert(t, ca, caKey, "operator")
	readerCert, readerKey := writeTestClientCert(t, ca, caKey, "reader")

	node := &fakeNode{deadline: time.Unix(1_700_000_000, 0)}
	b, err := NewBark(BarkConfig{
		DB:                  newTestDB(t),
		Node:                node,
		Host:                "127.0.0.1",
		TlsCertFilePath:     serverCertPath,
		TlsKeyFilePath:      serverKeyPath,
		TlsClientCAFilePath: caCertPath,
		OperatorCertificateFingerprints: []string{
			testCertificateFingerprint(t, operatorCert),
		},
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer func() { _ = b.Stop(context.Background()) }()

	newClient := func(certPath, keyPath string) lifecycleconnect.LifecycleServiceClient {
		return lifecycleconnect.NewLifecycleServiceClient(
			mtlsHTTPClient(t, certPath, keyPath),
			"https://"+b.Addr(),
		)
	}
	ctx := context.Background()

	t.Run("anonymous client is rejected from every RPC", func(t *testing.T) {
		client := newClient("", "")
		_, err := client.GetStatus(ctx, connect.NewRequest(
			&lifecyclev1alpha1.GetStatusRequest{}))
		require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		_, err = client.Stop(ctx, connect.NewRequest(
			&lifecyclev1alpha1.StopRequest{}))
		require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		_, err = client.Restart(ctx, connect.NewRequest(
			&lifecyclev1alpha1.RestartRequest{}))
		require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		require.Empty(t, node.recorded())
	})

	t.Run("authenticated reader may only read status", func(t *testing.T) {
		client := newClient(readerCert, readerKey)
		status, err := client.GetStatus(ctx, connect.NewRequest(
			&lifecyclev1alpha1.GetStatusRequest{}))
		require.NoError(t, err)
		require.Equal(t, "test-version", status.Msg.GetVersion())
		_, err = client.Stop(ctx, connect.NewRequest(
			&lifecyclev1alpha1.StopRequest{}))
		require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
		_, err = client.Restart(ctx, connect.NewRequest(
			&lifecyclev1alpha1.RestartRequest{}))
		require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
		require.Empty(t, node.recorded())
	})

	t.Run("operator stop and restart reach the node", func(t *testing.T) {
		client := newClient(operatorCert, operatorKey)
		stop, err := client.Stop(ctx, connect.NewRequest(
			&lifecyclev1alpha1.StopRequest{
				GracefulTimeout: durationpb.New(3 * time.Second),
			}))
		require.NoError(t, err)
		require.Equal(
			t,
			7*time.Second,
			stop.Msg.GetEffectiveTimeout().AsDuration(),
		)
		require.Equal(
			t,
			node.deadline.Unix(),
			stop.Msg.GetDeadline().AsTime().Unix(),
		)

		restart, err := client.Restart(ctx, connect.NewRequest(
			&lifecyclev1alpha1.RestartRequest{}))
		require.NoError(t, err)
		require.Equal(
			t,
			7*time.Second,
			restart.Msg.GetEffectiveTimeout().AsDuration(),
		)

		require.Equal(t, []shutdownCall{
			{restart: false, timeout: 3 * time.Second},
			{restart: true, timeout: 0},
		}, node.recorded())
	})

	t.Run(
		"invalid and refused requests map to Connect codes",
		func(t *testing.T) {
			client := newClient(operatorCert, operatorKey)
			before := len(node.recorded())
			_, err := client.Stop(ctx, connect.NewRequest(
				&lifecyclev1alpha1.StopRequest{
					GracefulTimeout: durationpb.New(-time.Second),
				}))
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Len(t, node.recorded(), before)

			node.mu.Lock()
			node.err = ErrShutdownInProgress
			node.mu.Unlock()
			_, err = client.Stop(ctx, connect.NewRequest(
				&lifecyclev1alpha1.StopRequest{}))
			require.Equal(
				t,
				connect.CodeFailedPrecondition,
				connect.CodeOf(err),
			)

			node.mu.Lock()
			node.err = ErrRestartUnsupported
			node.mu.Unlock()
			_, err = client.Restart(ctx, connect.NewRequest(
				&lifecyclev1alpha1.RestartRequest{}))
			require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
		},
	)
}
