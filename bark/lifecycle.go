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
	"errors"
	"time"

	"connectrpc.com/connect"
	lifecyclev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/lifecycle"
	lifecycleconnect "github.com/blinklabs-io/bark/proto/v1alpha1/lifecycle/lifecyclev1alpha1connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NodeControl is the node-side half of LifecycleService: bark owns the
// transport, authentication and request validation, the node owns the
// shutdown itself.
type NodeControl interface {
	// RequestShutdown accepts a graceful stop, or a stop followed by
	// re-execution when restart is true. A zero timeout selects the node's
	// default. It returns the timeout and deadline actually enforced, or
	// ErrShutdownInProgress / ErrRestartUnsupported.
	RequestShutdown(
		restart bool,
		timeout time.Duration,
	) (time.Duration, time.Time, error)
	// LifecycleStatus reports the node's lifecycle state, health, uptime,
	// version and chain synchronization state.
	LifecycleStatus() *lifecyclev1alpha1.GetStatusResponse
}

var (
	// ErrShutdownInProgress is returned by NodeControl.RequestShutdown when a
	// stop or restart has already been accepted.
	ErrShutdownInProgress = errors.New(
		"bark: a stop or restart is already in progress",
	)
	// ErrRestartUnsupported is returned by NodeControl.RequestShutdown for a
	// restart on a platform that cannot re-execute the process in place.
	ErrRestartUnsupported = errors.New(
		"bark: restart is not supported on this platform",
	)
)

// destructiveLifecycleProcedures can take the node offline, so they require
// an allowlisted operator certificate. readOnlyLifecycleProcedures need only
// a verified one. A procedure in neither is treated as destructive by
// newOperatorAuthInterceptor.
var destructiveLifecycleProcedures = map[string]bool{
	lifecycleconnect.LifecycleServiceStopProcedure:    true,
	lifecycleconnect.LifecycleServiceRestartProcedure: true,
}

var readOnlyLifecycleProcedures = map[string]bool{
	lifecycleconnect.LifecycleServiceGetStatusProcedure: true,
}

type lifecycleServiceHandler struct{ node NodeControl }

var _ lifecycleconnect.LifecycleServiceHandler = (*lifecycleServiceHandler)(nil)

// requestShutdown validates the requested timeout and maps the node's
// sentinel errors onto Connect codes.
func (h *lifecycleServiceHandler) requestShutdown(
	restart bool,
	requested *durationpb.Duration,
) (*durationpb.Duration, *timestamppb.Timestamp, error) {
	if requested != nil {
		if err := requested.CheckValid(); err != nil {
			return nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	timeout := requested.AsDuration()
	if timeout < 0 {
		return nil, nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("graceful_timeout must not be negative"),
		)
	}
	effective, deadline, err := h.node.RequestShutdown(restart, timeout)
	switch {
	case errors.Is(err, ErrShutdownInProgress):
		return nil, nil, connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrRestartUnsupported):
		return nil, nil, connect.NewError(connect.CodeUnimplemented, err)
	case err != nil:
		return nil, nil, connect.NewError(connect.CodeInternal, err)
	}
	return durationpb.New(effective), timestamppb.New(deadline), nil
}

func (h *lifecycleServiceHandler) Stop(
	_ context.Context,
	req *connect.Request[lifecyclev1alpha1.StopRequest],
) (*connect.Response[lifecyclev1alpha1.StopResponse], error) {
	effective, deadline, err := h.requestShutdown(
		false,
		req.Msg.GetGracefulTimeout(),
	)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lifecyclev1alpha1.StopResponse{
		EffectiveTimeout: effective,
		Deadline:         deadline,
	}), nil
}

func (h *lifecycleServiceHandler) Restart(
	_ context.Context,
	req *connect.Request[lifecyclev1alpha1.RestartRequest],
) (*connect.Response[lifecyclev1alpha1.RestartResponse], error) {
	effective, deadline, err := h.requestShutdown(
		true,
		req.Msg.GetGracefulTimeout(),
	)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lifecyclev1alpha1.RestartResponse{
		EffectiveTimeout: effective,
		Deadline:         deadline,
	}), nil
}

func (h *lifecycleServiceHandler) GetStatus(
	context.Context,
	*connect.Request[lifecyclev1alpha1.GetStatusRequest],
) (*connect.Response[lifecyclev1alpha1.GetStatusResponse], error) {
	return connect.NewResponse(h.node.LifecycleStatus()), nil
}
