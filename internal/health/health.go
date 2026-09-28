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

// Package health serves the node's liveness and readiness probes on a
// dedicated listener, separate from the Prometheus metrics listener, the
// pprof debug listener and every API listener.
//
// The two probes answer different questions and an orchestrator acts on
// them differently, so they are deliberately kept apart:
//
//   - Liveness (/healthz, and /health as its alias) reports only that the
//     process is up and this listener is serving. Docker, Swarm and ECS
//     respond to an unhealthy container by killing and replacing it, and
//     Kubernetes restarts a container whose livenessProbe fails. None of
//     dingo's known wedges are repaired by a restart, and a node doing an
//     initial sync legitimately takes hours or days to become useful, so
//     folding sync state into liveness would put a healthy node into a
//     replacement loop it can never escape. Liveness therefore stays
//     independent of sync state.
//
//   - Readiness (/readyz) reports whether the node is usefully following
//     the chain: its tip is within ReadyTipGapSlots of the wall-clock
//     slot. Failing readiness removes a pod from a Service or a target
//     from a load balancer without killing it, which is the correct
//     response both while a node is still catching up and when its tip has
//     frozen behind a stalled fetch or a rejection loop.
//
// The liveness response body carries the readiness verdict and the tip gap
// as well, so `docker inspect`'s health log shows why a live node is not
// yet ready without a second request.
package health

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Paths served by the health listener.
const (
	// PathHealth is the alias documented for container HEALTHCHECK use.
	PathHealth = "/health"
	// PathLive is the Kubernetes-style liveness path.
	PathLive = "/healthz"
	// PathReady is the Kubernetes-style readiness path.
	PathReady = "/readyz"
)

// TipGapFunc reports the most recent slot-clock tick's distance in slots
// between the wall-clock slot and the node's chain tip. ok is false until
// the node has processed its first tick, which is the case throughout
// database open, Mithril bootstrap and ledger startup.
type TipGapFunc func() (gap uint64, ok bool)

// Status is the classified node health an individual probe reports.
type Status struct {
	// Live is true whenever this listener answered the request.
	Live bool `json:"live"`
	// Ready is true when the node is following the chain within the
	// configured tip-gap tolerance.
	Ready bool `json:"ready"`
	// Reason explains a false Ready. Empty when the node is ready.
	Reason string `json:"reason,omitempty"`
	// TipGapSlots is the observed gap. Nil until the first slot tick.
	TipGapSlots *uint64 `json:"tipGapSlots,omitempty"`
	// Status is "ok" or "unhealthy" for the probe that produced it, so a
	// probe body can be read without knowing which path was requested.
	Status string `json:"status"`
}

// Evaluate classifies the node against a tip-gap tolerance. A nil or
// missing tipGap reports not-ready rather than defaulting to ready: a node
// that has not reached its first slot tick is still starting up.
func Evaluate(tipGap TipGapFunc, readyTipGapSlots uint64) Status {
	status := Status{Live: true}
	if tipGap == nil {
		status.Reason = "tip gap unavailable"
		return status
	}
	gap, ok := tipGap()
	if !ok {
		status.Reason = "no chain tip yet: node is starting or bootstrapping"
		return status
	}
	status.TipGapSlots = &gap
	if gap > readyTipGapSlots {
		status.Reason = fmt.Sprintf(
			"tip gap %d slots exceeds tolerance of %d slots",
			gap,
			readyTipGapSlots,
		)
		return status
	}
	status.Ready = true
	return status
}

// NewMux builds the health listener's routes. readyTipGapSlots is the
// number of slots the chain tip may trail the wall clock while still
// counting as ready.
func NewMux(tipGap TipGapFunc, readyTipGapSlots uint64) *http.ServeMux {
	mux := http.NewServeMux()
	live := probeHandler(tipGap, readyTipGapSlots, false)
	mux.Handle(PathHealth, live)
	mux.Handle(PathLive, live)
	mux.Handle(PathReady, probeHandler(tipGap, readyTipGapSlots, true))
	return mux
}

// probeHandler serves one probe. requireReady selects readiness semantics
// (503 when the node is not following the chain) over liveness semantics
// (200 whenever the handler runs at all).
func probeHandler(
	tipGap TipGapFunc,
	readyTipGapSlots uint64,
	requireReady bool,
) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.Header().Set("Allow", "GET, HEAD")
				http.Error(
					w,
					"method not allowed",
					http.StatusMethodNotAllowed,
				)
				return
			}
			status := Evaluate(tipGap, readyTipGapSlots)
			code := http.StatusOK
			status.Status = "ok"
			if requireReady && !status.Ready {
				code = http.StatusServiceUnavailable
				status.Status = "unhealthy"
			}
			body, err := json.Marshal(status)
			if err != nil {
				http.Error(
					w,
					"health encoding failed",
					http.StatusInternalServerError,
				)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(code)
			if r.Method == http.MethodHead {
				return
			}
			//nolint:errcheck // a probe that hung up needs no error path
			_, _ = w.Write(body)
		},
	)
}
