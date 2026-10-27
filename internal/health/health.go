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
//     Kubernetes restarts a container whose livenessProbe fails. A stalled
//     fetch or a rejection loop is not repaired by a restart, and a node
//     doing an initial sync legitimately takes hours or days to become useful, so
//     folding sync state into liveness would put a healthy node into a
//     replacement loop it can never escape. Liveness therefore stays
//     independent of sync state. The one condition beyond that is a
//     liveness Check: the node supplies one that fails when the slot clock
//     has ticked before and then stopped, which is a wedged event loop and
//     is repaired by a restart.
//
//   - Readiness (/readyz) reports whether the node is usefully following
//     the chain: its tip is within ReadyTipGapSlots of the wall-clock
//     slot, and every readiness Check passes (the node supplies database
//     availability and, for a block producer, forging state). Failing
//     readiness removes a pod from a Service or a target from a load
//     balancer without killing it, which is the correct response both
//     while a node is still catching up and when its tip has frozen behind
//     a stalled fetch or a rejection loop.
//
// The liveness response body carries the readiness verdict and the tip gap
// as well, so `docker inspect`'s health log shows why a live node is not
// yet ready without a second request.
package health

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
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

// Check is one extra condition on the node's health, beyond the tip gap.
type Check struct {
	// Liveness marks a check whose failure fails /healthz as well as
	// /readyz. Every other check gates /readyz alone.
	Liveness bool
	// Startup marks a readiness check that names a startup phase holding
	// the node back before its tip gap is known. It runs ahead of the tip
	// gap, so its reason is reported instead of the generic one for a node
	// still starting; every other readiness check runs only once the node
	// is within the tolerance.
	Startup bool
	// Reason is the stable public reason code returned when Fn fails.
	Reason string
	// Fn returns nil when the condition holds, and otherwise an error whose
	// full message is retained for server diagnostics.
	Fn func() error
}

func (c Check) evaluate() error {
	if c.Fn == nil {
		return errors.New("health check callback is unavailable")
	}
	return c.Fn()
}

// Status is the classified node health an individual probe reports.
type Status struct {
	// Live is true whenever this listener answered the request and no
	// liveness check failed.
	Live bool `json:"live"`
	// Ready is true when the node is following the chain within the
	// configured tip-gap tolerance.
	Ready bool `json:"ready"`
	// Reason explains a false Live or Ready. Empty when both are true.
	Reason string `json:"reason,omitempty"`
	// TipGapSlots is the observed gap. Nil until the first slot tick.
	TipGapSlots *uint64 `json:"tipGapSlots,omitempty"`
	// Status is "ok" or "unhealthy" for the probe that produced it, so a
	// probe body can be read without knowing which path was requested.
	Status string `json:"status"`
}

const (
	reasonTipGapUnavailable = "tip_gap_unavailable"
	reasonNodeStarting      = "node_starting"
	reasonTipGapExceeded    = "tip_gap_exceeded"
	reasonCheckFailed       = "health_check_failed"
)

type evaluation struct {
	status     Status
	diagnostic error
}

// Evaluate classifies the node against a tip-gap tolerance and the extra
// checks. A nil or missing tipGap reports not-ready rather than defaulting to
// ready: a node that has not reached its first slot tick is still starting up.
// Readiness checks run only once the node is within the tolerance, so a node
// that is still syncing reports the gap rather than a secondary condition.
func Evaluate(
	tipGap TipGapFunc,
	readyTipGapSlots uint64,
	checks ...Check,
) Status {
	return evaluate(tipGap, readyTipGapSlots, checks...).status
}

func evaluate(
	tipGap TipGapFunc,
	readyTipGapSlots uint64,
	checks ...Check,
) evaluation {
	status := Status{Live: true}
	for _, check := range checks {
		if !check.Liveness {
			continue
		}
		if err := check.evaluate(); err != nil {
			status.Live = false
			status.Reason = check.Reason
			if status.Reason == "" {
				status.Reason = reasonCheckFailed
			}
			return evaluation{status: status, diagnostic: err}
		}
	}
	for _, check := range checks {
		if check.Liveness || !check.Startup {
			continue
		}
		if err := check.evaluate(); err != nil {
			status.Reason = check.Reason
			if status.Reason == "" {
				status.Reason = reasonCheckFailed
			}
			return evaluation{status: status, diagnostic: err}
		}
	}
	if tipGap == nil {
		status.Reason = reasonTipGapUnavailable
		return evaluation{status: status}
	}
	gap, ok := tipGap()
	if !ok {
		status.Reason = reasonNodeStarting
		return evaluation{status: status}
	}
	status.TipGapSlots = &gap
	if gap > readyTipGapSlots {
		status.Reason = reasonTipGapExceeded
		return evaluation{status: status}
	}
	for _, check := range checks {
		if check.Liveness || check.Startup {
			continue
		}
		if err := check.evaluate(); err != nil {
			status.Reason = check.Reason
			if status.Reason == "" {
				status.Reason = reasonCheckFailed
			}
			return evaluation{status: status, diagnostic: err}
		}
	}
	status.Ready = true
	return evaluation{status: status}
}

type diagnosticReporter struct {
	logger *slog.Logger
	mu     sync.Mutex
	recent map[string]struct{}
	order  []string
}

const maxRememberedDiagnostics = 128

func (r *diagnosticReporter) report(result evaluation) {
	if r == nil || r.logger == nil {
		return
	}
	key := ""
	if result.diagnostic != nil {
		key = result.status.Reason + "\x00" + result.diagnostic.Error()
	}
	r.mu.Lock()
	if r.recent == nil {
		r.recent = make(map[string]struct{})
	}
	if _, exists := r.recent[key]; exists {
		r.mu.Unlock()
		return
	}
	if len(r.order) == maxRememberedDiagnostics {
		delete(r.recent, r.order[0])
		copy(r.order, r.order[1:])
		r.order = r.order[:len(r.order)-1]
	}
	r.recent[key] = struct{}{}
	r.order = append(r.order, key)
	r.mu.Unlock()
	if result.diagnostic != nil {
		r.logger.Warn(
			"health check failed",
			"reason", result.status.Reason,
			"error", result.diagnostic,
		)
	}
}

// NewMux builds the health listener's routes. readyTipGapSlots is the
// number of slots the chain tip may trail the wall clock while still
// counting as ready.
func NewMux(
	logger *slog.Logger,
	tipGap TipGapFunc,
	readyTipGapSlots uint64,
	checks ...Check,
) *http.ServeMux {
	mux := http.NewServeMux()
	reporter := &diagnosticReporter{logger: logger}
	live := probeHandler(reporter, tipGap, readyTipGapSlots, false, checks)
	mux.Handle(PathHealth, live)
	mux.Handle(PathLive, live)
	mux.Handle(
		PathReady,
		probeHandler(reporter, tipGap, readyTipGapSlots, true, checks),
	)
	return mux
}

// probeHandler serves one probe. requireReady selects readiness semantics
// (503 when the node is not following the chain) over liveness semantics
// (200 whenever the handler runs at all).
func probeHandler(
	reporter *diagnosticReporter,
	tipGap TipGapFunc,
	readyTipGapSlots uint64,
	requireReady bool,
	checks []Check,
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
			result := evaluate(tipGap, readyTipGapSlots, checks...)
			reporter.report(result)
			status := result.status
			code := http.StatusOK
			status.Status = "ok"
			if !status.Live || (requireReady && !status.Ready) {
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
