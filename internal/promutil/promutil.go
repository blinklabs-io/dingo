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

// Package promutil holds Prometheus registration helpers shared by
// components that must not panic on a reused registry.
package promutil

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Registration records the collectors it newly registers on a registry so a
// partially completed group of registrations can be undone. The first
// registration error is latched: later Register calls do nothing, so callers
// register a whole group and check Err once.
type Registration struct {
	registry prometheus.Registerer
	added    []prometheus.Collector
	err      error
}

// NewRegistration returns a Registration that registers on registry.
func NewRegistration(registry prometheus.Registerer) *Registration {
	return &Registration{registry: registry}
}

// Register registers c and returns it. When a collector with the same
// descriptor is already registered, that collector is returned instead and is
// not recorded for rollback, because this Registration does not own it. On any
// other error, or after an earlier error, c is returned unregistered and the
// error is available from Err.
func Register[C prometheus.Collector](r *Registration, c C) C {
	if r.err != nil {
		return c
	}
	err := r.registry.Register(c)
	if err == nil {
		r.added = append(r.added, c)
		return c
	}
	if already, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
		if existing, ok := already.ExistingCollector.(C); ok {
			return existing
		}
	}
	r.err = err
	return c
}

// Err returns the first registration error, if any.
func (r *Registration) Err() error {
	return r.err
}

// Rollback unregisters every collector this Registration newly registered.
func (r *Registration) Rollback() {
	for _, c := range r.added {
		r.registry.Unregister(c)
	}
	r.added = nil
}
