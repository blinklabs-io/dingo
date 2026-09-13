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

package metadata

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// ProviderDependencies are shared application settings injected into any
// storage.metadata provider.
type ProviderDependencies struct {
	DataDir        string
	StorageMode    string
	MaxConnections int
	Logger         *slog.Logger
	PromRegistry   prometheus.Registerer
	// TracingEnabled reports whether OpenTelemetry tracing is configured for
	// this node (the "tracing"/--tracing/DINGO_TRACING_ENABLED setting). A
	// provider's sqlstore.OpenDB calls use it to decide whether to pay for
	// otelsql's per-query span/attribute/metric instrumentation: with no
	// TracerProvider registered (tracing disabled, the default), that
	// instrumentation still runs against the no-op provider and allocates
	// on every single query for zero observability benefit.
	TracingEnabled bool
}
