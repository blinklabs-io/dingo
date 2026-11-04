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

package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/XSAM/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// OpenDB opens a database/sql pool, instrumented with OpenTelemetry tracing
// only when tracingEnabled is true. Keeping driver wrapping in the shared
// package gives every provider the same query and transaction tracing
// behavior when tracing is on.
//
// otelsql.Open's wrapping is not free even when no TracerProvider is
// registered: with tracing off (the default), every ExecContext/QueryContext/
// QueryRowContext call still starts a span against the no-op provider,
// computes its attributes, and allocates a wrapping *sql.Rows -- pure
// overhead for zero observability benefit. Measured on a from-genesis sync
// with tracing disabled, otelsql/otel accounted for roughly 9% of all
// allocated bytes over the run. Skipping the wrap entirely when tracing is
// off removes that cost without changing any query's behavior or result.
func OpenDB(
	driverName, dataSourceName, systemName string,
	tracingEnabled bool,
) (*sql.DB, error) {
	if !tracingEnabled {
		return sql.Open(driverName, dataSourceName)
	}
	return otelsql.Open(
		driverName,
		dataSourceName,
		otelsql.WithAttributes(
			semconv.DBSystemNameKey.String(systemName),
		),
		// A query with no span to join would start a root trace of its own
		// for every statement, and one inside an unsampled trace could never
		// export; record queries only inside a sampled trace.
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			SpanFilter: func(
				ctx context.Context,
				_ otelsql.Method,
				_ string,
				_ []driver.NamedValue,
			) bool {
				sc := trace.SpanContextFromContext(ctx)
				return sc.IsValid() && sc.IsSampled()
			},
		}),
	)
}
