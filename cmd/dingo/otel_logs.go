// Copyright 2024 Blink Labs Software
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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

// withOTLPLogs returns logger extended to also export every record it
// emits over OTLP when OTEL_LOGS_EXPORTER asks for it, plus the function
// that flushes the export at exit. Stdout logging is never replaced: an
// exporter that cannot be built is reported on warn and skipped.
func withOTLPLogs(
	ctx context.Context,
	logger *slog.Logger,
	newResource func(context.Context) (*resource.Resource, error),
	warn io.Writer,
) (*slog.Logger, func(context.Context) error) {
	noop := func(context.Context) error { return nil }
	v := os.Getenv("OTEL_LOGS_EXPORTER")
	if v == "" || v == "none" {
		return logger, noop
	}
	res, err := newResource(ctx)
	if errors.Is(err, resource.ErrPartialResource) {
		fmt.Fprintf(
			warn,
			"%s: ignoring unparsable OTEL_RESOURCE_ATTRIBUTES entries: %v\n",
			programName, err,
		)
	} else if err != nil {
		fmt.Fprintf(
			warn,
			"%s: building log export resource: %v; logging to stdout only\n",
			programName, err,
		)
		return logger, noop
	}
	exporter, err := autoexport.NewLogExporter(ctx)
	if err != nil {
		fmt.Fprintf(
			warn,
			"%s: OTEL_LOGS_EXPORTER=%q: %v; logging to stdout only\n",
			programName, v, err,
		)
		return logger, noop
	}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)
	exported := levelGate{
		Handler: otelslog.NewHandler(
			programName,
			otelslog.WithLoggerProvider(provider),
		),
		gate: logger.Handler(),
	}
	return slog.New(
		slog.NewMultiHandler(logger.Handler(), exported),
	), provider.Shutdown
}

// levelGate exports only the records the stdout handler would emit, so the
// configured log level applies to both destinations.
type levelGate struct {
	slog.Handler
	gate slog.Handler
}

func (l levelGate) Enabled(ctx context.Context, level slog.Level) bool {
	return l.gate.Enabled(ctx, level)
}

func (l levelGate) WithAttrs(attrs []slog.Attr) slog.Handler {
	return levelGate{Handler: l.Handler.WithAttrs(attrs), gate: l.gate}
}

func (l levelGate) WithGroup(name string) slog.Handler {
	return levelGate{Handler: l.Handler.WithGroup(name), gate: l.gate}
}
