// Copyright 2025 Blink Labs Software
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

package gcs

import (
	"fmt"
	"io"
	"log/slog"
)

// GcsLogger is a wrapper type to give our logger a consistent interface
type GcsLogger struct {
	logger *slog.Logger
}

func NewGcsLogger(logger *slog.Logger) *GcsLogger {
	if logger == nil {
		// Create logger to throw away logs
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &GcsLogger{logger: logger}
}

func (g *GcsLogger) Infof(msg string, args ...any) {
	g.logger.Info(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (g *GcsLogger) Warningf(msg string, args ...any) {
	g.logger.Warn(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (g *GcsLogger) Debugf(msg string, args ...any) {
	g.logger.Debug(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (g *GcsLogger) Errorf(msg string, args ...any) {
	g.logger.Error(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}
