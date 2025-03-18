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

package badger

import (
	"fmt"
	"io"
	"log/slog"
)

// BadgerLogger is a wrapper type to give our logger the expected interface
type BadgerLogger struct {
	logger *slog.Logger
}

func NewBadgerLogger(logger *slog.Logger) *BadgerLogger {
	if logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &BadgerLogger{logger: logger}
}

func (b *BadgerLogger) Infof(msg string, args ...any) {
	b.logger.Info(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (b *BadgerLogger) Warningf(msg string, args ...any) {
	b.logger.Warn(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (b *BadgerLogger) Debugf(msg string, args ...any) {
	b.logger.Debug(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (b *BadgerLogger) Errorf(msg string, args ...any) {
	b.logger.Error(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}
