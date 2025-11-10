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

package aws

import (
	"fmt"
	"io"
	"log/slog"
)

// S3Logger is a thin wrapper giving our logger a consistent interface.
type S3Logger struct {
	logger *slog.Logger
}

func NewS3Logger(logger *slog.Logger) *S3Logger {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &S3Logger{logger: logger}
}

func (g *S3Logger) Infof(msg string, args ...any) {
	g.logger.Info(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (g *S3Logger) Warningf(msg string, args ...any) {
	g.logger.Warn(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (g *S3Logger) Debugf(msg string, args ...any) {
	g.logger.Debug(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}

func (g *S3Logger) Errorf(msg string, args ...any) {
	g.logger.Error(
		fmt.Sprintf(msg, args...),
		"component", "database",
	)
}
