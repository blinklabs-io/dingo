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

package connmanager

import (
	"errors"
	"io"
	"log/slog"
)

// closeConnAndLog closes conn, logging at Debug level if it fails. Every
// caller already logs the reason the connection is being torn down, so a
// close failure here is a secondary diagnostic, not the primary event.
func closeConnAndLog(
	logger *slog.Logger,
	conn io.Closer,
	msg string,
	attrs ...any,
) {
	if conn == nil {
		return
	}
	if err := conn.Close(); err != nil {
		logger.Debug(msg, append(attrs, "error", err)...)
	}
}

// joinCloseErr closes closer and folds any close error into err, so a
// connection that fails to close on an already-failing setup path isn't
// silently lost.
func joinCloseErr(err error, closer io.Closer) error {
	if closeErr := closer.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}
