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

package txpump

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TxLog records the outcome of a single transaction submission attempt.
type TxLog struct {
	Timestamp string `json:"ts"`
	TxID      string `json:"tx_id"`
	TxType    string `json:"tx_type"` // "payment" | "delegation" | "governance" | "plutus"
	EraID     uint16 `json:"era_id"`
	Status    string `json:"status"` // "submitted" | "rejected" | "error"
	ErrorMsg  string `json:"error,omitempty"`
	NodeAddr  string `json:"node_addr"`
	BatchSize int    `json:"batch_size"`
}

// TxLogger writes JSON-encoded TxLog entries to a file in a thread-safe
// manner.
type TxLogger struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

// NewTxLogger opens (or creates) the log file at logDir/txpump.log and
// returns a TxLogger.  The file is opened in append mode so that successive
// runs accumulate history.
func NewTxLogger(logDir string) (*TxLogger, error) {
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		return nil, fmt.Errorf("txlogger: create log dir %q: %w", logDir, err)
	}
	path := filepath.Join(logDir, "txpump.log")
	//nolint:gosec // log file, not a security concern
	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0o640,
	)
	if err != nil {
		return nil, fmt.Errorf("txlogger: open %q: %w", path, err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	return &TxLogger{file: f, enc: enc}, nil
}

// Log writes a TxLog entry to the log file.  It is safe to call from multiple
// goroutines.
func (l *TxLogger) Log(entry TxLog) error {
	if entry.Timestamp == "" {
		entry.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enc.Encode(entry); err != nil {
		return fmt.Errorf("txlogger: encode entry: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying log file.
func (l *TxLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
