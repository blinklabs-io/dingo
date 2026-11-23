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

package dbtest

import (
	"errors"
	"sync"
)

// ErrSQLRefused is the error an armed SQLRefusal gives a statement.
var ErrSQLRefused = errors.New("SQL statement refused")

// SQLRefusal is a metadata.StatementGuard that, once armed, refuses every
// statement and records its text. A test passes it as
// Options.StatementGuard, arms it after the setup that may use SQL, and
// reads Statements: a caller that swallows the refusal is still seen. Record
// instead of Arm records the statements and lets them run.
type SQLRefusal struct {
	mu        sync.Mutex
	recording bool
	refusing  bool
	refused   []string
}

// CheckStatement implements metadata.StatementGuard.
func (g *SQLRefusal) CheckStatement(query string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.recording {
		return nil
	}
	g.refused = append(g.refused, query)
	if g.refusing {
		return ErrSQLRefused
	}
	return nil
}

// Arm makes the guard refuse every statement from here on.
func (g *SQLRefusal) Arm() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.recording, g.refusing = true, true
}

// Record makes the guard record every statement from here on and let it run.
func (g *SQLRefusal) Record() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.recording = true
}

// Statements returns the text of every statement seen since Arm or Record.
func (g *SQLRefusal) Statements() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.refused...)
}
