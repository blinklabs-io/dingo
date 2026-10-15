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

import "database/sql"

// lookupCachedStmt returns the cached statement for query without borrowing
// it, for tests that inspect the cache rather than execute through it.
func (s *Store) lookupCachedStmt(query string) (*sql.Stmt, bool) {
	s.stmtMu.Lock()
	defer s.stmtMu.Unlock()
	stmt, ok := s.stmts[query]
	return stmt, ok
}
