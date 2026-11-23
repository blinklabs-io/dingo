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

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExecTransactionInsertRejectsLiteralBlockHash pins that a block hash
// written as a SQL literal is refused: the row stores the block's
// block_identity id, which only a bound hash can be resolved to, and copying
// the literal into block_id would leave a blob no read resolves.
func TestExecTransactionInsertRejectsLiteralBlockHash(t *testing.T) {
	t.Parallel()
	_, err := ExecTransactionInsert(nil,
		`INSERT INTO "transaction" (hash, block_hash, slot)
VALUES (zeroblob(32), zeroblob(32), 1)`,
	)
	require.ErrorContains(t, err, "block_hash")
}
