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
	"github.com/blinklabs-io/dingo/database/models"
)

// sqliteOrOfRefs is the legacy OR-of-references predicate the plan tests
// compare the production statements against: one "(tx_id = ? AND output_idx =
// ?)" term per reference, independent of the production identity predicate.
func sqliteOrOfRefs(refs []models.UtxoId) (string, []any) {
	args := make([]any, 0, len(refs)*2)
	for _, ref := range refs {
		args = append(args, ref.Hash, ref.Idx)
	}
	return joinN(len(refs), " OR ", "(tx_id = ? AND output_idx = ?)"), args
}

func sqliteOrRefArgs(refs []models.UtxoId) []any {
	args := make([]any, 0, len(refs)*4)
	for _, ref := range refs {
		args = sqliteUtxoSQL.bindRef(args, ref.Hash, ref.Idx)
	}
	return args
}
