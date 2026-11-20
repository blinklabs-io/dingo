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

package metadata

import (
	"crypto/sha256"
	"encoding/hex"
)

// CoreNamespacePrefix starts the name of every consensus namespace derived
// from an adopted IndexerDB.
const CoreNamespacePrefix = "dingo_core_"

// CoreNamespaceName names the consensus namespace belonging to an adopted API
// database and schema: a schema of that database on PostgreSQL, a database on
// the same server on MySQL. It is the prefix and the first 26 bytes of the
// SHA-256 of the database and schema names, hex-encoded, 63 bytes in all,
// which both servers' identifier limits permit.
func CoreNamespaceName(database, schema string) string {
	sum := sha256.Sum256([]byte(database + "\x00" + schema))
	return CoreNamespacePrefix + hex.EncodeToString(sum[:26])
}
