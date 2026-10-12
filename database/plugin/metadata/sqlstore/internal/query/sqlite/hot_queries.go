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

package sqlitequery

// Exported aliases of generated query texts, so the parent sqlstore package
// can key its prepared-statement cache by exactly the text these methods
// send to the database. Defined as aliases (not copies) so regenerating this
// package can never leave the cache keyed by stale SQL.
const (
	SetTipQuery                       = setTip
	SetBlockNonceQuery                = setBlockNonce
	GetAccountByCredentialQuery       = getAccountByCredential
	GetActiveAccountByCredentialQuery = getActiveAccountByCredential
)
