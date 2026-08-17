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

package ledger

// accountExpiredAtEpoch reports whether a reward account with the given stored
// ExpirationEpoch is expired at currentEpoch, per CIP-0163: expired iff a
// concrete expiration was set (non-zero) and it is strictly before the current
// epoch. A zero ExpirationEpoch means "unset" and is never expired.
func accountExpiredAtEpoch(expirationEpoch, currentEpoch uint64) bool {
	return expirationEpoch != 0 && expirationEpoch < currentEpoch
}
