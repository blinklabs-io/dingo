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

package config

import (
	"encoding/hex"
	"fmt"
	"strings"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ParsePoolKeyHash parses a stake pool ID given either as a bech32 "pool1…"
// string or as the 56-character hex encoding of its 28-byte key hash.
func ParsePoolKeyHash(s string) ([28]byte, error) {
	v := strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToLower(v), "pool1") {
		id, err := lcommon.NewPoolIdFromBech32(v)
		if err != nil {
			return [28]byte{}, fmt.Errorf("invalid pool ID %q: %w", s, err)
		}
		return [28]byte(id), nil
	}
	raw, err := hex.DecodeString(v)
	if err != nil || len(raw) != 28 {
		return [28]byte{}, fmt.Errorf(
			"invalid pool ID %q: want bech32 pool1... or 56 hex characters",
			s,
		)
	}
	return [28]byte(raw), nil
}
