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

package utxorpc

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
)

// errBulkBusy is returned when every bulk-read slot is held.
var errBulkBusy = errors.New("too many concurrent bulk requests")

// acquireBulk reserves one of the MaxConcurrentBulkRequests slots shared by
// every unary handler whose cost scales with the size of its result. It never
// waits: a caller that cannot get a slot is refused immediately, because a
// queue of blocked handlers would hold the very memory the budget bounds.
// The returned function releases the slot and must be called exactly once.
func (u *Utxorpc) acquireBulk() (func(), error) {
	select {
	case u.bulkSlots <- struct{}{}:
		return func() { <-u.bulkSlots }, nil
	default:
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf(
				"%w: limit is %d",
				errBulkBusy,
				cap(u.bulkSlots),
			),
		)
	}
}

// byteBudget tracks the payload bytes a single response has retained.
type byteBudget struct {
	limit int64
	used  int64
}

// fits reports whether n more bytes stay within the limit. The first item
// always fits, so a limit smaller than one item still makes progress.
func (b *byteBudget) fits(n int) bool {
	return b.used == 0 || b.used+int64(n) <= b.limit
}

func (b *byteBudget) add(n int) { b.used += int64(n) }

// exceeded returns the error for a response that cannot be partially
// delivered once the limit is passed.
func (b *byteBudget) exceeded() error {
	return connect.NewError(
		connect.CodeResourceExhausted,
		fmt.Errorf(
			"response exceeds the %d byte limit; request fewer items",
			b.limit,
		),
	)
}
