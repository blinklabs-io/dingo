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
	"context"
	"errors"
	"fmt"
	"net/http"
	gosync "sync"

	"connectrpc.com/connect"
)

// errBulkBusy is returned when every bulk-read slot is held.
var errBulkBusy = errors.New("too many concurrent bulk requests")

type bulkSlotHolderKey struct{}

// bulkSlotHolder carries the slots a handler acquired out to
// holdBulkSlotsUntilWritten. Connect serializes and writes a unary response
// after the handler returns, so a slot released by the handler itself would
// be free again while a slow reader still pins the response it bounded.
type bulkSlotHolder struct {
	mu       gosync.Mutex
	releases []func()
}

func (h *bulkSlotHolder) add(release func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.releases = append(h.releases, release)
}

func (h *bulkSlotHolder) releaseAll() {
	h.mu.Lock()
	releases := h.releases
	h.releases = nil
	h.mu.Unlock()
	for _, release := range releases {
		release()
	}
}

// holdBulkSlotsUntilWritten keeps every bulk slot acquired while serving a
// request until next has finished writing the response.
func holdBulkSlotsUntilWritten(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		holder := &bulkSlotHolder{}
		defer holder.releaseAll()
		next.ServeHTTP(w, r.WithContext(
			context.WithValue(r.Context(), bulkSlotHolderKey{}, holder),
		))
	})
}

// acquireBulk reserves one of the MaxConcurrentBulkRequests slots shared by
// every unary handler whose cost scales with the size of its result. It never
// waits: a caller that cannot get a slot is refused immediately, because a
// queue of blocked handlers would hold the very memory the budget bounds.
// When ctx comes from holdBulkSlotsUntilWritten the slot is freed after the
// response is written and the returned function does nothing; otherwise the
// returned function frees it. Either way the caller defers it.
func (u *Utxorpc) acquireBulk(ctx context.Context) (func(), error) {
	select {
	case u.bulkSlots <- struct{}{}:
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
	release := gosync.OnceFunc(func() { <-u.bulkSlots })
	if holder, ok := ctx.Value(bulkSlotHolderKey{}).(*bulkSlotHolder); ok {
		holder.add(release)
		return func() {}, nil
	}
	return release, nil
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
