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

package chainselection

import (
	"bytes"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// frontierWindow is an append-only backing array shared by every PeerChainTip
// (and eagerness delivery entry) whose frontier view is a slice of it.
//
// Invariants:
//   - arr[:used] is never written after it is first filled. Every view lies
//     within arr[:used], so no holder can observe another holder's append, and
//     sharing a view needs no copy.
//   - A write happens only at index used. A holder whose next index is below
//     used may adopt the element already there when it is equal, which is how
//     a staged frontier and the committed peer converge on the same point.
//   - Views are capacity-clipped to their length, so an append through the
//     plain slice header reallocates rather than writing into the array.
//
// Trimming the front or truncating a view is a reslice. Nothing is cleared:
// the array is replaced, with only the live view copied, whenever it fills, so
// stale entries are retained for at most one array generation.
type frontierWindow[T any] struct {
	arr  []T
	used int
}

// windowRef locates a view inside a frontierWindow. The view itself stays in
// the owning struct's slice field so existing readers are unaffected. A view
// that this ref does not describe (assigned directly, or built elsewhere) is
// detected by address and rebased onto a fresh window instead of written.
type windowRef[T any] struct {
	win *frontierWindow[T]
	lo  int
}

// owns reports whether view is exactly arr[lo:lo+len(view)] of this window.
func (r *windowRef[T]) owns(view []T) bool {
	return r.win != nil && len(view) > 0 &&
		r.lo >= 0 && r.lo+len(view) <= r.win.used &&
		&r.win.arr[r.lo] == &view[0]
}

// windowAppend returns view extended by v. The result shares the window with
// view whenever that cannot be observed by another holder.
func windowAppend[T any](
	ref *windowRef[T],
	view []T,
	v T,
	equal func(a, b T) bool,
) []T {
	if ref.owns(view) {
		hi := ref.lo + len(view)
		switch {
		case hi < ref.win.used:
			if equal(ref.win.arr[hi], v) {
				return ref.win.arr[ref.lo : hi+1 : hi+1]
			}
		case hi < len(ref.win.arr):
			ref.win.arr[hi] = v
			ref.win.used++
			return ref.win.arr[ref.lo : hi+1 : hi+1]
		}
	}
	size := len(view)
	capacity := max(2*size+16, 16)
	win := &frontierWindow[T]{arr: make([]T, capacity)}
	copy(win.arr, view)
	win.arr[size] = v
	win.used = size + 1
	ref.win = win
	ref.lo = 0
	return win.arr[: size+1 : size+1]
}

// windowTrimFront drops the first n entries of view.
func windowTrimFront[T any](ref *windowRef[T], view []T, n int) []T {
	if n <= 0 {
		return view
	}
	if ref.owns(view) {
		ref.lo += n
	}
	return view[n:]
}

// windowTruncate keeps the first n entries of view, clipping its capacity so
// a later append cannot reach the entries it dropped.
func windowTruncate[T any](view []T, n int) []T {
	return view[:n:n]
}

func pointsEqual(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

func slotsEqual(a, b uint64) bool { return a == b }
