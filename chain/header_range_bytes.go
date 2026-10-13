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

package chain

import (
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/muxer"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// muxSegmentHeaderSize is the size of the header the muxer prepends to each
// segment of at most muxer.SegmentMaxPayloadLength bytes.
const muxSegmentHeaderSize = 8

// cborByteStringHeaderSize returns the size of a CBOR byte string's head for
// a payload of n bytes.
func cborByteStringHeaderSize(n uint64) uint64 {
	switch {
	case n < 24:
		return 1
	case n <= 0xff:
		return 2
	case n <= 0xffff:
		return 3
	case n <= 0xffffffff:
		return 5
	default:
		return 9
	}
}

// blockWireSize estimates the bytes a peer sends for one queued block in
// response to a block-fetch range request: the MsgBlock envelope, the
// era-tagged wrapper and the mux segment headers around the block itself.
// It reports false when the header does not determine the block size.
//
// The block is a CBOR array whose elements are the header and the body parts;
// the header's body size is the total size of those parts, so the block is
// one array head (all eras Shelley and later have at most 5 elements) plus
// the header plus the body. Byron headers carry no body size, and a header
// reporting zero cannot be sized either, so neither gets an estimate.
//
// The envelope is [msgBlock, #6.24(bytes .cbor [era, block])]: an array head
// and message type (2), the tag (2), the byte string head, and the era-tagged
// wrapper's array head and era id (2).
func blockWireSize(header lcommon.BlockHeader) (uint64, bool) {
	if header.Era().Id == byron.EraIdByron {
		return 0, false
	}
	bodySize := header.BlockBodySize()
	if bodySize == 0 {
		return 0, false
	}
	wrapped := 1 + uint64(len(header.Cbor())) + bodySize + 2
	msg := wrapped + cborByteStringHeaderSize(wrapped) + 4
	segments := (msg + muxer.SegmentMaxPayloadLength - 1) /
		muxer.SegmentMaxPayloadLength
	return msg + segments*muxSegmentHeaderSize, true
}

// HeaderRangeAfterBytes is HeaderRangeAfter with an additional bound on the
// estimated wire size of the window, so a run of large blocks does not form a
// single oversized block-fetch request. The window ends before the header
// that would take its estimated size past maxBytes, but always holds at least
// one header: a header larger than maxBytes forms a range of its own.
//
// A window is bounded by count alone when maxBytes is zero, and from the
// first header whose size cannot be estimated onward (see blockWireSize),
// because such a window gets no estimate at dispatch either.
func (c *Chain) HeaderRangeAfterBytes(
	skip, count int,
	maxBytes uint64,
) (start, end ocommon.Point, available int) {
	if c == nil || count <= 0 || skip < 0 {
		return ocommon.Point{}, ocommon.Point{}, 0
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if skip >= len(c.headers) {
		return ocommon.Point{}, ocommon.Point{}, 0
	}
	available = min(count, len(c.headers)-skip)
	if maxBytes > 0 {
		var total uint64
		for i := range available {
			size, ok := blockWireSize(c.headers[skip+i].header)
			if !ok {
				break
			}
			if i > 0 && total+size > maxBytes {
				available = i
				break
			}
			total += size
		}
	}
	return c.headers[skip].point, c.headers[skip+available-1].point, available
}

// QueuedRangeWireBytes returns the estimated wire size of the blocks from
// start through end inclusive, summed over the headers in the header queue.
// It reports false, and no partial sum, when either endpoint is not queued,
// the endpoints are out of order, or any header in between cannot be sized
// (a Byron header, for one).
func (c *Chain) QueuedRangeWireBytes(
	start, end ocommon.Point,
) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	first, err := c.findQueuedHeader(start)
	if err != nil || first < 0 {
		return 0, false
	}
	last, err := c.findQueuedHeader(end)
	if err != nil || last < first {
		return 0, false
	}
	var total uint64
	for _, queued := range c.headers[first : last+1] {
		size, ok := blockWireSize(queued.header)
		if !ok {
			return 0, false
		}
		total += size
	}
	return total, true
}
