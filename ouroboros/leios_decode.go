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

package ouroboros

import (
	"github.com/blinklabs-io/dingo/internal/safedecode"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// guardedDecode runs decode over raw under safedecode.Guard, so a decoder
// panic on peer-supplied bytes becomes an error wrapping
// safedecode.ErrDecodePanic and leaves by the decode-failure route the caller
// already has. Nothing above the Leios decode sites recovers: each protocol
// worker runs on its own goroutine, and gouroboros' cbor.Value decoder
// recovers exactly one panic class (an unhashable Go map key) and re-panics
// every other one.
//
// decode is a parameter rather than a direct call so the containment can be
// driven by a decoder that genuinely panics instead of by a stubbed-out
// recover, matching how blinklabs-io/dingo#4551 drives the txsubmission
// guard. Production always supplies a real constructor, through the two
// wrappers below.
//
// Recovering is sound only because every decoder used here is a pure function
// of raw: it builds a fresh value, takes no lock, performs no I/O and mutates
// nothing the caller shares, so a contained panic cannot leave shared state
// half-updated. Do not widen this to work that mutates shared state.
func guardedDecode[T any](
	raw []byte,
	decode func([]byte) (T, error),
) (T, error) {
	return safedecode.Guard(func() (T, error) {
		return decode(raw)
	})
}

// decodeLeiosEndorserBlock decodes an endorser-block manifest. The manifest is
// peer-supplied on every path that produces one: leios-fetch and leios-notify
// deliver it directly, the in-memory endorser-block cache retains those bytes
// verbatim, and the blob store replays bytes a peer delivered earlier.
//
// The plain guard is used rather than the shared decode cache
// (decodeWithPanicSafeMetrics) for three reasons. LeiosEndorserBlock
// deliberately does not implement ledger.Block, so neither existing cache
// instance can hold it and "reuse" would in fact mean a third decodeCache plus
// its own metrics. storeLeiosEndorserBlock decodes while holding
// leiosAnnouncementsMu, and getOrDecode can park its caller on another
// goroutine's in-flight decode, which would introduce a wait-for-goroutine
// dependency under that lock. And the decoded manifest is consumed immediately
// at every call site, behind the (slot, hash)-keyed endorser-block cache that
// already deduplicates these bytes across connections.
func decodeLeiosEndorserBlock(
	raw []byte,
) (*lcommon.LeiosEndorserBlock, error) {
	return guardedDecode(raw, lcommon.NewLeiosEndorserBlockFromCbor)
}

// decodeLeiosAnnouncementHeader decodes the ranking-block header a LeiosNotify
// peer sends in a block announcement.
//
// The shared header decode cache is deliberately not used. Its key,
// hashDecodeInput(blockType, bytes), identifies the input and not the decoder,
// while the cache is populated by decodeChainsyncHeader -- whose choice of
// decoder depends on o.config.NetworkMagic and the block type, and which
// additionally warms header.Hash(). Leios networking is enabled by
// --run-mode leios or a Dijkstra start era on any network, not only on
// Musashi, so on those networks the identical header bytes would be decoded by
// two different decoders under one key and either path could be served the
// other's value.
func decodeLeiosAnnouncementHeader(
	raw []byte,
) (*gdijkstra.DijkstraBlockHeader, error) {
	return guardedDecode(raw, gdijkstra.NewDijkstraBlockHeaderFromCbor)
}
