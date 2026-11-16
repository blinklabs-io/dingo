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

const (
	// MaxUtxoEventEntries bounds the outputs and spent references one UTxO
	// event carries into a single live-set write. A Badger transaction
	// rejects a write past its entry count and byte limits, so an event as
	// large as the genesis outputs must reach the set in several writes.
	MaxUtxoEventEntries = 10_000
	// MaxUtxoEventBytes bounds the payload of one such write. It sits well
	// below Badger's default transaction size limit.
	MaxUtxoEventBytes = 2 << 20
	// utxoEntryOverhead approximates the key and journal bytes a live-set
	// write adds for each output or spent reference besides its CBOR.
	utxoEntryOverhead = 100
)

// UtxoEventCost returns the entries evt writes to the live UTxO set and the
// payload bytes they carry.
func UtxoEventCost(evt UtxoEvent) (entries int, bytes int) {
	entries = len(evt.Added) + len(evt.Removed)
	bytes = len(evt.Removed) * utxoEntryOverhead
	for _, o := range evt.Added {
		bytes += len(o.Cbor) + utxoEntryOverhead
	}
	return entries, bytes
}

// splitUtxoEvent cuts evt into events that each stay within
// MaxUtxoEventEntries and MaxUtxoEventBytes, keeping its point, cause and
// transaction index. Removed references come first, matching the order a
// single event applies them in. A single output over the byte bound is
// alone in its event.
func splitUtxoEvent(evt UtxoEvent) []UtxoEvent {
	if entries, bytes := UtxoEventCost(evt); entries <= MaxUtxoEventEntries &&
		bytes <= MaxUtxoEventBytes {
		return []UtxoEvent{evt}
	}
	var out []UtxoEvent
	cur := UtxoEvent{Point: evt.Point, Cause: evt.Cause, TxIndex: evt.TxIndex}
	entries, bytes := 0, 0
	flush := func() {
		if entries > 0 {
			out = append(out, cur)
			cur = UtxoEvent{
				Point: evt.Point, Cause: evt.Cause, TxIndex: evt.TxIndex,
			}
			entries, bytes = 0, 0
		}
	}
	room := func(size int) {
		if entries >= MaxUtxoEventEntries ||
			(entries > 0 && bytes+size > MaxUtxoEventBytes) {
			flush()
		}
	}
	for _, r := range evt.Removed {
		room(utxoEntryOverhead)
		cur.Removed = append(cur.Removed, r)
		entries++
		bytes += utxoEntryOverhead
	}
	for _, o := range evt.Added {
		size := len(o.Cbor) + utxoEntryOverhead
		room(size)
		cur.Added = append(cur.Added, o)
		entries++
		bytes += size
	}
	flush()
	return out
}
